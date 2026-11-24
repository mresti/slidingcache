package countminsketch

import (
	"fmt"
	"testing"

	"github.com/mresti/slidingcache"
)

// reference is what a sketch cache is compared against: the root cache, or the
// naive model where the root has no equivalent configuration.
type reference interface {
	slidingcache.SlidingCache
	HighWater() (hw int64, ok bool)
	Stats() slidingcache.Stats
}

// call names one operation in a failure message. It is formatted only when a
// check fails, so replaying a script does not allocate a string per op.
type call struct {
	op    string
	epoch int64
	key   string
}

func (c call) String() string { return fmt.Sprintf("%s(%d, %s)", c.op, c.epoch, c.key) }

// harness replays the same operations on a sketch cache and its reference and
// checks, after every one, the returns with its comparison and the window
// invariants of the sketch cache.
type harness struct {
	t       *testing.T
	sketch  *Cache
	ref     reference
	compare comparison
	// bound tracks the exact live events of every key, for Mode B's upper bound.
	// It is nil in Mode A.
	bound  *referenceModel
	window *windowChecker
}

// comparison decides whether a sketch return agrees with the reference's.
type comparison func(h *harness, c call, got, want int)

// newExactHarness is Mode A: a collision-free injected hash, proven so for the
// dimensions of cfg's shards, under which every return, the high-water mark
// and the counters must equal the reference's.
func newExactHarness(t *testing.T, cfg Config, ref reference) *harness {
	t.Helper()
	requireCollisionFree(t, cfg, exactHashes())
	sketch := newTestCache(t, cfg, WithHashFunc(exactHash))
	requireShardsSharedAndSpanned(t, sketch)
	return &harness{
		t:       t,
		sketch:  sketch,
		ref:     ref,
		compare: requireExact,
		window:  newWindowChecker(),
	}
}

// newBoundedHarness is Mode B: the default maphash, collisions allowed, so a
// count may only lie between the oracle's and the window's total.
func newBoundedHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	sketch := newTestCache(t, cfg)
	reseedUntilShardsSharedAndSpanned(t, sketch)
	return &harness{
		t:       t,
		sketch:  sketch,
		ref:     newOracle(t, oracleConfig(t, cfg)),
		compare: requireBounded,
		bound:   newReferenceModel(cfg),
		window:  newWindowChecker(),
	}
}

func (h *harness) store(epoch int64, key string) int {
	h.t.Helper()
	want := h.ref.Store(epoch, key)
	if h.bound != nil {
		h.requireBoundAgrees(call{"Store", epoch, key}, h.bound.Store(epoch, key), want)
	}
	got := h.sketch.Store(epoch, key)
	h.compare(h, call{"Store", epoch, key}, got, want)
	h.requireWindows(key, got, 1)
	return got
}

func (h *harness) get(epoch int64, key string) int {
	h.t.Helper()
	want := h.ref.Get(epoch, key)
	if h.bound != nil {
		h.requireBoundAgrees(call{"Get", epoch, key}, h.bound.Get(epoch, key), want)
	}
	got := h.sketch.Get(epoch, key)
	h.compare(h, call{"Get", epoch, key}, got, want)
	h.requireWindows(key, got, 0)
	return got
}

// requireWindows checks the invariants of every shard, and when the operation
// on key returned a count, at least lowestCount, so it reached the lock, that
// its shard caught up.
func (h *harness) requireWindows(key string, got, lowestCount int) {
	h.t.Helper()
	if got >= lowestCount {
		h.window.requireReached(h.t, h.sketch, key)
		return
	}
	h.window.require(h.t, h.sketch)
}

// requireBoundAgrees validates the model that supplies Mode B's upper bound
// against root on every op, which also vouches for it where it is the only
// reference, beyond root's ranges.
func (h *harness) requireBoundAgrees(c call, model, oracle int) {
	h.t.Helper()
	if model != oracle {
		h.t.Fatalf("model %v = %d, oracle %d: the reference model is wrong", c, model, oracle)
	}
}

// requireFinalCounts reads every key at the high-water mark, the last point
// where all of them can be compared, as root's fuzz target does.
func (h *harness) requireFinalCounts() {
	h.t.Helper()
	hw, ok := h.ref.HighWater()
	if !ok {
		return
	}
	for _, key := range testKeys {
		h.get(epochOfHighWater(h.sketch, hw), key)
	}
}

// epochOfHighWater converts the mark, in seconds, back to an epoch in the
// cache's unit, which the bucket divisor and precision give.
func epochOfHighWater(c *Cache, hw int64) int64 {
	return hw / c.precision * c.bucketDiv
}

func requireExact(h *harness, c call, got, want int) {
	h.t.Helper()
	if got != want {
		h.t.Fatalf("%v = %d, reference %d", c, got, want)
	}
	requireSameHighWater(h.t, c, h.sketch, h.ref)
	requireSameStats(h.t, c, h.sketch.Stats(), h.ref.Stats())
}

func requireBounded(h *harness, c call, got, want int) {
	h.t.Helper()
	if want < 0 || got < 0 {
		if got != want {
			h.t.Fatalf("%v = %d, oracle %d: rejections must be identical", c, got, want)
		}
		return
	}
	if got < want {
		h.t.Fatalf("%v = %d, below the exact count %d", c, got, want)
	}
	if total := h.bound.liveTotal(); got > total {
		h.t.Fatalf("%v = %d, above the %d live events of all keys", c, got, total)
	}
}

func requireSameHighWater(t *testing.T, c call, sketch *Cache, ref reference) {
	t.Helper()
	gotHW, gotOK := sketch.HighWater()
	wantHW, wantOK := ref.HighWater()
	if gotHW != wantHW || gotOK != wantOK {
		t.Fatalf("after %v HighWater() = (%d, %t), reference (%d, %t)", c, gotHW, gotOK, wantHW, wantOK)
	}
}

// requireSameStats compares the counters both caches define the same way. The
// reference splits GetHit and GetMiss by key presence and the sketch by
// estimate, so only their sum is comparable.
func requireSameStats(t *testing.T, c call, got Stats, want slidingcache.Stats) {
	t.Helper()
	type sharedCounters struct {
		accepted, late, future, outOfRange, getLate, getFuture, getCounted uint64
	}
	gotC := sharedCounters{
		got.Accepted, got.Late, got.Future, got.OutOfRange, got.GetLate, got.GetFuture, got.GetHit + got.GetMiss,
	}
	wantC := sharedCounters{
		want.Accepted,
		want.Late,
		want.Future,
		want.OutOfRange,
		want.GetLate,
		want.GetFuture,
		want.GetHit + want.GetMiss,
	}
	if gotC != wantC {
		t.Fatalf("after %v Stats = %+v, reference %+v", c, gotC, wantC)
	}
}
