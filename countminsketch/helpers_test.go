package countminsketch

import (
	"math"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

// testKeys are the only keys the differential tests use, so that the
// collision-free hash can map each of them to a fixed 64-bit value.
var testKeys = [...]string{"key-0", "key-1", "key-2", "key-3"}

// splitmix64 turns a small id into a well-mixed 64-bit hash, the same mixer
// internal/countmin's tests use for their fixed hashes.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

// exactHash is the WithHashFunc of Mode A: a fixed mapping from testKeys to
// fixed hashes, which requireCollisionFree proves collision-free for the
// sketch dimensions in use. Any other key is a bug in the test.
func exactHash(key string) uint64 {
	for i, k := range testKeys {
		if k == key {
			return splitmix64(uint64(i) + 1)
		}
	}
	panic("exactHash: key " + key + " is not one of testKeys")
}

func exactHashes() []uint64 {
	hashes := make([]uint64, len(testKeys))
	for i, key := range testKeys {
		hashes[i] = exactHash(key)
	}
	return hashes
}

// requireCollisionFree proves the precondition of Mode A instead of assuming
// it: with every hash added once, each must be alone in at least one row, which
// is exactly when its estimate is 1. Then every estimate in the window is the
// exact count, whatever the counts of the other keys.
func requireCollisionFree(t *testing.T, cfg Config, hashes []uint64) {
	t.Helper()
	width, depth, err := cfg.dimensions()
	if err != nil {
		t.Fatalf("dimensions: %v", err)
	}
	agg, err := countmin.NewAggregate(width, depth)
	if err != nil {
		t.Fatalf("NewAggregate(%d, %d): %v", width, depth, err)
	}
	slot := agg.NewSlot()
	for _, h := range hashes {
		agg.Add(slot, h)
	}
	for i, h := range hashes {
		if got := agg.Count(h); got != 1 {
			t.Fatalf("hash %d (%#x) estimates %d after one add each at %dx%d, want 1: the hashes collide",
				i, h, got, width, depth)
		}
	}
}

func newTestCache(t *testing.T, cfg Config, opts ...Option) *Cache {
	t.Helper()
	c, err := New(cfg, opts...)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// oracleConfig is the root configuration whose window, guard and range match
// cfg: TimestampBits 39..55 are CountBits 24..8. The sweep interval is long so
// the janitor never runs during a test; it changes no observable result anyway.
func oracleConfig(t *testing.T, cfg Config) slidingcache.Config {
	t.Helper()
	bits := cfg.timestampBits()
	if bits > 55 {
		t.Fatalf("TimestampBits %d has no root equivalent; compare against the model instead", bits)
	}
	return slidingcache.Config{
		Precision:     cfg.Precision,
		WindowSize:    cfg.WindowSize,
		EpochUnit:     cfg.EpochUnit,
		MaxFutureSkew: cfg.MaxFutureSkew,
		Clock:         cfg.Clock,
		CountBits:     63 - bits,
		SweepInterval: time.Hour,
	}
}

func newOracle(t *testing.T, cfg slidingcache.Config) *slidingcache.Cache {
	t.Helper()
	oracle, err := slidingcache.New(cfg)
	if err != nil {
		t.Fatalf("slidingcache.New(%+v): %v", cfg, err)
	}
	t.Cleanup(func() { _ = oracle.Close() })
	return oracle
}

// secondsConfig is the window most tests use: 10-second buckets, a 60-second
// window, epochs in seconds and a small sketch, which keeps a fuzz execution
// cheap while 4 fixed hashes stay collision-free.
func secondsConfig(timestampBits int) Config {
	return Config{
		Precision:     10 * time.Second,
		WindowSize:    60 * time.Second,
		EpochUnit:     slidingcache.EpochInSeconds,
		Epsilon:       0.01,
		TimestampBits: timestampBits,
	}
}

// fakeClock is a deterministic Config.Clock that counts how often the cache
// consulted it.
type fakeClock struct {
	now   int64
	reads int
}

func (f *fakeClock) read() int64 {
	f.reads++
	return f.now
}

// windowChecker checks I1, I2 and I3 of a cache's window after every
// operation. It remembers the applied cutoff of the previous check for I3, and
// keeps an aggregate of the window's dimensions, which countmin does not
// expose, to rebuild for I1; reusing it keeps the fuzz targets off the
// allocator.
type windowChecker struct {
	rebuilt *countmin.Aggregate
	hashes  []uint64
	applied int64
}

func newWindowChecker(t *testing.T, cfg Config, hashes []uint64) *windowChecker {
	t.Helper()
	width, depth, err := cfg.dimensions()
	if err != nil {
		t.Fatalf("dimensions: %v", err)
	}
	rebuilt, err := countmin.NewAggregate(width, depth)
	if err != nil {
		t.Fatalf("NewAggregate(%d, %d): %v", width, depth, err)
	}
	return &windowChecker{rebuilt: rebuilt, hashes: hashes, applied: math.MinInt64}
}

func (wc *windowChecker) require(t *testing.T, c *Cache) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	w := c.window
	highWater := c.highWater.Load()
	if w.applied < wc.applied {
		t.Fatalf("I3: applied went back from %d to %d", wc.applied, w.applied)
	}
	wc.applied = w.applied
	if limit := cutoffFor(highWater, c.windowSize); w.applied > limit {
		t.Fatalf("applied %d is ahead of the cutoff %d of HW %d", w.applied, limit, highWater)
	}
	requireSlotsPlacedAndBounded(t, w, highWater)
	wc.requireAggregateIsSumOfLiveSlots(t, w)
}

// requireSlotsPlacedAndBounded checks I2 slot by slot: a live slot sits at the
// index of its timestamp inside (applied, HW], and a slot that has been used
// and is no longer live was expired at or below applied and holds nothing.
func requireSlotsPlacedAndBounded(t *testing.T, w *window, highWater int64) {
	t.Helper()
	for i := range w.slots {
		s := &w.slots[i]
		switch {
		case s.live:
			if s.timestamp <= w.applied || s.timestamp > highWater {
				t.Fatalf("I2: live slot %d has timestamp %d outside (%d, %d]", i, s.timestamp, w.applied, highWater)
			}
			if w.index(s.timestamp) != i {
				t.Fatalf(
					"I2: live slot %d holds timestamp %d, which belongs at %d",
					i,
					s.timestamp,
					w.index(s.timestamp),
				)
			}
		case s.sketch != nil:
			if s.timestamp > w.applied {
				t.Fatalf("I2: slot %d with timestamp %d is not live though above applied %d", i, s.timestamp, w.applied)
			}
			if total := s.sketch.Total(); total != 0 {
				t.Fatalf("expired slot %d still holds %d events", i, total)
			}
		}
	}
}

// requireAggregateIsSumOfLiveSlots checks I1 through countmin's exported API:
// an aggregate rebuilt from the live slots must give every key's hash the same
// estimate as the window's.
func (wc *windowChecker) requireAggregateIsSumOfLiveSlots(t *testing.T, w *window) {
	t.Helper()
	rebuilt := wc.rebuilt
	rebuilt.Reset()
	for i := range w.slots {
		if s := &w.slots[i]; s.live {
			if err := rebuilt.AddSketch(s.sketch); err != nil {
				t.Fatalf("AddSketch(slot %d): %v", i, err)
			}
		}
	}
	for _, h := range wc.hashes {
		if got, want := w.agg.Count(h), rebuilt.Count(h); got != want {
			t.Fatalf("I1: window aggregate estimates %d for %#x, the live slots sum to %d", got, h, want)
		}
	}
}
