package countminsketch

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

func liveSlots(s *shard) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := 0
	for _, timestamp := range s.window.timestamps {
		if timestamp != deadSlot {
			live++
		}
	}
	return live
}

func appliedOf(s *shard) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.window.applied
}

// TestLaggingShardCatchesUpOnItsNextOperation moves the mark past the window
// through another shard's key, which leaves the first key's shard holding its
// expired buckets. Whichever operation reaches that shard next must expire them
// before answering, so the lag never shows in a result.
func TestLaggingShardCatchesUpOnItsNextOperation(t *testing.T) {
	catchUps := map[string]struct {
		op   func(h *harness, key string) int
		want int
	}{
		"Get":   {func(h *harness, key string) int { return h.get(1_000, key) }, 0},
		"Store": {func(h *harness, key string) int { return h.store(1_000, key) }, 1},
	}
	for name, catchUp := range catchUps {
		t.Run(name, func(t *testing.T) {
			h := newOracleHarness(t, shardedSecondsConfig(0, 2))
			lagging, advancing := testKeys[0], testKeys[2]
			laggingShard := h.sketch.shardOf(lagging)
			if laggingShard == h.sketch.shardOf(advancing) {
				t.Fatalf("%s and %s share a shard: pick other exactHashIDs", lagging, advancing)
			}
			h.store(100, lagging)
			h.store(110, lagging)
			h.store(1_000, advancing)

			cutoff := cutoffFor(1_000, h.sketch.windowSize)
			if applied := appliedOf(laggingShard); applied >= cutoff {
				t.Fatalf("the shard of %s applied %d, want it behind the cutoff %d", lagging, applied, cutoff)
			}
			if live := liveSlots(laggingShard); live != 2 {
				t.Fatalf("the lagging shard has %d live slots, want its 2 expired buckets", live)
			}

			requireReturn(t, name+" on the lagging shard", catchUp.op(h, lagging), catchUp.want)
			if applied := appliedOf(laggingShard); applied != cutoff {
				t.Fatalf("the shard of %s applied %d after its next operation, want %d", lagging, applied, cutoff)
			}
			requireReturn(t, "Store at the old epoch", h.store(100, lagging), slidingcache.LateEvent)
			if live := liveSlots(laggingShard); live != catchUp.want {
				t.Fatalf("the caught-up shard has %d live slots, want %d", live, catchUp.want)
			}
		})
	}
}

type acceptedEvent struct {
	key    string
	bucket int64
}

type concurrentLoad struct {
	stores, gets uint64
	accepted     []acceptedEvent
}

// runConcurrentLoad drives c from one goroutine with a deterministic sequence:
// random keys, a third of the calls Gets, epochs that advance at a speed of
// their own, so the goroutines move the mark at different paces, with a jitter
// that sends some calls out of order and some behind the window.
func runConcurrentLoad(c *Cache, seed uint64, ops int) concurrentLoad {
	rng := rand.New(rand.NewPCG(seed, 1))
	speed := int64(1 + seed%3)
	var load concurrentLoad
	for i := range ops {
		key := testKeys[rng.IntN(len(testKeys))]
		epoch := int64(i)*speed + rng.Int64N(80) - 40
		if rng.IntN(3) == 0 {
			c.Get(epoch, key)
			load.gets++
			continue
		}
		load.stores++
		if c.Store(epoch, key) >= 1 {
			load.accepted = append(load.accepted, acceptedEvent{key, c.bucket(epoch)})
		}
	}
	return load
}

// TestConcurrentShardedStoresMatchAcceptedEvents runs Mode A under -race: with
// a collision-free hash over keys that share a shard and keys that do not,
// the counters must partition the calls, every shard must keep its invariants,
// and each key's final count must be exactly its accepted events still live.
func TestConcurrentShardedStoresMatchAcceptedEvents(t *testing.T) {
	const (
		goroutines = 8
		opsEach    = 2_000
	)
	cfg := shardedSecondsConfig(0, 4)
	requireCollisionFree(t, cfg, exactHashes())
	c := newTestCache(t, cfg, WithHashFunc(exactHash))
	requireShardsSharedAndSpanned(t, c)

	loads := make([]concurrentLoad, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() { loads[g] = runConcurrentLoad(c, uint64(g), opsEach) })
	}
	wg.Wait()

	var stores, gets uint64
	for _, load := range loads {
		stores += load.stores
		gets += load.gets
	}
	requirePartitions(t, c.Stats(), stores, gets)
	newWindowChecker().require(t, c)

	hw, _ := c.HighWater()
	cutoff := cutoffFor(hw, c.windowSize)
	for _, key := range testKeys {
		want := 0
		for _, load := range loads {
			for _, event := range load.accepted {
				if event.key == key && event.bucket > cutoff {
					want++
				}
			}
		}
		requireReturn(t, "final count of "+key, c.Get(epochOfHighWater(c, hw), key), want)
	}
	newWindowChecker().require(t, c)
}

// TestStatsSumTheShards drives keys of two different shards to every outcome,
// b's rejections twice as often as a's: each shard counts the outcomes of its
// own keys, wherever they were decided, and Stats is their sum.
func TestStatsSumTheShards(t *testing.T) {
	clock := &fakeClock{now: 1_000}
	cfg := shardedSecondsConfig(0, 2)
	cfg.MaxFutureSkew, cfg.Clock = 30*time.Second, clock.read
	c := newTestCache(t, cfg, WithHashFunc(exactHash))
	a, b := testKeys[0], testKeys[2]
	if c.shardOf(a) == c.shardOf(b) {
		t.Fatalf("%s and %s share a shard: pick other exactHashIDs", a, b)
	}

	requireReturn(t, "Store a", c.Store(1_000, a), 1)
	requireReturn(t, "Store b", c.Store(1_000, b), 1)
	requireReturn(t, "Store a again", c.Store(1_001, a), 2)
	for key, repeats := range map[string]int{a: 1, b: 2} {
		for range repeats {
			requireReturn(t, "late Store "+key, c.Store(900, key), slidingcache.LateEvent)
			requireReturn(t, "future Store "+key, c.Store(5_000, key), slidingcache.FutureEvent)
			requireReturn(t, "out-of-range Store "+key, c.Store(1<<50, key), slidingcache.LateEvent)
			requireReturn(t, "late Get "+key, c.Get(900, key), slidingcache.LateEvent)
			requireReturn(t, "future Get "+key, c.Get(5_000, key), slidingcache.FutureEvent)
			requireReturn(t, "out-of-range Get "+key, c.Get(-1<<50, key), slidingcache.LateEvent)
		}
	}
	requireReturn(t, "Get a", c.Get(1_000, a), 2)
	clock.now = 1_060
	requireReturn(t, "Store a expiring b's bucket", c.Store(1_060, a), 1)
	requireReturn(t, "Get b after expiry", c.Get(1_060, b), 0)

	shardStats := func(key string) Stats {
		var stats Stats
		c.shardOf(key).addTo(&stats)
		return stats
	}
	wantA := Stats{Accepted: 3, Late: 1, Future: 1, OutOfRange: 1, GetHit: 1, GetLate: 2, GetFuture: 1}
	wantB := Stats{Accepted: 1, Late: 2, Future: 2, OutOfRange: 2, GetMiss: 1, GetLate: 4, GetFuture: 2}
	if got := shardStats(a); got != wantA {
		t.Fatalf("shard of %s counts %+v, want %+v", a, got, wantA)
	}
	if got := shardStats(b); got != wantB {
		t.Fatalf("shard of %s counts %+v, want %+v", b, got, wantB)
	}
	want := Stats{
		Accepted: 4, Late: 3, Future: 3, OutOfRange: 3,
		GetHit: 1, GetMiss: 1, GetLate: 6, GetFuture: 3,
	}
	if got := c.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want the sum of the shards %+v", got, want)
	}
}
