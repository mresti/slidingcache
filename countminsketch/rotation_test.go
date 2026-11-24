package countminsketch

import (
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

// The scenarios here run in Mode A against root, which checks every return,
// the mark and the counters; the expected values spelled out in each test are
// there to make the scenario readable.

func newOracleHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	return newExactHarness(t, cfg, newOracle(t, oracleConfig(t, cfg)))
}

// shardCounts are the Config.Shards every scenario runs with: one shard, as
// before sharding, a few shards that each hold several keys, and the default of
// secondsConfig, where testKeys share one shard and leave the others lagging.
var shardCounts = []int{1, 4, 16}

// forEachShardCount runs a scenario once per entry of shardCounts.
func forEachShardCount(t *testing.T, scenario func(t *testing.T, shards int)) {
	t.Helper()
	for _, shards := range shardCounts {
		t.Run("Shards="+strconv.Itoa(shards), func(t *testing.T) { scenario(t, shards) })
	}
}

func shardedSecondsConfig(timestampBits, shards int) Config {
	cfg := secondsConfig(timestampBits)
	cfg.Shards = shards
	return cfg
}

func requireReturn(t *testing.T, what string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %d, want %d", what, got, want)
	}
}

func TestBucketExactlyWindowBehindIsExpired(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		k0, k1 := testKeys[0], testKeys[1]

		requireReturn(t, "first event", h.store(100, k0), 1)
		requireReturn(t, "event one bucket short of the window", h.store(150, k1), 1)
		requireReturn(t, "k0 still live", h.get(150, k0), 1)

		h.store(160, k1)
		requireReturn(t, "k0 at HW - WindowSize", h.get(160, k0), 0)
		requireReturn(t, "query at the expired bucket", h.get(100, k0), slidingcache.LateEvent)
		requireReturn(t, "store into the expired bucket", h.store(105, k0), slidingcache.LateEvent)
		requireReturn(t, "first live bucket", h.store(110, k0), 1)
	})
}

func TestHighWaterJumps(t *testing.T) {
	tests := []struct {
		name    string
		jump    int64
		wantOld int
	}{
		{"less than the window", 30, 2},
		{"exactly the window", 60, 0},
		{"far beyond the window", 1_000_000, 0},
	}
	forEachShardCount(t, func(t *testing.T, shards int) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				h := newOracleHarness(t, shardedSecondsConfig(0, shards))
				k0, k1 := testKeys[0], testKeys[1]
				h.store(1_000, k0)
				h.store(1_005, k0)

				requireReturn(t, "event after the jump", h.store(1_000+tt.jump, k1), 1)
				requireReturn(t, "events before the jump", h.get(1_000+tt.jump, k0), tt.wantOld)
				requireReturn(t, "first event of k0 after the jump", h.store(1_000+tt.jump, k0), tt.wantOld+1)
			})
		}
	})
}

func TestOutOfOrderEventIntoOlderLiveBucket(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		k0 := testKeys[0]
		h.store(100, k0)
		h.store(140, k0)

		requireReturn(t, "older live bucket", h.store(110, k0), 3)
		requireReturn(t, "back into the oldest bucket", h.store(101, k0), 4)
		if hw, _ := h.sketch.HighWater(); hw != 140 {
			t.Fatalf("HighWater() = %d after out-of-order events, want 140", hw)
		}

		h.store(170, k0)
		requireReturn(t, "after the buckets at 100 and 110 expire", h.get(170, k0), 2)
	})
}

func TestLateEventLeavesCountsAndHighWaterUntouched(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		k0, k1 := testKeys[0], testKeys[1]
		h.store(200, k0)
		h.store(200, k1)

		requireReturn(t, "late store", h.store(100, k0), slidingcache.LateEvent)
		requireReturn(t, "late get", h.get(140, k1), slidingcache.LateEvent)
		requireReturn(t, "k0 after the late store", h.get(200, k0), 1)
		if hw, ok := h.sketch.HighWater(); hw != 200 || !ok {
			t.Fatalf("HighWater() = (%d, %t), want (200, true)", hw, ok)
		}
	})
}

func TestWindowNotAMultipleOfPrecision(t *testing.T) {
	tests := []struct {
		name                  string
		precision, windowSize time.Duration
	}{
		{"10s buckets in a 65s window", 10 * time.Second, 65 * time.Second},
		{"3s buckets in a 10s window", 3 * time.Second, 10 * time.Second},
		{"one bucket wider than half the window", 7 * time.Second, 13 * time.Second},
	}
	forEachShardCount(t, func(t *testing.T, shards int) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := shardedSecondsConfig(0, shards)
				cfg.Precision, cfg.WindowSize = tt.precision, tt.windowSize
				h := newOracleHarness(t, cfg)
				step := int64(tt.precision / time.Second)
				// Every bucket, so the live buckets fill the ring, which has one slot
				// more than whole buckets fit in the window.
				for epoch := int64(0); epoch < 20*step; epoch += step {
					for _, key := range testKeys[:2] {
						h.store(epoch, key)
					}
					h.get(epoch, testKeys[2])
				}
			})
		}
	})
}

func TestTimestampsCrossingZero(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		for epoch := int64(-95); epoch <= 95; epoch += 7 {
			h.store(epoch, testKeys[int(epoch&3)])
			h.get(epoch, testKeys[0])
		}
		requireReturn(t, "-1 lands in the bucket of -10", h.store(-1, testKeys[1]), slidingcache.LateEvent)
		h.requireFinalCounts()
	})
}

func TestSlotsAreReusedOverSeveralTurnsOfTheRing(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		k0, k1 := testKeys[0], testKeys[1]
		const turns, buckets = 5, 6
		for epoch := int64(0); epoch < turns*buckets*10; epoch += 10 {
			h.store(epoch, k0)
			h.store(epoch+1, k0)
			h.store(epoch, k1)
			if epoch >= buckets*10 {
				requireReturn(t, "k0 over a full window", h.get(epoch, k0), 2*buckets)
				requireReturn(t, "k1 over a full window", h.get(epoch, k1), buckets)
			}
		}
		// k0 and k1 share a shard, whose ring every bucket has reached.
		w := &h.sketch.shardOf(k0).window
		live := 0
		for _, timestamp := range w.timestamps {
			if timestamp != deadSlot {
				live++
			}
		}
		if slots := w.ring.Slots(); slots != buckets || live != buckets {
			t.Fatalf("%d of %d slots live after %d turns, want all of the %d buckets", live, slots, turns, buckets)
		}
	})
}

func TestFreshCacheGetReturnsZero(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		h := newOracleHarness(t, shardedSecondsConfig(0, shards))
		requireReturn(t, "Get on a fresh cache", h.get(1_700_000_000, testKeys[0]), 0)
		if _, ok := h.sketch.HighWater(); ok {
			t.Fatal("Get advanced the high-water mark of a fresh cache")
		}
		if stats := h.sketch.Stats(); stats.GetMiss != 1 {
			t.Fatalf("Stats().GetMiss = %d, want 1", stats.GetMiss)
		}
	})
}

// TestFreshCacheAcceptsFirstStoreAtEitherEndOfTheRange covers the first expiry
// of a fresh cache, which starts from applied near math.MinInt64: it must
// neither overflow nor walk bucket by bucket towards the first timestamp.
func TestFreshCacheAcceptsFirstStoreAtEitherEndOfTheRange(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		configs := map[string]Config{"guard disabled": wideConfig(wideBits - 1)}
		for _, bits := range []int{minTimestampBits, defaultTimestampBits, 55, 56, maxTimestampBits} {
			configs["TimestampBits "+strconv.Itoa(bits)] = secondsConfig(bits)
		}
		for name, cfg := range configs {
			cfg.Shards = shards
			bits := uint(cfg.timestampBits())
			precision := int64(cfg.Precision / time.Second)
			lowestBucket := -(int64(1) << bits / precision) * precision
			for _, epoch := range []int64{lowestBucket, 1<<bits - 1} {
				t.Run(name+" at "+strconv.FormatInt(epoch, 10), func(t *testing.T) {
					h := newExactHarness(t, cfg, newReferenceModel(cfg))
					if got := h.store(epoch, testKeys[0]); got != 1 {
						t.Fatalf("first Store(%d) = %d, want 1", epoch, got)
					}
					requireReturn(t, "Get after the first store", h.get(epoch, testKeys[0]), 1)
				})
			}
		}
	})
}

// TestGetExpiresWhatNoStoreHasYet reaches the window after a Store has moved
// the mark but before it took the lock, through a hash that queries from that
// gap. The Get must expire the old bucket itself rather than count it.
func TestGetExpiresWhatNoStoreHasYet(t *testing.T) {
	var c *Cache
	var reentered bool
	inner := 0
	c = newTestCache(t, secondsConfig(0), WithHashFunc(func(key string) uint64 {
		if !reentered && key == testKeys[1] {
			reentered = true
			inner = c.Get(1_000, testKeys[0])
		}
		return exactHash(key)
	}))
	c.Store(100, testKeys[0])

	requireReturn(t, "Store moving the mark", c.Store(1_000, testKeys[1]), 1)
	requireReturn(t, "Get between the mark and the lock", inner, 0)
	requireReturn(t, "Get afterwards", c.Get(1_000, testKeys[0]), 0)
}

func TestIndexMapsConsecutiveBucketsOntoConsecutiveSlots(t *testing.T) {
	w := &window{timestamps: make([]int64, 6), precision: 10}
	tests := []struct {
		timestamp int64
		want      int
	}{
		{0, 0},
		{10, 1},
		{50, 5},
		{60, 0},
		{70, 1},
		{-10, 5},
		{-60, 0},
		{-70, 5},
		{-50, 1},
		{1_700_000_000, 2},
		{-4_611_686_018_427_387_890, 1},
		{4_611_686_018_427_387_890, 5},
	}
	for _, tt := range tests {
		if got := w.index(tt.timestamp); got != tt.want {
			t.Errorf("index(%d) = %d, want %d", tt.timestamp, got, tt.want)
		}
	}
}

// TestExpireToAStaleCutoffChangesNothing is the Get that computed its cutoff
// before a newer one was applied: the window must keep the newer state, slots,
// cells and applied cutoff alike, rather than move back (I3).
func TestExpireToAStaleCutoffChangesNothing(t *testing.T) {
	ring, err := countmin.NewRing(8, 2, 6)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	w := newWindow(ring, 10, math.MinInt64)
	for timestamp := int64(100); timestamp <= 150; timestamp += 10 {
		w.record(timestamp, splitmix64(uint64(timestamp)))
	}
	w.expireTo(110)
	applied, timestamps := w.applied, slices.Clone(w.timestamps)
	aggregate := ring.AppendAggregateCells(nil)
	var slotCells []uint32
	for slot := range ring.Slots() {
		slotCells = ring.AppendSlotCells(slotCells, slot)
	}

	for _, stale := range []int64{110, 100, math.MinInt64} {
		w.expireTo(stale)
		if w.applied != applied {
			t.Fatalf("expireTo(%d) moved applied from %d to %d", stale, applied, w.applied)
		}
		if !slices.Equal(w.timestamps, timestamps) {
			t.Fatalf("expireTo(%d) changed the live slots from %v to %v", stale, timestamps, w.timestamps)
		}
		var gotSlotCells []uint32
		for slot := range ring.Slots() {
			gotSlotCells = ring.AppendSlotCells(gotSlotCells, slot)
		}
		if !slices.Equal(gotSlotCells, slotCells) || !slices.Equal(ring.AppendAggregateCells(nil), aggregate) {
			t.Fatalf("expireTo(%d) changed the cells", stale)
		}
	}
}
