package countminsketch

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

// The scenarios here run in Mode A against root, which checks every return,
// the mark and the counters; the expected values spelled out in each test are
// there to make the scenario readable.

func newOracleHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	return newExactHarness(t, cfg, newOracle(t, oracleConfig(t, cfg)))
}

func requireReturn(t *testing.T, what string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %d, want %d", what, got, want)
	}
}

func TestBucketExactlyWindowBehindIsExpired(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
	k0, k1 := testKeys[0], testKeys[1]

	requireReturn(t, "first event", h.store(100, k0), 1)
	requireReturn(t, "event one bucket short of the window", h.store(150, k1), 1)
	requireReturn(t, "k0 still live", h.get(150, k0), 1)

	h.store(160, k1)
	requireReturn(t, "k0 at HW - WindowSize", h.get(160, k0), 0)
	requireReturn(t, "query at the expired bucket", h.get(100, k0), slidingcache.LateEvent)
	requireReturn(t, "store into the expired bucket", h.store(105, k0), slidingcache.LateEvent)
	requireReturn(t, "first live bucket", h.store(110, k0), 1)
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newOracleHarness(t, secondsConfig(0))
			k0, k1 := testKeys[0], testKeys[1]
			h.store(1_000, k0)
			h.store(1_005, k0)

			requireReturn(t, "event after the jump", h.store(1_000+tt.jump, k1), 1)
			requireReturn(t, "events before the jump", h.get(1_000+tt.jump, k0), tt.wantOld)
			requireReturn(t, "first event of k0 after the jump", h.store(1_000+tt.jump, k0), tt.wantOld+1)
		})
	}
}

func TestOutOfOrderEventIntoOlderLiveBucket(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
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
}

func TestLateEventLeavesCountsAndHighWaterUntouched(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
	k0, k1 := testKeys[0], testKeys[1]
	h.store(200, k0)
	h.store(200, k1)

	requireReturn(t, "late store", h.store(100, k0), slidingcache.LateEvent)
	requireReturn(t, "late get", h.get(140, k1), slidingcache.LateEvent)
	requireReturn(t, "k0 after the late store", h.get(200, k0), 1)
	if hw, ok := h.sketch.HighWater(); hw != 200 || !ok {
		t.Fatalf("HighWater() = (%d, %t), want (200, true)", hw, ok)
	}
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := secondsConfig(0)
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
}

func TestTimestampsCrossingZero(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
	for epoch := int64(-95); epoch <= 95; epoch += 7 {
		h.store(epoch, testKeys[int(epoch&3)])
		h.get(epoch, testKeys[0])
	}
	requireReturn(t, "-1 lands in the bucket of -10", h.store(-1, testKeys[1]), slidingcache.LateEvent)
	h.requireFinalCounts()
}

func TestSlotsAreReusedOverSeveralTurnsOfTheRing(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
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
	allocated := 0
	for i := range h.sketch.window.slots {
		if h.sketch.window.slots[i].sketch != nil {
			allocated++
		}
	}
	if allocated != buckets {
		t.Fatalf("%d slots own a sketch after %d turns, want each of the %d slots once", allocated, turns, buckets)
	}
}

func TestFreshCacheGetReturnsZero(t *testing.T) {
	h := newOracleHarness(t, secondsConfig(0))
	requireReturn(t, "Get on a fresh cache", h.get(1_700_000_000, testKeys[0]), 0)
	if _, ok := h.sketch.HighWater(); ok {
		t.Fatal("Get advanced the high-water mark of a fresh cache")
	}
	if stats := h.sketch.Stats(); stats.GetMiss != 1 {
		t.Fatalf("Stats().GetMiss = %d, want 1", stats.GetMiss)
	}
}

// TestFreshCacheAcceptsFirstStoreAtEitherEndOfTheRange covers the first expiry
// of a fresh cache, which starts from applied near math.MinInt64: it must
// neither overflow nor walk bucket by bucket towards the first timestamp.
func TestFreshCacheAcceptsFirstStoreAtEitherEndOfTheRange(t *testing.T) {
	configs := map[string]Config{"guard disabled": wideConfig(wideBits - 1)}
	for _, bits := range []int{minTimestampBits, defaultTimestampBits, 55, 56, maxTimestampBits} {
		configs["TimestampBits "+strconv.Itoa(bits)] = secondsConfig(bits)
	}
	for name, cfg := range configs {
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
	w := newWindow(6, 10, nil, math.MinInt64)
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
