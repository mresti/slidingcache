package countminsketch

import (
	"math"
	"strconv"
	"testing"

	"github.com/mresti/slidingcache"
)

// rangeConfigs are the accepted ranges at both ends of each family, the root
// equivalents and the ones beyond, plus the disabled guard.
func rangeConfigs() map[string]Config {
	configs := map[string]Config{"guard disabled": wideConfig(wideBits - 1)}
	for _, bits := range []int{minTimestampBits, defaultTimestampBits, 55, 56, 61, maxTimestampBits} {
		configs["TimestampBits "+strconv.Itoa(bits)] = secondsConfig(bits)
	}
	return configs
}

// TestRangeBoundsAgreeWithModel walks each range across both of its ends, where
// one step accepts and the next rejects, against the naive model.
func TestRangeBoundsAgreeWithModel(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		for name, cfg := range rangeConfigs() {
			cfg.Shards = shards
			t.Run(name, func(t *testing.T) {
				bits := uint(cfg.timestampBits())
				low := newExactHarness(t, cfg, newReferenceModel(cfg))
				for epoch := -int64(1)<<bits - 30; epoch <= -int64(1)<<bits+30; epoch += 3 {
					low.store(epoch, testKeys[int(epoch&3)])
					low.get(epoch, testKeys[0])
				}
				high := newExactHarness(t, cfg, newReferenceModel(cfg))
				for epoch := int64(1)<<bits - 30; epoch <= int64(1)<<bits+30; epoch += 3 {
					high.store(epoch, testKeys[int(epoch&3)])
					high.get(epoch, testKeys[0])
				}
				high.requireFinalCounts()
			})
		}
	})
}

// TestInt64ExtremeEpochs feeds the most extreme epochs in every unit to a fresh
// cache of every range. Whether each is accepted comes from the model, which
// computes buckets without wraparound: in seconds with 10-second buckets,
// bucket(math.MinInt64) overflows int64 and must end in -1 with the mark
// untouched, never in a bogus timestamp; in millis and nanos the buckets fit
// and the widest ranges accept them.
func TestInt64ExtremeEpochs(t *testing.T) {
	units := map[string]slidingcache.EpochUnit{
		"seconds": slidingcache.EpochInSeconds,
		"millis":  slidingcache.EpochInMillis,
		"nanos":   slidingcache.EpochInNanos,
	}
	epochs := []int64{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 9, math.MaxInt64 - 1, math.MaxInt64}
	forEachShardCount(t, func(t *testing.T, shards int) {
		for rangeName, cfg := range rangeConfigs() {
			cfg.Shards = shards
			for unitName, unit := range units {
				cfg.EpochUnit = unit
				for _, epoch := range epochs {
					t.Run(rangeName+"/"+unitName+"/"+strconv.FormatInt(epoch, 10), func(t *testing.T) {
						model := newReferenceModel(cfg)
						h := newExactHarness(t, cfg, model)
						got := h.store(epoch, testKeys[0])
						if _, representable := model.bucket(epoch); !representable {
							if _, ok := h.sketch.HighWater(); got != slidingcache.LateEvent || ok {
								t.Fatalf("Store(%d) = %d with HW observed %t, want -1 and HW untouched", epoch, got, ok)
							}
						}
						h.get(epoch, testKeys[0])
					})
				}
			}
		}
	})
}

// TestSecondsAtMinInt64OverflowsBucket pins the case TestInt64ExtremeEpochs
// relies on: with the guard disabled, the widest range, the overflowing bucket
// is still rejected.
func TestSecondsAtMinInt64OverflowsBucket(t *testing.T) {
	cfg := wideConfig(wideBits - 1)
	c := newTestCache(t, cfg)
	if ts := c.bucket(math.MinInt64); ts > 0 {
		t.Logf("bucket(math.MinInt64) wraps to %d", ts)
	} else {
		t.Fatalf("bucket(math.MinInt64) = %d: the test no longer exercises the overflow", ts)
	}
	if got := c.Store(math.MinInt64, "k"); got != slidingcache.LateEvent {
		t.Fatalf("Store(math.MinInt64) = %d, want -1", got)
	}
	if got := c.Get(math.MinInt64, "k"); got != slidingcache.LateEvent {
		t.Fatalf("Get(math.MinInt64) = %d, want -1", got)
	}
	if _, ok := c.HighWater(); ok {
		t.Fatal("an unrepresentable epoch advanced the high-water mark")
	}
	if stats := c.Stats(); stats.OutOfRange != 1 || stats.GetLate != 1 {
		t.Fatalf("Stats() = %+v, want OutOfRange 1 and GetLate 1", stats)
	}
	if c.Store(1_700_000_000, "k") != 1 {
		t.Fatal("the cache no longer accepts a real epoch after the rejection")
	}
}
