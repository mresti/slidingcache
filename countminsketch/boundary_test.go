package countminsketch

import (
	"strconv"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

type boundaryCase struct {
	name   string
	cfg    Config
	newRef func(*testing.T, Config) reference
}

// boundaryCases use one-second buckets, so the exact ends of every range,
// 2^tb - 1 and -2^tb, are bucket timestamps a Store can land on.
func boundaryCases() []boundaryCase {
	againstOracle := func(t *testing.T, cfg Config) reference { return newOracle(t, oracleConfig(t, cfg)) }
	againstModel := func(_ *testing.T, cfg Config) reference { return newReferenceModel(cfg) }
	cases := []boundaryCase{{"guard disabled", wideConfig(wideBits - 1), againstModel}}
	for _, bits := range []int{minTimestampBits, defaultTimestampBits, 55} {
		cases = append(cases, boundaryCase{"TimestampBits " + strconv.Itoa(bits), secondsConfig(bits), againstOracle})
	}
	for _, bits := range []int{56, maxTimestampBits} {
		cases = append(cases, boundaryCase{"TimestampBits " + strconv.Itoa(bits), secondsConfig(bits), againstModel})
	}
	for i := range cases {
		cases[i].cfg.Precision = time.Second
	}
	return cases
}

// TestRangeEndsAreExact stores and queries, each on a fresh cache, exactly at
// both ends of every range and one second beyond.
func TestRangeEndsAreExact(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		for _, bc := range boundaryCases() {
			t.Run(bc.name, func(t *testing.T) {
				cfg := bc.cfg
				cfg.Shards = shards
				bits := uint(cfg.timestampBits())
				highest, lowest := int64(1)<<bits-1, -int64(1)<<bits
				fresh := func() *harness { return newExactHarness(t, cfg, bc.newRef(t, cfg)) }

				for _, edge := range []int64{highest, lowest} {
					h := fresh()
					requireReturn(t, "Store at "+strconv.FormatInt(edge, 10), h.store(edge, testKeys[0]), 1)
					if hw, ok := h.sketch.HighWater(); hw != edge || !ok {
						t.Fatalf("HighWater() = (%d, %t) after Store(%d), want (%d, true)", hw, ok, edge, edge)
					}
					requireReturn(
						t,
						"Get at "+strconv.FormatInt(edge, 10)+" on a fresh cache",
						fresh().get(edge, testKeys[0]),
						0,
					)
				}
				for _, beyond := range []int64{highest + 1, lowest - 1} {
					h := fresh()
					requireReturn(
						t,
						"Store at "+strconv.FormatInt(beyond, 10),
						h.store(beyond, testKeys[0]),
						slidingcache.LateEvent,
					)
					if _, ok := h.sketch.HighWater(); ok {
						t.Fatalf("Store(%d) beyond the range moved the high-water mark", beyond)
					}
					requireReturn(
						t,
						"Get at "+strconv.FormatInt(beyond, 10),
						h.get(beyond, testKeys[0]),
						slidingcache.LateEvent,
					)
					if stats := h.sketch.Stats(); stats.OutOfRange != 1 || stats.GetLate != 1 {
						t.Fatalf("Stats() = %+v, want OutOfRange 1 and GetLate 1", stats)
					}
				}
			})
		}
	})
}

// TestFutureGuardBoundaryIsInclusive lands exactly on bucket(clock) +
// MaxFutureSkew, which is accepted, and one bucket past it, which is not, for
// Store and for Get on timestamps above the mark.
func TestFutureGuardBoundaryIsInclusive(t *testing.T) {
	forEachShardCount(t, func(t *testing.T, shards int) {
		sketchClock, oracleClock := &fakeClock{now: 1_005}, &fakeClock{now: 1_005}
		cfg := shardedSecondsConfig(0, shards)
		cfg.MaxFutureSkew = 30 * time.Second
		cfg.Clock = oracleClock.read
		oracle := newOracle(t, oracleConfig(t, cfg))
		cfg.Clock = sketchClock.read
		h := newExactHarness(t, cfg, oracle)
		k0 := testKeys[0]

		requireReturn(t, "Store at clock+skew", h.store(1_030, k0), 1)
		if hw, _ := h.sketch.HighWater(); hw != 1_030 {
			t.Fatalf("HighWater() = %d, want 1030 from the boundary Store", hw)
		}
		requireReturn(t, "Store one bucket past clock+skew", h.store(1_040, k0), slidingcache.FutureEvent)
		if hw, _ := h.sketch.HighWater(); hw != 1_030 {
			t.Fatalf("HighWater() = %d after a future Store, want 1030", hw)
		}

		sketchClock.now, oracleClock.now = 1_015, 1_015
		requireReturn(t, "Get at clock+skew above the mark", h.get(1_040, k0), 1)
		requireReturn(t, "Get one bucket past clock+skew", h.get(1_050, k0), slidingcache.FutureEvent)
		if stats := h.sketch.Stats(); stats.Future != 1 || stats.GetFuture != 1 {
			t.Fatalf("Stats() = %+v, want Future 1 and GetFuture 1", stats)
		}
	})
}
