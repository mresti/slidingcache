package countminsketch

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

// The benchmarks below mirror the root package's under the same names, so the
// Makefile's families select them, with the same workloads and data. Each runs
// twice, impl=exact on the root cache as the reference and impl=sketch on this
// package, both through the slidingcache.SlidingCache interface so the call
// overhead is the same. The benchmarks after the mirrored ones are specific to
// the sketch.
//
// A mirrored workload that sets epoch = i opens a new one-second bucket on
// every op. The sketch then expires a whole slot per Store, shardWidth x depth
// cells, which is its worst case; BenchmarkStoreDense is the same traffic with
// many events per bucket.

// benchConfig is the root benchmarks' configuration: 1s precision, a one-hour
// window, second epochs. SweepInterval only applies to the exact arm.
func benchConfig() slidingcache.Config {
	return slidingcache.Config{
		Precision:     time.Second,
		WindowSize:    3600 * time.Second,
		EpochUnit:     slidingcache.EpochInSeconds,
		SweepInterval: time.Hour,
	}
}

func benchConfigShards(shards int) slidingcache.Config {
	cfg := benchConfig()
	cfg.Shards = shards
	return cfg
}

// sketchConfigOf carries the window, shard and guard fields of a root config
// over to a sketch with the default Epsilon and Delta.
func sketchConfigOf(cfg slidingcache.Config) Config {
	return Config{
		Precision:     cfg.Precision,
		WindowSize:    cfg.WindowSize,
		Shards:        cfg.Shards,
		EpochUnit:     cfg.EpochUnit,
		MaxFutureSkew: cfg.MaxFutureSkew,
		Clock:         cfg.Clock,
	}
}

type benchImpl struct {
	name  string
	build func(b *testing.B, cfg slidingcache.Config) slidingcache.SlidingCache
}

var benchImpls = [...]benchImpl{
	{"impl=exact", newExactBenchCache},
	{"impl=sketch", func(b *testing.B, cfg slidingcache.Config) slidingcache.SlidingCache {
		b.Helper()
		return newSketchBenchCache(b, sketchConfigOf(cfg))
	}},
}

func newExactBenchCache(b *testing.B, cfg slidingcache.Config) slidingcache.SlidingCache {
	b.Helper()
	c, err := slidingcache.New(cfg)
	if err != nil {
		b.Fatalf("slidingcache.New returned error: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

func newSketchBenchCache(b *testing.B, cfg Config) *Cache {
	b.Helper()
	c, err := New(cfg)
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

// forEachImpl runs body once per implementation, as the innermost
// sub-benchmark, on a cache built from cfg for every run, outside the timed
// region; body resets the timer after its own setup.
func forEachImpl(b *testing.B, cfg slidingcache.Config, body func(b *testing.B, c slidingcache.SlidingCache)) {
	b.Helper()
	for _, impl := range benchImpls {
		b.Run(impl.name, func(b *testing.B) { body(b, impl.build(b, cfg)) })
	}
}

func BenchmarkStoreSingleKey(b *testing.B) {
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Store(int64(i), "single")
		}
	})
}

func BenchmarkStoreManyKeys(b *testing.B) {
	const keys = 100_000
	keySet := makeKeys(keys)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Store(int64(i), keySet[i%keys])
		}
	})
}

func BenchmarkGetHit(b *testing.B) {
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		c.Store(1000, "present")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Get(1000, "present")
		}
	})
}

func BenchmarkGetMiss(b *testing.B) {
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Get(1000, "absent")
		}
	})
}

func BenchmarkStoreSteadyState(b *testing.B) {
	for _, keys := range []int{1_000, 100_000} {
		keySet := makeKeys(keys)
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c.Store(int64(i), keySet[i%keys])
				}
			})
		})
	}
}

func BenchmarkStoreLateReject(b *testing.B) {
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		c.Store(1_000_000, "high-water")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Store(0, "late")
		}
	})
}

func BenchmarkGetHitManyKeys(b *testing.B) {
	const (
		keys      = 100_000
		liveEpoch = 1_000_000
	)
	keySet := makeKeys(keys)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		populate(c, keySet)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Get(liveEpoch, keySet[i%keys])
		}
	})
}

func BenchmarkParallelGet(b *testing.B) {
	const (
		keys      = 100_000
		liveEpoch = 1_000_000
	)
	keySet := makeKeys(keys)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		populate(c, keySet)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				c.Get(liveEpoch, keySet[i%keys])
				i++
			}
		})
	})
}

func BenchmarkParallelMixed(b *testing.B) {
	const (
		keys      = 100_000
		baseEpoch = 1_000_000
	)
	keySet := makeKeys(keys)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		populate(c, keySet)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keySet[i%keys]
				if i%10 == 0 {
					c.Store(int64(baseEpoch+i), key)
				} else {
					c.Get(int64(baseEpoch+i), key)
				}
				i++
			}
		})
	})
}

func BenchmarkStoreOutOfOrder(b *testing.B) {
	jitter := makeJitter(1024)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			epoch := int64(i) - jitter[i%len(jitter)]
			c.Store(epoch, "hot")
		}
	})
}

func BenchmarkParallelStore(b *testing.B) {
	const keys = 10_000
	keySet := makeKeys(keys)
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				c.Store(int64(i), keySet[i%keys])
				i++
			}
		})
	})
}

func BenchmarkParallelStoreShards(b *testing.B) {
	const keys = 10_000
	keySet := makeKeys(keys)
	for _, shards := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			forEachImpl(b, benchConfigShards(shards), func(b *testing.B, c slidingcache.SlidingCache) {
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						c.Store(int64(i), keySet[i%keys])
						i++
					}
				})
			})
		})
	}
}

func BenchmarkStoreHotKeySameBucket(b *testing.B) {
	const epoch = 1_000_000
	forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.Store(epoch, "hot")
		}
	})
}

func BenchmarkStoreMediumCardinalitySameBucket(b *testing.B) {
	const epoch = 1_000_000
	for _, keys := range []int{100, 1_000, 10_000} {
		keySet := makeKeys(keys)
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			forEachImpl(b, benchConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					c.Store(epoch, keySet[i%keys])
				}
			})
		})
	}
}

func BenchmarkStoreHotKeyNanos(b *testing.B) {
	const (
		baseEpoch     = 1_700_000_000 * int64(time.Second)
		nanosPerEvent = 50_000
	)
	cfg := slidingcache.Config{
		Precision:     time.Second,
		WindowSize:    300 * time.Second,
		EpochUnit:     slidingcache.EpochInNanos,
		SweepInterval: time.Hour,
	}
	forEachImpl(b, cfg, func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			c.Store(baseEpoch+int64(i)*nanosPerEvent, "hot")
		}
	})
}

// benchFutureConfig arms the future guard with a clock parked far beyond every
// epoch the benchmarks use, as the root package's does.
func benchFutureConfig() slidingcache.Config {
	const clockFarAhead = int64(1) << 40
	cfg := benchConfig()
	cfg.MaxFutureSkew = 5 * time.Minute
	cfg.Clock = func() int64 { return clockFarAhead }
	return cfg
}

func BenchmarkStoreFutureReject(b *testing.B) {
	const now = 1_700_000_000
	cfg := benchConfig()
	cfg.MaxFutureSkew = 5 * time.Minute
	cfg.Clock = func() int64 { return now }
	forEachImpl(b, cfg, func(b *testing.B, c slidingcache.SlidingCache) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.Store(now+3600, "future")
		}
	})
}

func BenchmarkStoreAdvancingHighWater(b *testing.B) {
	cases := []struct {
		name string
		cfg  slidingcache.Config
	}{
		{"skew=off", benchConfig()},
		{"skew=on", benchFutureConfig()},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			forEachImpl(b, tc.cfg, func(b *testing.B, c slidingcache.SlidingCache) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					c.Store(int64(i), "advancing")
				}
			})
		})
	}
}

func BenchmarkStoreSteadyStateWithSkew(b *testing.B) {
	const keys = 100_000
	keySet := makeKeys(keys)
	b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
		forEachImpl(b, benchFutureConfig(), func(b *testing.B, c slidingcache.SlidingCache) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Store(int64(i), keySet[i%keys])
			}
		})
	})
}

// eventsPerDenseBucket is the density of the dense workloads: a thousand
// events share each one-second bucket, so a bucket change, and with it an
// expiry, comes once per thousand Stores.
const eventsPerDenseBucket = 1_000

// prewarmDense runs one full window of the dense workload, so the timed loop
// starts with every slot live and expires as many as it opens, whatever b.N.
// It returns the op index the timed loop continues from.
func prewarmDense(c slidingcache.SlidingCache, keySet []string, buckets int) int {
	return prewarmAtDensity(c, keySet, buckets*eventsPerDenseBucket, eventsPerDenseBucket)
}

// prewarmAtDensity runs ops Stores of the round-robin workload at events per
// bucket and returns the op index the timed loop continues from.
func prewarmAtDensity(c slidingcache.SlidingCache, keySet []string, ops, events int) int {
	for i := range ops {
		c.Store(int64(i/events), keySet[i%len(keySet)])
	}
	runtime.GC()
	return ops
}

// BenchmarkStoreDense is BenchmarkStoreManyKeys at a thousand events per
// bucket: 100k keys round-robin over the one-hour window.
func BenchmarkStoreDense(b *testing.B) {
	const keys = 100_000
	keySet := makeKeys(keys)
	cfg := benchConfig()
	forEachImpl(b, cfg, func(b *testing.B, c slidingcache.SlidingCache) {
		start := prewarmDense(c, keySet, bucketsOf(cfg))
		b.ReportAllocs()
		b.ResetTimer()
		for i := start; i < start+b.N; i++ {
			c.Store(int64(i/eventsPerDenseBucket), keySet[i%keys])
		}
	})
}

// maxDensityWarmup caps the warm-up of BenchmarkStoreDensity, so the densest
// arm does not spend tens of millions of untimed Stores filling the window.
const maxDensityWarmup = 4_000_000

// BenchmarkStoreDensity is BenchmarkStoreDense swept over the events per
// one-second bucket. It prices the sketch's expiry, one slot per shard per new
// bucket, spread over the events of that bucket, against the exact cache at
// the same density: the reproducer of the density at which the sketch catches
// up. events=1000 is the same workload as BenchmarkStoreDense, kept separately
// because that one is in the archived files.
//
// The warm-up fills the whole window, so the timed loop expires as many slots
// as it opens, up to 1000 events per bucket. At 10000 it stops at
// maxDensityWarmup events, 400 of the 3600 buckets, so nothing expires in the
// timed part there unless b.N reaches past the rest of the window.
func BenchmarkStoreDensity(b *testing.B) {
	const keys = 100_000
	keySet := makeKeys(keys)
	cfg := benchConfig()
	for _, events := range []int{1, 10, 30, 100, 300, 1_000, 10_000} {
		b.Run(fmt.Sprintf("events=%d", events), func(b *testing.B) {
			forEachImpl(b, cfg, func(b *testing.B, c slidingcache.SlidingCache) {
				warmup := min(bucketsOf(cfg)*events, maxDensityWarmup)
				start := prewarmAtDensity(c, keySet, warmup, events)
				b.ReportAllocs()
				b.ResetTimer()
				for i := start; i < start+b.N; i++ {
					c.Store(int64(i/events), keySet[i%keys])
				}
			})
		})
	}
}

// BenchmarkParallelStoreDense is BenchmarkParallelStore at a thousand events
// per bucket. Each goroutine keeps its own op index, as there, starting after
// the prewarmed window.
func BenchmarkParallelStoreDense(b *testing.B) {
	const keys = 10_000
	keySet := makeKeys(keys)
	cfg := benchConfig()
	forEachImpl(b, cfg, func(b *testing.B, c slidingcache.SlidingCache) {
		start := prewarmDense(c, keySet, bucketsOf(cfg))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := start
			for pb.Next() {
				c.Store(int64(i/eventsPerDenseBucket), keySet[i%keys])
				i++
			}
		})
	})
}

func bucketsOf(cfg slidingcache.Config) int {
	return int(cfg.WindowSize / cfg.Precision)
}

// BenchmarkStoreWindowBuckets prices the window length on the sketch alone:
// at a fixed density, the cost of a Store must not depend on the number of
// buckets. dense is a thousand events per bucket and sparse one, so every
// sparse Store opens a bucket and expires one. Both start on a full window.
func BenchmarkStoreWindowBuckets(b *testing.B) {
	const keys = 100_000
	keySet := makeKeys(keys)
	densities := []struct {
		name   string
		events int
	}{
		{"dense", eventsPerDenseBucket},
		{"sparse", 1},
	}
	for _, buckets := range []int{60, 1_800} {
		for _, density := range densities {
			b.Run(fmt.Sprintf("buckets=%d/density=%s", buckets, density.name), func(b *testing.B) {
				cfg := benchConfig()
				cfg.WindowSize = time.Duration(buckets) * time.Second
				c := newSketchBenchCache(b, sketchConfigOf(cfg))
				warm := buckets * density.events
				for i := range warm {
					c.Store(int64(i/density.events), keySet[i%keys])
				}
				runtime.GC()
				b.ReportAllocs()
				b.ResetTimer()
				for i := warm; i < warm+b.N; i++ {
					c.Store(int64(i/density.events), keySet[i%keys])
				}
			})
		}
	}
}

// BenchmarkStoreHighWaterJump prices expiry on the sketch alone, with a window
// of 1800 one-second buckets over 16 shards, one key, so one shard.
//
// Setup fills every bucket of the window. Each op is then exactly one timed
// Store whose epoch is jump buckets past the mark: it expires the jump oldest
// slots, all of them live, and counts one event into the newest. Untimed, the
// op then refills the jump-1 buckets the jump left empty between the old and
// the new mark, one Store each, so the next op again starts with every slot
// live. ns/op is therefore the cost of a Store that expires jump live slots;
// jump=1 has nothing to refill and never stops the timer.
func BenchmarkStoreHighWaterJump(b *testing.B) {
	const (
		buckets = 1_800
		shards  = 16
		base    = 1_000_000
	)
	jumps := []struct {
		name    string
		buckets int64
	}{
		{"1", 1},
		{"quarter", buckets / 4},
		{"half", buckets / 2},
		{"window", buckets},
	}
	for _, jump := range jumps {
		b.Run("jump="+jump.name, func(b *testing.B) {
			cfg := benchConfigShards(shards)
			cfg.WindowSize = buckets * time.Second
			c := newSketchBenchCache(b, sketchConfigOf(cfg))
			for epoch := int64(base); epoch < base+buckets; epoch++ {
				c.Store(epoch, "hot")
			}
			highWater := int64(base + buckets - 1)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				previous := highWater
				highWater += jump.buckets
				c.Store(highWater, "hot")
				if jump.buckets > 1 {
					b.StopTimer()
					for epoch := previous + 1; epoch < highWater; epoch++ {
						c.Store(epoch, "hot")
					}
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(jump.buckets), "expired-slots/op")
		})
	}
}

// BenchmarkMemoryFootprint reports, for the exact arm, the heap per key as the
// root package does, and for the sketch the heap New and the same population
// leave behind next to the estimate New checks: both independent of the keys.
func BenchmarkMemoryFootprint(b *testing.B) {
	const windows = 10
	b.Run("impl=exact", func(b *testing.B) {
		const keys = 100_000
		keySet := makeKeys(keys)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			heap := heapGrowth(func() (slidingcache.SlidingCache, func()) {
				c, err := slidingcache.New(benchConfig())
				if err != nil {
					b.Fatalf("slidingcache.New returned error: %v", err)
				}
				populateWindows(c, keySet, windows)
				return c, func() { _ = c.Close() }
			})
			b.ReportMetric(float64(heap)/float64(keys), "bytes/key")
		}
	})
	b.Run("impl=sketch", func(b *testing.B) {
		cfg := sketchConfigOf(benchConfig())
		estimate, ok := mustBenchLayout(b, cfg).memoryBytes()
		if !ok {
			b.Fatal("the memory estimate of the benchmark config overflows")
		}
		for _, keys := range []int{100_000, 1_000_000} {
			b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
				keySet := makeKeys(keys)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					heap := heapGrowth(func() (slidingcache.SlidingCache, func()) {
						c, err := New(cfg)
						if err != nil {
							b.Fatalf("New returned error: %v", err)
						}
						populateWindows(c, keySet, windows)
						return c, func() { _ = c.Close() }
					})
					b.ReportMetric(float64(heap), "heap-bytes")
					b.ReportMetric(float64(estimate), "estimate-bytes")
				}
			})
		}
	})
}

func mustBenchLayout(b *testing.B, cfg Config) windowLayout {
	b.Helper()
	layout, err := cfg.layout()
	if err != nil {
		b.Fatalf("layout of %+v: %v", cfg, err)
	}
	return layout
}

// heapGrowth returns the live heap that build's cache holds once collected,
// keeping the cache reachable until the second reading.
func heapGrowth(build func() (slidingcache.SlidingCache, func())) int64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c, release := build()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(c)
	release()
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func populateWindows(c slidingcache.SlidingCache, keySet []string, windows int) {
	for w := range windows {
		epoch := int64(w * 60)
		for _, key := range keySet {
			c.Store(epoch, key)
		}
	}
}

// statsSink keeps the loop below from being optimized away.
var statsSink Stats

// BenchmarkStats measures the diagnostics snapshot of the sketch at 256
// shards; its name is outside the throughput families, like the root's.
func BenchmarkStats(b *testing.B) {
	const (
		shards = 256
		keys   = 10_000
	)
	c := newSketchBenchCache(b, sketchConfigOf(benchConfigShards(shards)))
	populate(c, makeKeys(keys))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		statsSink = c.Stats()
	}
}

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// makeJitter returns the root package's n deterministic backward offsets in
// [0, 1800) seconds, drawn from the same source and seed.
func makeJitter(n int) []int64 {
	r := rand.New(rand.NewSource(1))
	jitter := make([]int64, n)
	for i := range jitter {
		jitter[i] = r.Int63n(1800)
	}
	return jitter
}

func populate(c slidingcache.SlidingCache, keySet []string) {
	const epoch = 1_000_000
	for _, key := range keySet {
		c.Store(epoch, key)
	}
}
