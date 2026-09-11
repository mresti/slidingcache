package slidingcache

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// benchConfig is the default benchmark cache configuration: 1s precision, a
// one-hour window, second epochs, and a janitor interval long enough to keep
// background sweeps out of the measured hot loops.
func benchConfig() Config {
	return Config{
		Precision:     time.Second,
		WindowSize:    3600 * time.Second,
		EpochUnit:     EpochInSeconds,
		SweepInterval: time.Hour, // keep the janitor out of the measurement.
	}
}

func benchCache(b *testing.B) *Cache {
	b.Helper()
	return newBenchCache(b, benchConfig())
}

// benchCacheShards builds a benchmark cache with an explicit shard count, used
// by the shard-sensitivity benchmarks.
func benchCacheShards(b *testing.B, shards int) *Cache {
	b.Helper()
	cfg := benchConfig()
	cfg.Shards = shards
	return newBenchCache(b, cfg)
}

func newBenchCache(b *testing.B, cfg Config) *Cache {
	b.Helper()
	c, err := New(cfg)
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

// BenchmarkStoreSingleKey characterizes the hot-key cost when every op opens a
// new bucket: with epoch advancing by one second per op, the key holds ~3600
// live buckets, each op appends one and each prune drops the expired one by
// re-slicing the long survivor run forward.
func BenchmarkStoreSingleKey(b *testing.B) {
	c := benchCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Store(int64(i), "single")
	}
}

func BenchmarkStoreManyKeys(b *testing.B) {
	c := benchCache(b)
	const keys = 100_000
	keySet := makeKeys(keys)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Store(int64(i), keySet[i%keys])
	}
}

// BenchmarkGetHit is the 1-key CPU-cache best case: the single entry stays hot
// in L1/L2, so it lower-bounds Get and is not representative of a populated map.
func BenchmarkGetHit(b *testing.B) {
	c := benchCache(b)
	c.Store(1000, "present")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1000, "present")
	}
}

// BenchmarkGetMiss is the 1-key CPU-cache best case for a miss: a near-empty map
// with one absent lookup, so it lower-bounds the miss path rather than modeling
// a realistic map with cache misses.
func BenchmarkGetMiss(b *testing.B) {
	c := benchCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1000, "absent")
	}
}

// BenchmarkStoreSteadyState measures the storage path in its steady-state
// regime: the epoch advances every op, so old events expire as the high-water
// mark moves and per-key entries stay bounded by the window. Per-op cost is
// therefore independent of b.N, unlike a fixed-epoch loop that grows entries
// without bound.
func BenchmarkStoreSteadyState(b *testing.B) {
	for _, keys := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			c := benchCache(b)
			keySet := makeKeys(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Store(int64(i), keySet[i%keys])
			}
		})
	}
}

// BenchmarkStoreLateReject measures the rejection fast path explicitly: after a
// single Store advances the high-water mark far ahead, every subsequent Store at
// epoch 0 is expired and returns before any shard is touched.
func BenchmarkStoreLateReject(b *testing.B) {
	c := benchCache(b)
	c.Store(1_000_000, "high-water") // advance HW so epoch-0 stores are always late.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Store(0, "late")
	}
}

// BenchmarkGetHitManyKeys measures Get against a fully populated 100k-key map,
// so lookups hit realistic hash and CPU-cache misses rather than a single hot
// entry. Each key holds one live event and reads never expire it, keeping the
// benchmark steady-state.
func BenchmarkGetHitManyKeys(b *testing.B) {
	const (
		keys      = 100_000
		liveEpoch = 1_000_000
	)
	c := benchCache(b)
	keySet := makeKeys(keys)
	populate(c, keySet)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(liveEpoch, keySet[i%keys])
	}
}

// BenchmarkParallelGet measures concurrent read throughput over a populated
// 100k-key map, with goroutines round-robining over the key set to spread load
// across shards. Reads never expire the single live event per key.
func BenchmarkParallelGet(b *testing.B) {
	const (
		keys      = 100_000
		liveEpoch = 1_000_000
	)
	c := benchCache(b)
	keySet := makeKeys(keys)
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
}

// BenchmarkParallelMixed measures a concurrent 90% Get / 10% Store workload over
// a populated 100k-key map. The epoch advances per goroutine, so a key is
// re-stored only after ~keys ops (far beyond the window) and its prior event has
// already expired: entries stay bounded and per-op cost is b.N-independent. As
// the initial events age past the window, untouched keys drift from hits to
// misses; both are realistic map lookups.
func BenchmarkParallelMixed(b *testing.B) {
	const (
		keys      = 100_000
		baseEpoch = 1_000_000
	)
	c := benchCache(b)
	keySet := makeKeys(keys)
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
}

// BenchmarkStoreOutOfOrder measures the worst-case ordered-insert cost on a
// single hot key: the epoch is jittered back by up to 1800s, so events arrive
// out of order and land mid-slice, forcing a memmove on every insert that opens
// a bucket; repeats of an already-populated bucket only increment it. HW tracks
// i while jitter < window, so events are never late and the entry stays bounded
// at ~window live buckets.
func BenchmarkStoreOutOfOrder(b *testing.B) {
	jitter := makeJitter(1024)
	c := benchCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		epoch := int64(i) - jitter[i%len(jitter)]
		c.Store(epoch, "hot")
	}
}

// BenchmarkSweep measures the per-sweep traversal cost of a shard map populated
// with live events, calling sweep directly. The cutoff expires nothing, so this
// is steady-state whole-map traversal. ns/op approximates the per-shard
// lock-hold latency spike concurrent Stores observe during a sweep, divided by
// the shard count.
func BenchmarkSweep(b *testing.B) {
	for _, keys := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			c := benchCache(b)
			populate(c, makeKeys(keys))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.sweep()
			}
		})
	}
}

func BenchmarkParallelStore(b *testing.B) {
	c := benchCache(b)
	const keys = 10_000
	keySet := makeKeys(keys)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Store(int64(i), keySet[i%keys])
			i++
		}
	})
}

// BenchmarkParallelStoreShards runs the BenchmarkParallelStore workload across
// varying shard counts to separate shard-lock contention from the global
// high-water CAS: more shards cut lock contention but every Store still contends
// on the single atomic high-water mark.
func BenchmarkParallelStoreShards(b *testing.B) {
	const keys = 10_000
	for _, shards := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := benchCacheShards(b, shards)
			keySet := makeKeys(keys)
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
}

// BenchmarkMemoryFootprint reports the heap delta per stored key after
// populating N keys across M windows and forcing a garbage collection. It
// measures the steady-state memory cost of the cache rather than throughput.
func BenchmarkMemoryFootprint(b *testing.B) {
	const (
		keys    = 100_000
		windows = 10
	)
	keySet := makeKeys(keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		c, err := New(benchConfig())
		if err != nil {
			b.Fatalf("New returned error: %v", err)
		}
		for w := range windows {
			epoch := int64(w * 60)
			for k := range keys {
				c.Store(epoch, keySet[k])
			}
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(keys), "bytes/key")

		runtime.KeepAlive(c)
		_ = c.Close()
	}
}

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// makeJitter returns n deterministic backward offsets in [0, 1800) seconds,
// used to synthesize out-of-order-but-still-live events from a monotonic index.
func makeJitter(n int) []int64 {
	r := rand.New(rand.NewSource(1))
	jitter := make([]int64, n)
	for i := range jitter {
		jitter[i] = r.Int63n(1800)
	}
	return jitter
}

func populate(c *Cache, keySet []string) {
	const epoch = 1_000_000
	for _, key := range keySet {
		c.Store(epoch, key)
	}
}

// BenchmarkStoreHotKeySameBucket is the regression case for same-bucket
// inserts: every event lands in one Precision bucket on one key, so an insert
// that stored one timestamp per event would grow the entry with b.N. With events
// counted in their bucket, the cost is an increment and must stay
// b.N-independent, as must the memory.
func BenchmarkStoreHotKeySameBucket(b *testing.B) {
	const epoch = 1_000_000
	c := benchCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.Store(epoch, "hot")
	}
}

// BenchmarkStoreMediumCardinalitySameBucket spreads the same-bucket workload
// over a realistic number of keys: the epoch never advances, so nothing expires
// and each key keeps counting into its single bucket, while the round-robin
// over the key set adds map and CPU-cache misses the single-key benchmark
// hides.
func BenchmarkStoreMediumCardinalitySameBucket(b *testing.B) {
	const epoch = 1_000_000
	for _, keys := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			c := benchCache(b)
			keySet := makeKeys(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Store(epoch, keySet[i%keys])
			}
		})
	}
}

// BenchmarkStoreHotKeyNanos models the production hot-key rate: nanosecond
// epochs advancing 50µs per op on a single key, so ~20k events share each 1s
// bucket and the 300s window holds at most 300 buckets however many events land
// in them. The window slides, so entries stay bounded and per-op cost is
// b.N-independent.
func BenchmarkStoreHotKeyNanos(b *testing.B) {
	const (
		baseEpoch     = 1_700_000_000 * int64(time.Second)
		nanosPerEvent = 50_000
	)
	c := newBenchCache(b, Config{
		Precision:     time.Second,
		WindowSize:    300 * time.Second,
		EpochUnit:     EpochInNanos,
		SweepInterval: time.Hour, // keep the janitor out of the measurement.
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		c.Store(baseEpoch+int64(i)*nanosPerEvent, "hot")
	}
}

// BenchmarkPruneCopyThreshold is the measurement behind pruneCopyMaxLen. Each
// key receives one event per second and is pruned on every Store, so the
// survivor run is the window length in buckets: sweeping the window sweeps the
// entry sizes on both sides of the threshold. Its name is deliberately outside
// the Store/Get/Sweep/Memory families, so it stays out of throughput comparisons
// and is run on purpose when the threshold is retuned.
func BenchmarkPruneCopyThreshold(b *testing.B) {
	const keys = 8
	for _, windowSeconds := range []int{60, 130, 200, 300, 600, 3600} {
		b.Run(fmt.Sprintf("buckets=%d", windowSeconds), func(b *testing.B) {
			c := newBenchCache(b, Config{
				Precision:     time.Second,
				WindowSize:    time.Duration(windowSeconds) * time.Second,
				EpochUnit:     EpochInSeconds,
				SweepInterval: time.Hour, // keep the janitor out of the measurement.
			})
			keySet := makeKeys(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Store(int64(i/keys), keySet[i%keys])
			}
		})
	}
}
