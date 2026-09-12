package guard

import (
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

// These benchmarks price the guard's two integer comparisons against the raw
// cache hot path. RealClock rows include the time.Now().UnixNano() syscall the
// guard's default clock pays per call; FakeClock rows isolate the guard's own
// overhead by pinning the clock, with every op incrementing the same bucket.

func benchCache(b *testing.B) *slidingcache.Cache {
	b.Helper()
	c, err := slidingcache.New(slidingcache.Config{
		Precision:     time.Second,
		WindowSize:    1800 * time.Second,
		EpochUnit:     slidingcache.EpochInNanos,
		SweepInterval: time.Hour, // keep the janitor out of the measurement.
	})
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

func BenchmarkRawCacheStoreRealClock(b *testing.B) {
	c := benchCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.Store(time.Now().UnixNano(), "hot")
	}
}

func BenchmarkGuardStoreRealClock(b *testing.B) {
	g, err := New(benchCache(b), Config{Enabled: true})
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		g.Store(time.Now().UnixNano(), "hot")
	}
}

func BenchmarkGuardStoreFakeClock(b *testing.B) {
	const now = 1_700_000_000 * int64(time.Second)
	g, err := New(benchCache(b), Config{Enabled: true, Now: func() int64 { return now }})
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		g.Store(now, "hot")
	}
}

func BenchmarkGuardGetFakeClock(b *testing.B) {
	const now = 1_700_000_000 * int64(time.Second)
	g, err := New(benchCache(b), Config{Enabled: true, Now: func() int64 { return now }})
	if err != nil {
		b.Fatalf("New returned error: %v", err)
	}
	g.Store(now, "hot")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		g.Get(now, "hot")
	}
}
