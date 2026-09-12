package slidingcache

import (
	"math"
	"sync"
	"testing"
	"time"
)

func newStatsCache(t *testing.T) *Cache {
	t.Helper()
	return newTestCache(t, Config{Precision: time.Second, WindowSize: 1800 * time.Second, EpochUnit: EpochInNanos})
}

const statsBase = 1_700_000_000 * int64(time.Second)

func TestFreshCacheStatsAndHighWater(t *testing.T) {
	c := newStatsCache(t)

	if got := c.Stats(); got != (Stats{}) {
		t.Fatalf("fresh Stats = %+v, want zero", got)
	}
	// Before any Store the mark sits below every usable epoch, at the smallest
	// value whose cutoff is representable.
	if got, want := c.HighWater(), int64(math.MinInt64)+1800; got != want {
		t.Fatalf("fresh HighWater = %d, want %d", got, want)
	}
}

func TestStatsPartitionStoreOutcomes(t *testing.T) {
	c := newStatsCache(t)

	if got := c.Store(statsBase, "k"); got != 1 {
		t.Fatalf("Store = %d, want 1", got)
	}
	if got := c.Store(statsBase, "k"); got != 2 {
		t.Fatalf("repeat Store = %d, want 2", got)
	}
	if got := c.Store(statsBase+3*3600*int64(time.Second), "future"); got != 1 {
		t.Fatalf("future Store = %d, want 1", got)
	}
	// Late: the jump above puts every epoch inside the old window past the cutoff.
	if got := c.Store(statsBase+int64(time.Second), "late"); got != -1 {
		t.Fatalf("late Store = %d, want -1", got)
	}
	if got := c.Stats(); got != (Stats{StoresAccepted: 3, StoresLate: 1}) {
		t.Fatalf("Stats = %+v", got)
	}
	// The out-of-range rejection must not move the mark; the future one did.
	if got, want := c.HighWater(), statsBase/1e9+3*3600; got != want {
		t.Fatalf("HighWater = %d, want %d", got, want)
	}
}

func TestStatsCountsGetLate(t *testing.T) {
	c := newStatsCache(t)

	if got := c.Get(statsBase, "k"); got != 0 {
		t.Fatalf("Get on fresh cache = %d, want 0 (not late)", got)
	}
	c.Store(statsBase, "k")

	// Out-of-window query after the mark advanced.
	if got := c.Get(statsBase-1801*int64(time.Second), "k"); got != -1 {
		t.Fatalf("Get far past = %d, want -1", got)
	}
	// In-window query, hit and miss, are not rejections.
	if got := c.Get(statsBase, "k"); got != 1 {
		t.Fatalf("Get hit = %d, want 1", got)
	}
	if got := c.Get(statsBase, "absent"); got != 0 {
		t.Fatalf("Get miss = %d, want 0", got)
	}
	if got := c.Stats(); got != (Stats{StoresAccepted: 1, GetsLate: 1}) {
		t.Fatalf("Stats = %+v", got)
	}
}

// The whole int64 nanos range is representable, so the out-of-range counters
// need a seconds cache: +-2^43 seconds is the representable bucket range there.
func TestStatsCountsOutOfRange(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 1800 * time.Second, EpochUnit: EpochInSeconds})

	if got := c.Store(1<<43, "huge"); got != -1 {
		t.Fatalf("Store out of range = %d, want -1", got)
	}
	if got := c.Get(1<<43, "huge"); got != -1 {
		t.Fatalf("Get out of range = %d, want -1", got)
	}
	if got := c.Stats(); got != (Stats{StoresOutOfRange: 1, GetsLate: 1}) {
		t.Fatalf("Stats = %+v", got)
	}
	if got, want := c.HighWater(), int64(math.MinInt64)+1800; got != want {
		t.Fatalf("HighWater moved to %d, want %d", got, want)
	}
}

func TestStatsSnapshotIsIsolated(t *testing.T) {
	c := newStatsCache(t)
	c.Store(statsBase, "k")
	snapshot := c.Stats()
	c.Store(statsBase, "k")
	if snapshot.StoresAccepted != 1 {
		t.Fatalf("snapshot mutated: %+v", snapshot)
	}
	if c.Stats().StoresAccepted != 2 {
		t.Fatal("live stats did not advance")
	}
}

func TestHighWaterDoesNotAdvanceOnGet(t *testing.T) {
	c := newStatsCache(t)
	c.Store(statsBase, "k")
	before := c.HighWater()
	// The queried epoch gates only the late check; a live key still counts.
	if got := c.Get(statsBase+3600*int64(time.Second), "absent"); got != 0 {
		t.Fatalf("Get ahead of the mark = %d, want 0", got)
	}
	if c.HighWater() != before {
		t.Fatal("Get advanced the high-water mark")
	}
}

func TestStatsConcurrent(t *testing.T) {
	c := newStatsCache(t)
	const workers, perWorker = 8, 10_000
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range perWorker {
				epoch := statsBase + int64(i)*int64(time.Second)/1000 // +10s span
				c.Store(epoch, "key")
				c.Get(epoch, "key")
			}
		})
	}
	wg.Wait()

	s := c.Stats()
	if got := s.StoresAccepted + s.StoresLate + s.StoresOutOfRange; got != workers*perWorker {
		t.Fatalf("store outcomes = %d, want %d", got, workers*perWorker)
	}
	if s.StoresAccepted == 0 {
		t.Fatal("no accepted stores under concurrency")
	}
}

// TestStatsCountsShardLevelLateReject forces the race the under-lock cutoff
// re-check exists for: writer B passes the pre-lock check just inside the
// window, and writer A drags the high-water mark past B's timestamp before B
// acquires the shard lock. B's rejection then happens under the lock and must
// be counted as StoresLate. The race window is small, so B retries until it
// loses once.
func TestStatsCountsShardLevelLateReject(t *testing.T) {
	c := newStatsCache(t)
	const window = 1800
	sec := func(s int64) int64 { return s * int64(time.Second) }
	base := statsBase / 1e9

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { // writer A: keeps pushing the mark one second past the window.
		for i := int64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
				c.Store(sec(base+i), "a")
			}
		}
	})

	defer func() {
		close(stop)
		wg.Wait()
	}()

	const attempts = 200_000
	var rejected int64
	before := c.Stats().StoresLate
	for range attempts {
		ts := c.HighWater() - window + 1 // one second inside the window.
		if c.Store(sec(ts), "b") == -1 {
			rejected++
		}
		if late := c.Stats().StoresLate - before; late >= rejected && rejected > 0 {
			return // the under-lock rejection was observed and counted.
		}
	}
	t.Fatalf("no under-lock rejection in %d attempts (rejected=%d, counted=%d)",
		attempts, rejected, c.Stats().StoresLate-before)
}
