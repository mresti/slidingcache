package countminsketch

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

func requirePartitions(t *testing.T, stats Stats, stores, gets uint64) {
	t.Helper()
	if got := stats.Accepted + stats.Late + stats.Future + stats.OutOfRange; got != stores {
		t.Fatalf("Store counters sum to %d over %d calls: %+v", got, stores, stats)
	}
	if got := stats.GetHit + stats.GetMiss + stats.GetLate + stats.GetFuture; got != gets {
		t.Fatalf("Get counters sum to %d over %d calls: %+v", got, gets, stats)
	}
	if stats.Saturated > stats.Accepted {
		t.Fatalf("Saturated %d exceeds Accepted %d", stats.Saturated, stats.Accepted)
	}
}

func TestStatsCountEveryOutcomeOnce(t *testing.T) {
	clock := &fakeClock{now: 1_000}
	cfg := secondsConfig(0)
	cfg.MaxFutureSkew, cfg.Clock = 30*time.Second, clock.read
	c := newTestCache(t, cfg, WithHashFunc(exactHash))
	k0, k1 := testKeys[0], testKeys[1]

	stores := []struct {
		epoch int64
		want  int
	}{
		{1_000, 1},
		{1_001, 2},
		{900, slidingcache.LateEvent},
		{5_000, slidingcache.FutureEvent},
		{1 << 50, slidingcache.LateEvent},
	}
	for _, s := range stores {
		requireReturn(t, "Store", c.Store(s.epoch, k0), s.want)
	}
	gets := []struct {
		epoch int64
		key   string
		want  int
	}{
		{1_000, k0, 2},
		{1_000, k1, 0},
		{900, k0, slidingcache.LateEvent},
		{5_000, k0, slidingcache.FutureEvent},
		{-1 << 50, k0, slidingcache.LateEvent},
	}
	for _, g := range gets {
		requireReturn(t, "Get", c.Get(g.epoch, g.key), g.want)
	}

	want := Stats{
		Accepted: 2, Late: 1, Future: 1, OutOfRange: 1,
		GetHit: 1, GetMiss: 1, GetLate: 2, GetFuture: 1,
	}
	if got := c.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
	requirePartitions(t, c.Stats(), uint64(len(stores)), uint64(len(gets)))
}

// TestGetHitAndMissFollowTheEstimate pins the documented difference from the
// root package: a key whose events have all expired is a miss here, since its
// estimate is 0, and an absent key colliding with a present one is a hit.
func TestGetHitAndMissFollowTheEstimate(t *testing.T) {
	cfg := secondsConfig(0)
	c := newTestCache(t, cfg, WithHashFunc(func(key string) uint64 {
		if key == "absent twin" {
			return exactHash(testKeys[0])
		}
		return exactHash(key)
	}))
	c.Store(100, testKeys[0])
	requireReturn(t, "absent key sharing every counter", c.Get(100, "absent twin"), 1)

	c.Store(200, testKeys[1])
	requireReturn(t, "expired key", c.Get(200, testKeys[0]), 0)

	if stats := c.Stats(); stats.GetHit != 1 || stats.GetMiss != 1 {
		t.Fatalf("Stats() = %+v, want GetHit 1 and GetMiss 1", stats)
	}
}

// TestStoreRejectsUnderTheLockWhenTheMarkMovesMeanwhile reaches the late
// rejection that only a concurrent Store can trigger, deterministically: the
// hash runs after the fast reject and before the lock, so a hash that stores
// one far newer event moves the mark in exactly that gap.
func TestStoreRejectsUnderTheLockWhenTheMarkMovesMeanwhile(t *testing.T) {
	var c *Cache
	var reentered bool
	c = newTestCache(t, secondsConfig(0), WithHashFunc(func(key string) uint64 {
		if !reentered && key == testKeys[0] {
			reentered = true
			c.Store(10_000, testKeys[1])
		}
		return exactHash(key)
	}))

	requireReturn(t, "Store overtaken before the lock", c.Store(1_000, testKeys[0]), slidingcache.LateEvent)
	if hw, _ := c.HighWater(); hw != 10_000 {
		t.Fatalf("HighWater() = %d, want 10000 from the reentrant Store", hw)
	}
	if stats := c.Stats(); stats.Accepted != 1 || stats.Late != 1 {
		t.Fatalf("Stats() = %+v, want Accepted 1 and Late 1", stats)
	}
	requireReturn(t, "nothing of the overtaken event was counted", c.Get(10_000, testKeys[0]), 0)
}

func TestClampToInt(t *testing.T) {
	tests := []struct {
		estimate uint64
		want     int
	}{
		{0, 0},
		{7, 7},
		{math.MaxInt, math.MaxInt},
		{math.MaxInt + 1, math.MaxInt},
		{math.MaxUint64, math.MaxInt},
	}
	for _, tt := range tests {
		if got := clampToInt(tt.estimate); got != tt.want {
			t.Errorf("clampToInt(%d) = %d, want %d", tt.estimate, got, tt.want)
		}
	}
}

// TestConcurrentStoreAndGet is a smoke test for -race: goroutines with
// overlapping, partly out-of-order epochs. Afterwards the counters still
// partition the calls and the mark is the largest accepted timestamp.
func TestConcurrentStoreAndGet(t *testing.T) {
	const (
		goroutines = 8
		opsEach    = 2_000
	)
	cfg := secondsConfig(0)
	c := newTestCache(t, cfg)

	var largestAccepted atomic.Int64
	largestAccepted.Store(math.MinInt64)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range opsEach {
				epoch := int64(i*3 + g*7)
				key := testKeys[(i+g)%len(testKeys)]
				if i%3 == 0 {
					c.Get(epoch, key)
					continue
				}
				if c.Store(epoch, key) > 0 {
					raiseTo(&largestAccepted, c.bucket(epoch))
				}
			}
		})
	}
	wg.Wait()

	const stores = goroutines * (opsEach - (opsEach+2)/3)
	requirePartitions(t, c.Stats(), stores, goroutines*opsEach-stores)
	if hw, _ := c.HighWater(); hw != largestAccepted.Load() {
		t.Fatalf("HighWater() = %d, the largest accepted timestamp is %d", hw, largestAccepted.Load())
	}
	hashes := make([]uint64, len(testKeys))
	for i, key := range testKeys {
		hashes[i] = c.hashOf(key)
	}
	newWindowChecker(t, cfg, hashes).require(t, c)
}

func raiseTo(highest *atomic.Int64, value int64) {
	for current := highest.Load(); value > current; current = highest.Load() {
		if highest.CompareAndSwap(current, value) {
			return
		}
	}
}

// TestSteadyStateDoesNotAllocate warms the ring until every slot owns a
// sketch, after which neither accepting, advancing, expiring nor rejecting may
// allocate.
func TestSteadyStateDoesNotAllocate(t *testing.T) {
	clock := &fakeClock{now: 0}
	cfg := secondsConfig(0)
	cfg.MaxFutureSkew, cfg.Clock = time.Hour, clock.read
	c := newTestCache(t, cfg)
	buckets := int64(len(c.window.slots))
	epoch := int64(0)
	for ; epoch < 2*buckets*c.precision; epoch += c.precision {
		c.Store(epoch, testKeys[0])
	}
	clock.now = 1 << 40

	operations := map[string]func(){
		"Store into the newest bucket": func() { c.Store(epoch, testKeys[0]) },
		"Store advancing the mark": func() {
			epoch += c.precision
			c.Store(epoch, testKeys[1])
		},
		"Get":                  func() { c.Get(epoch, testKeys[0]) },
		"Store late":           func() { c.Store(0, testKeys[0]) },
		"Store out of range":   func() { c.Store(1<<62, testKeys[0]) },
		"Get late":             func() { c.Get(0, testKeys[0]) },
		"Store in the future":  func() { c.Store(1<<41, testKeys[0]) },
		"Get in the future":    func() { c.Get(1<<41, testKeys[0]) },
		"Get out of range":     func() { c.Get(-1<<62, testKeys[0]) },
		"Stats":                func() { _ = c.Stats() },
		"HighWater":            func() { _, _ = c.HighWater() },
		"Store far beyond all": func() { epoch += 1_000 * c.precision; c.Store(epoch, testKeys[2]) },
	}
	for name, op := range operations {
		if allocs := testing.AllocsPerRun(100, op); allocs != 0 {
			t.Errorf("%s allocates %v times per call, want 0", name, allocs)
		}
	}
}
