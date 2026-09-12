package guard

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

// spyCache records delegation and returns canned results, so tests can assert
// both what the guard returns and whether the cache was touched at all.
type spyCache struct {
	stores  int
	gets    int
	storeFn func(epoch int64, key string) int
	getFn   func(epoch int64, key string) int
}

func (s *spyCache) Store(epoch int64, key string) int {
	s.stores++
	if s.storeFn != nil {
		return s.storeFn(epoch, key)
	}
	return s.stores
}

func (s *spyCache) Get(epoch int64, key string) int {
	s.gets++
	if s.getFn != nil {
		return s.getFn(epoch, key)
	}
	return 42
}

// clock is a mutable fake clock for deterministic boundary tests.
type clock struct{ value int64 }

func (c *clock) now() int64 { return c.value }

const nanos = int64(time.Second)

func newGuard(t *testing.T, c slidingcache.SlidingCache, mutate func(*Config)) *Guard {
	t.Helper()
	cfg := Config{Enabled: true, Now: func() int64 { return 0 }}
	if mutate != nil {
		mutate(&cfg)
	}
	g, err := New(c, cfg)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	typed, ok := g.(*Guard)
	if !ok {
		t.Fatalf("New returned %T, want *Guard", g)
	}
	return typed
}

func TestNewDisabledReturnsCacheUnchanged(t *testing.T) {
	spy := &spyCache{}
	got, err := New(spy, Config{Enabled: false})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if got != slidingcache.SlidingCache(spy) {
		t.Fatalf("New returned %T, want the cache itself", got)
	}
	// Pass-through in both directions, with no clock reads to configure.
	if res := got.Store(1, "k"); res != 1 {
		t.Fatalf("Store = %d, want delegated 1", res)
	}
	if res := got.Get(1, "k"); res != 42 {
		t.Fatalf("Get = %d, want delegated 42", res)
	}
	if spy.stores != 1 || spy.gets != 1 {
		t.Fatalf("delegation counts = %d/%d, want 1/1", spy.stores, spy.gets)
	}
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  func(*Config)
	}{
		{"nil cache", nil},
		{"negative future skew", func(c *Config) { c.MaxFutureSkew = -time.Second }},
		{"negative past skew", func(c *Config) { c.MaxPastSkew = -time.Second }},
		{"invalid epoch unit", func(c *Config) { u := slidingcache.EpochUnit(9); c.EpochUnit = &u }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			cfg.Enabled = true
			if tc.name == "nil cache" {
				if _, err := New(nil, cfg); err == nil {
					t.Fatal("New(nil cache) returned nil error")
				}
				return
			}
			tc.cfg(&cfg)
			if _, err := New(&spyCache{}, cfg); err == nil {
				t.Fatal("New returned nil error")
			}
		})
	}
}

func TestFutureReject(t *testing.T) {
	spy := &spyCache{}
	g := newGuard(t, spy, nil) // defaults: +-120s around clock 0, nanos.

	if got := g.Store(120*nanos+1, "k"); got != FutureRejected {
		t.Fatalf("Store(+120s+1ns) = %d, want %d", got, FutureRejected)
	}
	if got := g.Store(3*3600*nanos, "k"); got != FutureRejected {
		t.Fatalf("Store(+3h) = %d, want %d", got, FutureRejected)
	}
	if got := g.Get(120*nanos+1, "k"); got != FutureRejected {
		t.Fatalf("Get(+120s+1ns) = %d, want %d", got, FutureRejected)
	}
	if spy.stores != 0 || spy.gets != 0 {
		t.Fatalf("cache touched on future rejects: %d stores, %d gets", spy.stores, spy.gets)
	}
}

func TestPastReject(t *testing.T) {
	spy := &spyCache{}
	g := newGuard(t, spy, nil)

	if got := g.Store(-120*nanos-1, "k"); got != LateRejected {
		t.Fatalf("Store(-120s-1ns) = %d, want %d", got, LateRejected)
	}
	if got := g.Get(-120*nanos-1, "k"); got != LateRejected {
		t.Fatalf("Get(-120s-1ns) = %d, want %d", got, LateRejected)
	}
	if spy.stores != 0 || spy.gets != 0 {
		t.Fatalf("cache touched on past rejects: %d stores, %d gets", spy.stores, spy.gets)
	}
}

func TestBoundariesDelegate(t *testing.T) {
	spy := &spyCache{}
	g := newGuard(t, spy, nil)

	// The edges themselves are inside the skews: only strictly beyond is
	// rejected, on both sides, for Store and Get.
	for _, epoch := range []int64{120 * nanos, 0, -120 * nanos} {
		if got := g.Store(epoch, "k"); got < 1 {
			t.Fatalf("Store(%d) = %d, want delegation", epoch, got)
		}
		if got := g.Get(epoch, "k"); got != 42 {
			t.Fatalf("Get(%d) = %d, want delegation", epoch, got)
		}
	}
	if spy.stores != 3 || spy.gets != 3 {
		t.Fatalf("delegation counts = %d/%d, want 3/3", spy.stores, spy.gets)
	}
}

func TestDefaultsAre120sBothSides(t *testing.T) {
	spy := &spyCache{}
	g := newGuard(t, spy, func(c *Config) { c.MaxFutureSkew = 0; c.MaxPastSkew = 0 })

	if got := g.Store(120*nanos+1, "k"); got != FutureRejected {
		t.Fatalf("unconfigured future skew = %d, want %d", got, FutureRejected)
	}
	if got := g.Store(-120*nanos-1, "k"); got != LateRejected {
		t.Fatalf("unconfigured past skew = %d, want %d", got, LateRejected)
	}
	if got := g.Store(120*nanos, "k"); got < 1 {
		t.Fatalf("Store(+120s) = %d, want delegation", got)
	}
}

func TestExplicitSkewsOverrideDefaults(t *testing.T) {
	spy := &spyCache{}
	g := newGuard(t, spy, func(c *Config) { c.MaxFutureSkew = time.Second; c.MaxPastSkew = 2 * time.Second })

	if got := g.Store(nanos+1, "k"); got != FutureRejected {
		t.Fatalf("Store(+1s+1ns) = %d, want %d", got, FutureRejected)
	}
	if got := g.Store(nanos, "k"); got < 1 {
		t.Fatalf("Store(+1s) = %d, want delegation", got)
	}
	if got := g.Store(-2*nanos-1, "k"); got != LateRejected {
		t.Fatalf("Store(-2s-1ns) = %d, want %d", got, LateRejected)
	}
	if got := g.Store(-2*nanos, "k"); got < 1 {
		t.Fatalf("Store(-2s) = %d, want delegation", got)
	}
}

func TestEpochUnitMillis(t *testing.T) {
	// With a millis cache, the skews convert to the epoch unit and Now must
	// return millis. 120s = 120_000ms.
	spy := &spyCache{}
	millis := slidingcache.EpochInMillis
	g := newGuard(t, spy, func(c *Config) {
		c.EpochUnit = &millis
		c.Now = func() int64 { return 0 }
	})

	if got := g.Store(120_001, "k"); got != FutureRejected {
		t.Fatalf("Store(+120.001s in millis) = %d, want %d", got, FutureRejected)
	}
	if got := g.Store(120_000, "k"); got < 1 {
		t.Fatalf("Store(+120s in millis) = %d, want delegation", got)
	}
	if got := g.Store(-120_001, "k"); got != LateRejected {
		t.Fatalf("Store(-120.001s in millis) = %d, want %d", got, LateRejected)
	}
}

func TestDelegatedValuesPassThrough(t *testing.T) {
	// The cache's own -1 for late events is forwarded verbatim for epochs
	// inside the skews: the guard only adds the future rejection.
	spy := &spyCache{storeFn: func(_ int64, _ string) int { return -1 }}
	g := newGuard(t, spy, nil)
	if got := g.Store(0, "k"); got != -1 {
		t.Fatalf("Store forwarding cache -1 = %d, want -1", got)
	}
}

func TestGuardOverRealCache(t *testing.T) {
	// End-to-end: the +3h jump that poisons a bare cache is discarded by the
	// guard, and real-time ingest continues untouched.
	c, err := slidingcache.New(slidingcache.Config{
		Precision:  time.Second,
		WindowSize: 1800 * time.Second,
		EpochUnit:  slidingcache.EpochInNanos,
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer c.Close()

	clk := &clock{value: 1_700_000_000 * nanos}
	g, err := New(c, Config{Enabled: true, Now: clk.now})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	for range 1800 { // fill the window, one event per second per key.
		clk.value += nanos
		g.Store(clk.value, "k")
	}
	full := g.Get(clk.value, "k")

	if got := g.Store(clk.value+3*3600*nanos, "future"); got != FutureRejected {
		t.Fatalf("future Store = %d, want %d", got, FutureRejected)
	}
	clk.value += nanos
	// Steady state: HW advances to t+1s, so the oldest 1s bucket leaves the
	// 1800s window as the new one enters -- one in, one out, count unchanged.
	// The rejected jump changed nothing: ingest continues at full count.
	if got := g.Store(clk.value, "k"); got != full {
		t.Fatalf("real-time Store after rejected jump = %d, want %d", got, full)
	}
}

func TestConcurrent(t *testing.T) {
	c, err := slidingcache.New(slidingcache.Config{
		Precision:     time.Second,
		WindowSize:    1800 * time.Second,
		EpochUnit:     slidingcache.EpochInNanos,
		SweepInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer c.Close()

	var now atomic.Int64
	now.Store(1_700_000_000 * nanos)
	g, err := New(c, Config{Enabled: true, Now: now.Load})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10_000 {
				epoch := now.Load() + nanos/1000 // +1ms span per iteration
				g.Store(epoch, "key")
				g.Get(epoch, "key")
			}
		})
	}
	wg.Wait()
}
