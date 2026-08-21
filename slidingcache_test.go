package slidingcache

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

// newTestCache builds a Cache with a long sweep interval so the background
// janitor never interferes with deterministic assertions. Tests that need
// sweeping call c.sweep() directly.
func newTestCache(t *testing.T, cfg Config) *Cache {
	t.Helper()
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = time.Hour
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v) returned error: %v", cfg, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (c *Cache) totalKeys() int {
	total := 0
	for _, s := range c.shards {
		total += s.size()
	}
	return total
}

func TestStoreOnNewKeyReturnsOneAndIncrements(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	if got := c.Store(1000, "a"); got != 1 {
		t.Fatalf("first Store = %d, want 1", got)
	}
	if got := c.Store(1001, "a"); got != 2 {
		t.Fatalf("second Store = %d, want 2", got)
	}
	if got := c.Store(1002, "a"); got != 3 {
		t.Fatalf("third Store = %d, want 3", got)
	}
	if got := c.Get(1002, "a"); got != 3 {
		t.Fatalf("Get = %d, want 3", got)
	}
}

func TestGetMissingKeyReturnsZero(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	if got := c.Get(1000, "absent"); got != 0 {
		t.Fatalf("Get(absent) = %d, want 0", got)
	}
}

func TestExpiryWhenHighWaterAdvances(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "a")
	c.Store(1005, "a")
	if got := c.Get(1005, "a"); got != 2 {
		t.Fatalf("Get before expiry = %d, want 2", got)
	}

	// Boundary: alive iff t > HW - WindowSize. Advancing HW to 1060 makes the
	// cutoff exactly 1000, so t=1000 (not > 1000) expires while t=1005 survives.
	c.Store(1060, "b")
	if got := c.Get(1060, "a"); got != 1 {
		t.Fatalf("Get at boundary = %d, want 1 (only t=1005 alive)", got)
	}

	// Advance HW far beyond the window: every timestamp for "a" expires. The key
	// itself survives until a sweep removes it.
	c.Store(5000, "b")
	if got := c.Get(5000, "a"); got != 0 {
		t.Fatalf("Get after full expiry = %d, want 0", got)
	}
}

func TestOutOfOrderStore(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 100 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "a") // sets HW = 1000
	// Older but still live (1000 - 950 = 50 < window 100): counted.
	if got := c.Store(950, "a"); got != 2 {
		t.Fatalf("out-of-order live Store = %d, want 2", got)
	}

	// Expired event: t=850, cutoff = HW(1000) - 100 = 900, 850 <= 900 -> rejected.
	if got := c.Store(850, "a"); got != -1 {
		t.Fatalf("expired Store = %d, want -1 (late event)", got)
	}
	if got := c.Get(1000, "a"); got != 2 {
		t.Fatalf("Get after rejected Store = %d, want unchanged 2", got)
	}
}

func TestExpiredStoreReturnsMinusOne(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(10000, "hw") // push HW far into the future.

	// Absent key: still not created, returns -1.
	if got := c.Store(100, "absent"); got != -1 {
		t.Fatalf("expired Store on absent key = %d, want -1", got)
	}
	if c.Get(10000, "absent") != 0 {
		t.Fatalf("absent key should not have been created")
	}

	// Present-but-expired key: existing live events are untouched, returns -1.
	c.Store(10000, "present")
	if got := c.Store(100, "present"); got != -1 {
		t.Fatalf("expired Store on present key = %d, want -1", got)
	}
	if got := c.Get(10000, "present"); got != 1 {
		t.Fatalf("Get after expired Store on present key = %d, want unchanged 1", got)
	}
}

func TestExpiredGetReturnsMinusOne(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(10000, "present") // HW = 10000, cutoff = 9940.

	// Querying an epoch outside the live window returns -1 regardless of key
	// presence.
	if got := c.Get(100, "present"); got != -1 {
		t.Fatalf("Get with expired epoch (present key) = %d, want -1", got)
	}
	if got := c.Get(100, "absent"); got != -1 {
		t.Fatalf("Get with expired epoch (absent key) = %d, want -1", got)
	}

	// An in-window epoch still returns the live count / 0.
	if got := c.Get(10000, "present"); got != 1 {
		t.Fatalf("Get with in-window epoch = %d, want 1", got)
	}
	if got := c.Get(10000, "absent"); got != 0 {
		t.Fatalf("Get in-window absent key = %d, want 0", got)
	}
}

func TestLateEventBoundary(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "hw") // HW = 1000, cutoff = 940.

	// t == HW - WindowSize (940): strictly not alive -> -1 for both Store and Get.
	if got := c.Store(940, "a"); got != -1 {
		t.Fatalf("Store at boundary t==HW-W = %d, want -1", got)
	}
	if got := c.Get(940, "a"); got != -1 {
		t.Fatalf("Get at boundary t==HW-W = %d, want -1", got)
	}

	// t just inside the window (941): normal behavior.
	if got := c.Store(941, "a"); got != 1 {
		t.Fatalf("Store just inside window = %d, want 1", got)
	}
	if got := c.Get(941, "a"); got != 1 {
		t.Fatalf("Get just inside window = %d, want 1", got)
	}
}

func TestPrecisionTruncationCountsEachEvent(t *testing.T) {
	c := newTestCache(t, Config{Precision: 10 * time.Second, WindowSize: 100 * time.Second, EpochUnit: EpochInSeconds})

	// 1001..1009 all truncate to bucket 1000 but are stored as distinct events.
	for epoch := int64(1001); epoch <= 1009; epoch++ {
		c.Store(epoch, "a")
	}
	if got := c.Get(1009, "a"); got != 9 {
		t.Fatalf("Get = %d, want 9 events in the same bucket", got)
	}
}

func TestEpochUnitConversionsAgree(t *testing.T) {
	instantSeconds := int64(1_700_000_000)
	cases := []struct {
		unit  EpochUnit
		epoch int64
	}{
		{EpochInSeconds, instantSeconds},
		{EpochInMillis, instantSeconds * 1_000},
		{EpochInNanos, instantSeconds * 1_000_000_000},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("unit_%d", tc.unit), func(t *testing.T) {
			c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: tc.unit})
			c.Store(tc.epoch, "a")
			c.Store(tc.epoch, "a")
			if got := c.Get(tc.epoch, "a"); got != 2 {
				t.Fatalf("Get = %d, want 2", got)
			}
		})
	}
}

// TestEpochUnitConversionsAgreeAcrossOffsets exercises expiry across every unit
// with a relative time offset. Storing at the same instant (as
// TestEpochUnitConversionsAgree does) cannot detect a wrong seconds-per-unit
// divisor because it never crosses a window boundary; a 61s offset does.
func TestEpochUnitConversionsAgreeAcrossOffsets(t *testing.T) {
	const baseSeconds = int64(1_700_000_000)
	cases := []struct {
		unit           EpochUnit
		unitsPerSecond int64
	}{
		{EpochInMillis, 1_000},
		{EpochInNanos, 1_000_000_000},
		{EpochInSeconds, 1},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("unit_%d", tc.unit), func(t *testing.T) {
			c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: tc.unit})
			base := baseSeconds * tc.unitsPerSecond
			secondsBehind := func(n int64) int64 { return base - n*tc.unitsPerSecond }

			if got := c.Store(base, "a"); got != 1 {
				t.Fatalf("Store(base) = %d, want 1", got)
			}
			// 61s behind the high-water mark: expired, rejected.
			if got := c.Store(secondsBehind(61), "a"); got != -1 {
				t.Fatalf("Store(base-61s) = %d, want -1 (late)", got)
			}
			// 59s behind: still alive, appended to the key.
			if got := c.Store(secondsBehind(59), "a"); got != 2 {
				t.Fatalf("Store(base-59s) = %d, want 2 (alive)", got)
			}
		})
	}
}

// TestRealClockUnixMilliScenario reproduces the README example against a real
// time.Now().UnixMilli() clock. Offsets are whole seconds, and
// (now.Add(-Ns)).UnixMilli()/1000 == now.UnixMilli()/1000 - N exactly, so the
// assertions are deterministic despite reading the wall clock.
func TestRealClockUnixMilliScenario(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInMillis})

	now := time.Now()

	if got := c.Store(now.UnixMilli(), "user-42"); got != 1 {
		t.Fatalf("Store(now) = %d, want 1", got)
	}
	if got := c.Store(now.UnixMilli(), "user-42"); got != 2 {
		t.Fatalf("Store(now) again = %d, want 2", got)
	}
	if got := c.Get(now.UnixMilli(), "user-42"); got != 2 {
		t.Fatalf("Get(now) = %d, want 2", got)
	}

	sec61 := now.Add(-61 * time.Second)
	if got := c.Store(sec61.UnixMilli(), "user-42"); got != -1 {
		t.Fatalf("Store(now-61s) = %d, want -1 (late)", got)
	}
	if got := c.Get(sec61.UnixMilli(), "user-42"); got != -1 {
		t.Fatalf("Get(now-61s) = %d, want -1", got)
	}

	sec302 := now.Add(-302 * time.Second)
	if got := c.Store(sec302.UnixMilli(), "user-42"); got != -1 {
		t.Fatalf("Store(now-302s) = %d, want -1 (late)", got)
	}
	if got := c.Get(sec302.UnixMilli(), "user-42"); got != -1 {
		t.Fatalf("Get(now-302s) = %d, want -1", got)
	}

	if got := c.Get(now.UnixMilli(), "user-42"); got != 2 {
		t.Fatalf("Get(now) after late events = %d, want unchanged 2", got)
	}
}

// TestRealClockUnixMilliBoundary pins the strict window boundary against a real
// millisecond clock: exactly WindowSize behind the high-water mark is expired,
// one second inside is alive.
func TestRealClockUnixMilliBoundary(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInMillis})

	now := time.Now()
	c.Store(now.UnixMilli(), "hw") // HW = now (seconds), cutoff = now - 60s.

	// Exactly 60s behind: t == HW - WindowSize, strictly not alive.
	sec60 := now.Add(-60 * time.Second)
	if got := c.Store(sec60.UnixMilli(), "a"); got != -1 {
		t.Fatalf("Store(now-60s) = %d, want -1 (boundary expired)", got)
	}
	if got := c.Get(sec60.UnixMilli(), "a"); got != -1 {
		t.Fatalf("Get(now-60s) = %d, want -1", got)
	}

	// 59s behind: just inside the window, alive.
	sec59 := now.Add(-59 * time.Second)
	if got := c.Store(sec59.UnixMilli(), "a"); got != 1 {
		t.Fatalf("Store(now-59s) = %d, want 1 (alive)", got)
	}
	if got := c.Get(sec59.UnixMilli(), "a"); got != 1 {
		t.Fatalf("Get(now-59s) = %d, want 1", got)
	}
}

func TestStoreAfterWindowExpiresRealTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-time expiry test in -short mode (sleeps 1s and 3s)")
	}
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 2 * time.Second, EpochUnit: EpochInMillis})
	itemTimeUnixMilliTest := time.Now().UnixMilli()
	hashItemTest := "user-42"
	if got := c.Store(itemTimeUnixMilliTest, hashItemTest); got != 1 {
		t.Fatalf("first Store = %d, want 1", got)
	}
	time.Sleep(1 * time.Second)
	if got := c.Store(itemTimeUnixMilliTest, hashItemTest); got != 2 {
		t.Fatalf("second Store = %d, want 2", got)
	}
	time.Sleep(3 * time.Second)
	// Wall-clock time has passed, but the cache's window is anchored on the
	// high-water mark (the max epoch seen by Store), not on time.Now(). Nothing
	// newer has been stored yet, so the original epoch is still live and a
	// third Store of it simply increments.
	if got := c.Store(itemTimeUnixMilliTest, hashItemTest); got != 3 {
		t.Fatalf("third Store (same epoch, HW unchanged) = %d, want 3", got)
	}
	// Storing a fresh epoch advances the high-water mark past the window, which
	// expires the earlier events: the new event is the only live one.
	fresh := time.Now().UnixMilli()
	if got := c.Store(fresh, hashItemTest); got != 1 {
		t.Fatalf("Store after sleeping past the window = %d, want 1 (earlier events expired)", got)
	}
	if got := c.Get(fresh, hashItemTest); got != 1 {
		t.Fatalf("Get after sleeping past the window = %d, want 1", got)
	}
	// Now that HW has moved on, the original epoch is late and gets rejected.
	if got := c.Store(itemTimeUnixMilliTest, hashItemTest); got != -1 {
		t.Fatalf("Store of original epoch after window advanced = %d, want -1 (late event)", got)
	}
	if got := c.Get(fresh, hashItemTest); got != 1 {
		t.Fatalf("Get after rejected late Store = %d, want 1 (late event not stored)", got)
	}
}

// TestEpochUnitZeroValueDefaultsToMillis asserts the zero value of EpochUnit is
// milliseconds, so a Config that omits EpochUnit interprets epochs as UnixMilli.
func TestEpochUnitZeroValueDefaultsToMillis(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second})

	if c.unit != EpochInMillis {
		t.Fatalf("default EpochUnit = %d, want EpochInMillis (%d)", c.unit, EpochInMillis)
	}

	baseMillis := int64(1_700_000_000) * 1_000
	if got := c.Store(baseMillis, "a"); got != 1 {
		t.Fatalf("Store(base) = %d, want 1", got)
	}
	// 61s earlier expressed in millis lands outside the 60s window.
	if got := c.Store(baseMillis-61_000, "a"); got != -1 {
		t.Fatalf("Store(base-61s in millis) = %d, want -1 (late)", got)
	}
	// 59s earlier is still inside the window.
	if got := c.Store(baseMillis-59_000, "a"); got != 2 {
		t.Fatalf("Store(base-59s in millis) = %d, want 2 (alive)", got)
	}
}

// TestEpochUnits reproduces the real-clock late-event scenario across every
// EpochUnit, converting each timestamp with the matching time.Time method so a
// wrong per-unit divisor would surface as a mismatched window boundary.
func TestEpochUnits(t *testing.T) {
	cases := []struct {
		unit    EpochUnit
		toEpoch func(time.Time) int64
	}{
		{EpochInMillis, time.Time.UnixMilli},
		{EpochInSeconds, time.Time.Unix},
		{EpochInNanos, time.Time.UnixNano},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("unit_%d", tc.unit), func(t *testing.T) {
			c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: tc.unit})

			now := time.Now()
			at := func(offset time.Duration) int64 { return tc.toEpoch(now.Add(offset)) }

			if got := c.Store(at(0), "user-42"); got != 1 {
				t.Fatalf("Store(now) = %d, want 1", got)
			}
			if got := c.Store(at(0), "user-42"); got != 2 {
				t.Fatalf("Store(now) again = %d, want 2", got)
			}
			if got := c.Get(at(0), "user-42"); got != 2 {
				t.Fatalf("Get(now) = %d, want 2", got)
			}

			if got := c.Store(at(-61*time.Second), "user-42"); got != -1 {
				t.Fatalf("Store(now-61s) = %d, want -1 (late)", got)
			}
			if got := c.Get(at(-61*time.Second), "user-42"); got != -1 {
				t.Fatalf("Get(now-61s) = %d, want -1", got)
			}

			if got := c.Store(at(-302*time.Second), "user-42"); got != -1 {
				t.Fatalf("Store(now-302s) = %d, want -1 (late)", got)
			}
			if got := c.Get(at(-302*time.Second), "user-42"); got != -1 {
				t.Fatalf("Get(now-302s) = %d, want -1", got)
			}
		})
	}
}

func TestManyKeysManyWindowsAndExpiry(t *testing.T) {
	const (
		keys           = 10_000
		windows        = 100
		precision      = time.Second
		windowSize     = 10 * time.Second
		eventsPerBurst = 3
	)
	c := newTestCache(t, Config{Precision: precision, WindowSize: windowSize, EpochUnit: EpochInSeconds})

	// Every key gets eventsPerBurst events in its own window, windows far apart
	// so that each burst fully expires the previous one.
	for w := range windows {
		epoch := int64(w) * 1000
		for k := range keys {
			key := fmt.Sprintf("key-%d", k)
			for range eventsPerBurst {
				c.Store(epoch, key)
			}
		}
	}

	lastEpoch := int64(windows-1) * 1000
	for k := range keys {
		key := fmt.Sprintf("key-%d", k)
		if got := c.Get(lastEpoch, key); got != eventsPerBurst {
			t.Fatalf("Get(%s) = %d, want %d", key, got, eventsPerBurst)
		}
	}

	// Advance HW well past the window so every key is fully expired, then sweep.
	c.Store(lastEpoch+1_000_000, "future")
	c.sweep()

	if got := c.totalKeys(); got != 1 {
		t.Fatalf("totalKeys after sweep = %d, want 1 (only the future key)", got)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"precision zero", Config{Precision: 0, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds}},
		{
			"precision below one second",
			Config{Precision: 500 * time.Millisecond, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds},
		},
		{
			"precision not whole seconds",
			Config{Precision: 1500 * time.Millisecond, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds},
		},
		{
			"window below one second",
			Config{Precision: time.Second, WindowSize: 500 * time.Millisecond, EpochUnit: EpochInSeconds},
		},
		{
			"window not whole seconds",
			Config{Precision: time.Second, WindowSize: 1500 * time.Millisecond, EpochUnit: EpochInSeconds},
		},
		{
			"window below precision",
			Config{Precision: 10 * time.Second, WindowSize: 5 * time.Second, EpochUnit: EpochInSeconds},
		},
		{"invalid epoch unit", Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochUnit(99)}},
		{
			"negative shards",
			Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds, Shards: -1},
		},
		{
			"shards above maximum",
			Config{
				Precision:  time.Second,
				WindowSize: 60 * time.Second,
				EpochUnit:  EpochInSeconds,
				Shards:     maxShards + 1,
			},
		},
		{
			"negative sweep interval",
			Config{
				Precision:     time.Second,
				WindowSize:    60 * time.Second,
				EpochUnit:     EpochInSeconds,
				SweepInterval: -time.Second,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Fatalf("New(%+v) = nil error, want error", tc.cfg)
			}
		})
	}
}

func TestValidConfigDefaults(t *testing.T) {
	c, err := New(Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer c.Close()

	if len(c.shards) != defaultShards {
		t.Fatalf("shard count = %d, want default %d", len(c.shards), defaultShards)
	}
	if c.sweepEvery != 60*time.Second {
		t.Fatalf("sweepEvery = %s, want 60s (WindowSize default)", c.sweepEvery)
	}
}

func TestConcurrentStoreAndGet(t *testing.T) {
	c := newTestCache(
		t,
		Config{Precision: time.Second, WindowSize: 1_000_000 * time.Second, EpochUnit: EpochInSeconds, Shards: 8},
	)

	const (
		goroutines   = 8
		perGoroutine = 2000
		sharedKeys   = 16
		baseEpoch    = 1_000_000
	)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				key := fmt.Sprintf("key-%d", i%sharedKeys)
				c.Store(int64(baseEpoch+i), key)
				c.Get(int64(baseEpoch+i), key)
			}
		})
	}
	wg.Wait()

	// Window is large enough that nothing expires; every event is retained.
	total := 0
	for k := range sharedKeys {
		total += c.Get(baseEpoch+perGoroutine, fmt.Sprintf("key-%d", k))
	}
	want := goroutines * perGoroutine
	if total != want {
		t.Fatalf("total stored events = %d, want %d", total, want)
	}
}

func TestCloseIsIdempotentAndConcurrencySafe(t *testing.T) {
	c, err := New(
		Config{
			Precision:     time.Second,
			WindowSize:    60 * time.Second,
			EpochUnit:     EpochInSeconds,
			SweepInterval: time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	// Let the janitor tick at least once.
	time.Sleep(5 * time.Millisecond)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := c.Close(); err != nil {
				t.Errorf("Close returned error: %v", err)
			}
		})
	}
	wg.Wait()

	// A further Close is still safe.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestSweepCompactsShrunkenMap(t *testing.T) {
	c := newTestCache(
		t,
		Config{Precision: time.Second, WindowSize: 10 * time.Second, EpochUnit: EpochInSeconds, Shards: 1},
	)
	shard := c.shards[0]

	const keys = 4000 // above mapCompactMinPeak so compaction can trigger.
	for k := range keys {
		c.Store(1000, fmt.Sprintf("key-%d", k))
	}
	if shard.peak < keys {
		t.Fatalf("peak = %d, want >= %d", shard.peak, keys)
	}
	mapBefore := reflect.ValueOf(shard.keys).Pointer()

	// Expire every key, then sweep to prune and compact.
	c.Store(1_000_000, "future")
	c.sweep()

	if got := shard.size(); got != 1 {
		t.Fatalf("shard size after sweep = %d, want 1 (only future key)", got)
	}
	if shard.peak != 1 {
		t.Fatalf("peak after compaction = %d, want reset to 1", shard.peak)
	}
	if reflect.ValueOf(shard.keys).Pointer() == mapBefore {
		t.Fatalf("shard map was not rebuilt during compaction")
	}
}

func TestWithHashFuncNilReturnsError(t *testing.T) {
	c, err := New(secondsConfig(), WithHashFunc(nil))
	if err == nil {
		_ = c.Close()
		t.Fatal("New with WithHashFunc(nil) = nil error, want error")
	}
}

func TestWithHashFuncRoutesByCustomHash(t *testing.T) {
	cfg := secondsConfig()
	cfg.Shards = 4
	const keys = 50
	allToShardZero := func(string) uint64 { return 0 }

	c, err := New(cfg, WithHashFunc(allToShardZero))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	for k := range keys {
		c.Store(1000, fmt.Sprintf("key-%d", k))
	}

	if got := c.shards[0].size(); got != keys {
		t.Fatalf("shards[0].size() = %d, want %d", got, keys)
	}
	for i := 1; i < len(c.shards); i++ {
		if got := c.shards[i].size(); got != 0 {
			t.Fatalf("shards[%d].size() = %d, want 0", i, got)
		}
	}
}

func TestWithHashFuncRoundTripCounts(t *testing.T) {
	xorHash := func(key string) uint64 {
		var h uint64
		for i := 0; i < len(key); i++ {
			h ^= uint64(key[i])
		}
		return h
	}

	c, err := New(secondsConfig(), WithHashFunc(xorHash))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	c.Store(1000, "a")
	c.Store(1000, "a")
	c.Store(1000, "b")

	if got := c.Get(1000, "a"); got != 2 {
		t.Fatalf("Get(a) = %d, want 2", got)
	}
	if got := c.Get(1000, "b"); got != 1 {
		t.Fatalf("Get(b) = %d, want 1", got)
	}
	if got := c.Get(1000, "absent"); got != 0 {
		t.Fatalf("Get(absent) = %d, want 0", got)
	}
}

func secondsConfig() Config {
	return Config{Precision: time.Second, WindowSize: 3600 * time.Second, EpochUnit: EpochInSeconds}
}

// retainedEvents reports how many events are physically retained for key,
// those in expired buckets included, or -1 when the key is absent. It
// distinguishes "counted as zero" from "actually removed".
func (c *Cache) retainedEvents(key string) int {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.keys[key]
	if !ok {
		return -1
	}
	return e.total
}

// bucketBreadth reports how many Precision buckets are physically retained for
// key, expired ones included, or -1 when the key is absent. It is the memory the
// key occupies, which the run-length encoding keeps independent of the event
// rate.
func (c *Cache) bucketBreadth(key string) int {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.keys[key]
	if !ok {
		return -1
	}
	return len(e.buckets)
}

func TestRoundUpPow2(t *testing.T) {
	cases := []struct {
		n    int
		want int
	}{
		{-5, 1},
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{16, 16},
		{17, 32},
		{maxShards - 1, maxShards},
		{maxShards, maxShards},
	}

	for _, tc := range cases {
		if got := roundUpPow2(tc.n); got != tc.want {
			t.Fatalf("roundUpPow2(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

// TestRoundUpPow2TerminatesForHugeN guards the loop-free implementation: a
// doubling loop never reaches an n above 1<<62, so it would hang the test binary
// rather than fail it.
func TestRoundUpPow2TerminatesForHugeN(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = roundUpPow2(1<<62 + 1)
		_ = roundUpPow2(math.MaxInt64)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("roundUpPow2 did not terminate for a huge n")
	}
}

// TestStoreRejectsTimestampExpiredByConcurrentHighWaterAdvance reproduces
// deterministically the interleaving that a stale cutoff would mishandle: a
// Store passes the pre-lock liveness check, and a concurrent Store advances the
// high-water mark past its timestamp before the shard lock is acquired.
func TestStoreRejectsTimestampExpiredByConcurrentHighWaterAdvance(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "a") // HW = 1000, cutoff = 940: t=950 is alive here.
	c.advanceHighWater(2000)

	if got := c.shardFor("a").store("a", 950, &c.highWater, c.windowSize); got != lateEvent {
		t.Fatalf("store under an advanced high-water mark = %d, want %d", got, lateEvent)
	}
	if got := c.retainedEvents("a"); got != 1 {
		t.Fatalf("retained events = %d, want 1 (the late event must not be inserted)", got)
	}
	if got := c.Get(2000, "a"); got != 0 {
		t.Fatalf("Get after the rejected store = %d, want 0", got)
	}
}

func TestNegativeEpochsTruncateTowardNegativeInfinity(t *testing.T) {
	c := newTestCache(t, Config{Precision: 10 * time.Second, WindowSize: 10 * time.Second, EpochUnit: EpochInSeconds})

	// Bucket boundaries are uniform across zero: epoch -5 belongs to bucket -10,
	// exactly WindowSize behind the high-water mark and therefore late.
	// Truncating toward zero would place -5 in bucket 0 and count it as live.
	if got := c.Store(0, "k"); got != 1 {
		t.Fatalf("Store(0) = %d, want 1", got)
	}
	if got := c.Store(-5, "k"); got != -1 {
		t.Fatalf("Store(-5) = %d, want -1 (bucket -10 is exactly WindowSize behind)", got)
	}
	if got := c.Get(0, "k"); got != 1 {
		t.Fatalf("Get(0) = %d, want unchanged 1", got)
	}
}

// TestFirstStoreOnFreshCacheAcceptsNegativeEpoch pins that a Cache with no
// observed epoch has no window yet, so its first event is accepted whatever its
// sign. A zero-valued high-water mark would reject anything before the epoch.
func TestFirstStoreOnFreshCacheAcceptsNegativeEpoch(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	if got := c.Store(-100, "k"); got != 1 {
		t.Fatalf("Store(-100) on a fresh cache = %d, want 1", got)
	}
	if got := c.Get(-100, "k"); got != 1 {
		t.Fatalf("Get(-100) = %d, want 1", got)
	}
}

// TestUnobservedHighWaterDoesNotUnderflow exercises every reader of the
// high-water mark against its "never stored" value, where a plain
// HW - WindowSize subtraction would wrap around.
func TestUnobservedHighWaterDoesNotUnderflow(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	if got := c.Get(math.MinInt64+1, "absent"); got != 0 {
		t.Fatalf("Get on a fresh cache = %d, want 0", got)
	}
	c.sweep()
	if got := c.totalKeys(); got != 0 {
		t.Fatalf("totalKeys after sweeping a fresh cache = %d, want 0", got)
	}
	if got := c.Store(math.MinInt64+1, "k"); got != 1 {
		t.Fatalf("Store at the smallest usable epoch = %d, want 1", got)
	}
}

// TestGetIsReadOnlyAndOnlySweepRemovesDeadKeys pins the cleanup contract: Get
// never mutates an entry, and a key whose events have all expired disappears
// only when the janitor sweeps.
func TestGetIsReadOnlyAndOnlySweepRemovesDeadKeys(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "a")
	c.Store(1001, "a")
	c.Store(9000, "hw") // every event for "a" is now expired.

	if got := c.Get(9000, "a"); got != 0 {
		t.Fatalf("Get after expiry = %d, want 0", got)
	}
	if got := c.retainedEvents("a"); got != 2 {
		t.Fatalf("retained events after Get = %d, want 2 (Get must not mutate)", got)
	}

	c.sweep()

	if got := c.retainedEvents("a"); got != -1 {
		t.Fatalf("key retained %d events after sweep, want the key removed", got)
	}
	if got := c.Get(9000, "a"); got != 0 {
		t.Fatalf("Get after sweep = %d, want 0", got)
	}
}

// TestStorePrunesExpiredPrefixOfTouchedKey pins that a Store drops the key's
// dead buckets before inserting, so a long-lived key does not accumulate them
// between sweeps.
func TestStorePrunesExpiredPrefixOfTouchedKey(t *testing.T) {
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 60 * time.Second, EpochUnit: EpochInSeconds})

	c.Store(1000, "a")
	c.Store(1001, "a")
	c.Store(9000, "hw")

	if got := c.Store(9000, "a"); got != 1 {
		t.Fatalf("Store after full expiry = %d, want 1", got)
	}
	if got := c.retainedEvents("a"); got != 1 {
		t.Fatalf("retained events = %d, want 1 (expired prefix not pruned)", got)
	}
	// Pruning is confined to the touched key; an untouched one waits for a sweep.
	if got := c.retainedEvents("hw"); got != 1 {
		t.Fatalf("retained events for the untouched key = %d, want 1", got)
	}
}

// windowModel is a deliberately naive reference implementation of the window
// semantics: one unsorted slice of timestamps per key and a linear scan. It
// shares no code with the cache, so any disagreement indicts the cache.
type windowModel struct {
	precision  int64
	windowSize int64
	highWater  int64
	observed   bool
	events     map[string][]int64
}

func newWindowModel(precision, windowSize int64) *windowModel {
	return &windowModel{precision: precision, windowSize: windowSize, events: make(map[string][]int64)}
}

func (m *windowModel) bucket(epoch int64) int64 {
	remainder := epoch % m.precision
	if remainder < 0 {
		remainder += m.precision
	}
	return epoch - remainder
}

func (m *windowModel) alive(timestamp int64) bool {
	return !m.observed || timestamp > m.highWater-m.windowSize
}

func (m *windowModel) store(epoch int64, key string) int {
	timestamp := m.bucket(epoch)
	if !m.observed || timestamp > m.highWater {
		m.highWater = timestamp
		m.observed = true
	}
	if !m.alive(timestamp) {
		return lateEvent
	}
	m.events[key] = append(m.events[key], timestamp)
	return m.count(key)
}

func (m *windowModel) get(epoch int64, key string) int {
	if !m.alive(m.bucket(epoch)) {
		return lateEvent
	}
	return m.count(key)
}

func (m *windowModel) count(key string) int {
	live := 0
	for _, timestamp := range m.events[key] {
		if m.alive(timestamp) {
			live++
		}
	}
	return live
}

// FuzzStoreGetInvariants drives arbitrary Store/Get sequences over a handful of
// keys and asserts every return value against the reference model, including the
// live counts left behind at the end.
func FuzzStoreGetInvariants(f *testing.F) {
	seeds := []struct {
		base int64
		ops  []byte
	}{
		{1_700_000_000, []byte{0, 1, 2, 200, 3}},
		{0, []byte{250, 251, 0, 5, 7}},
		{-1_000, []byte{0, 60, 120, 1, 255}},
		// Many duplicates of one bucket for one key, then an out-of-order
		// duplicate of an earlier bucket, which exercises ordered insertion into
		// an existing run of equal timestamps.
		{1_700_000_000, []byte{128, 128, 128, 128, 128, 120, 120, 128, 132}},
		// Repeats of an older bucket interleaved with newer ones, so an
		// out-of-order arrival lands on a bucket that earlier prunes may already
		// have dropped.
		{1_700_000_000, []byte{128, 128, 136, 136, 144, 128, 120, 128}},
	}
	for _, seed := range seeds {
		f.Add(seed.base, seed.ops)
	}

	f.Fuzz(func(t *testing.T, base int64, ops []byte) {
		const (
			precision  = 10
			windowSize = 60
			epochScale = 1 << 40
			maxFuzzOps = 256
		)

		// Epochs are kept well inside int64 so the reference model's arithmetic
		// cannot overflow; the semantics under test do not depend on scale. A
		// bounded op count keeps every execution fast, which buys more coverage
		// than replaying arbitrarily long inputs.
		base %= epochScale
		if len(ops) > maxFuzzOps {
			ops = ops[:maxFuzzOps]
		}

		c := newTestCache(t, Config{
			Precision:  precision * time.Second,
			WindowSize: windowSize * time.Second,
			EpochUnit:  EpochInSeconds,
		})
		model := newWindowModel(precision, windowSize)

		for _, op := range ops {
			key, epoch := fuzzOperand(base, op, precision)
			if op&fuzzReadBit == 0 {
				want := model.store(epoch, key)
				if got := c.Store(epoch, key); got != want {
					t.Fatalf("Store(%d, %s) = %d, want %d", epoch, key, got, want)
				}
				continue
			}
			want := model.get(epoch, key)
			if got := c.Get(epoch, key); got != want {
				t.Fatalf("Get(%d, %s) = %d, want %d", epoch, key, got, want)
			}
		}

		if !model.observed {
			return
		}
		for k := range fuzzKeys {
			key := fmt.Sprintf("key-%d", k)
			want := model.get(model.highWater, key)
			if got := c.Get(model.highWater, key); got != want {
				t.Fatalf("final Get(%d, %s) = %d, want %d", model.highWater, key, got, want)
			}
		}
	})
}

const (
	fuzzKeys    = 4
	fuzzKeyMask = fuzzKeys - 1
	fuzzReadBit = 1 << 2
)

// fuzzOperand derives a key and an epoch from one input byte: the low bits pick
// the key and a sub-precision jitter (so truncation is exercised), the high bits
// a signed bucket offset wide enough to cross the window in both directions.
func fuzzOperand(base int64, op byte, precision int64) (string, int64) {
	const bucketOffsetBias = 16
	key := fmt.Sprintf("key-%d", op&fuzzKeyMask)
	epoch := base + (int64(op>>3)-bucketOffsetBias)*precision + int64(op&fuzzKeyMask)
	return key, epoch
}

// TestEntryInvariantsHoldForMixedArrivals drives one entry through in-order
// arrivals, repeats of the newest bucket, a repeat of a middle one, a backwards
// jump, a repeat of the smallest, and a new smallest, asserting after every
// arrival that the entry still satisfies the invariants documented on the type
// and that no event was lost or duplicated.
func TestEntryInvariantsHoldForMixedArrivals(t *testing.T) {
	arrivals := []int64{10, 20, 30, 30, 30, 20, 15, 10, 40, 40, 5, 5, 25}

	e := &entry{}
	for i, timestamp := range arrivals {
		e.insert(timestamp)
		requireEntryInvariants(t, e)

		want := slices.Clone(arrivals[:i+1])
		slices.Sort(want)
		if got := e.expanded(); !slices.Equal(got, want) {
			t.Fatalf("after arrival %d (insert %d): events = %v, want %v", i, timestamp, got, want)
		}
	}
}

// TestEntryRecordInOrderIncrementsNewestBucket pins the property the run-length
// encoding exists for: repeats of the newest bucket cost an increment, leaving
// the slice untouched, so a hot key's memory tracks elapsed time and not the
// event rate.
func TestEntryRecordInOrderIncrementsNewestBucket(t *testing.T) {
	e := &entry{buckets: make([]bucket, 0, 16)} // pre-grown so append cannot reallocate.
	for _, timestamp := range []int64{1, 2, 3} {
		e.insert(timestamp)
	}
	backingArray := &e.buckets[0]

	for range 3 {
		e.insert(3)
	}

	requireEntryInvariants(t, e)
	if got, want := len(e.buckets), 3; got != want {
		t.Fatalf("buckets after repeats = %d, want %d (repeats must not grow the slice)", got, want)
	}
	if want := []int64{1, 2, 3, 3, 3, 3}; !slices.Equal(e.expanded(), want) {
		t.Fatalf("events = %v, want %v", e.expanded(), want)
	}
	if &e.buckets[0] != backingArray {
		t.Fatal("repeat of the newest bucket moved earlier buckets, want an in-place increment")
	}

	e.insert(4)

	requireEntryInvariants(t, e)
	if got, want := len(e.buckets), 4; got != want {
		t.Fatalf("buckets after a new timestamp = %d, want %d", got, want)
	}
	if got, want := e.total, 7; got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
}

// TestEntryRecordOutOfOrderIncrementsExistingBucket pins that a late arrival
// only shifts buckets when it opens a new one: a repeat of an older bucket is an
// increment, so jittered timestamps on a hot key cannot degrade into a memmove
// per event.
func TestEntryRecordOutOfOrderIncrementsExistingBucket(t *testing.T) {
	e := &entry{}
	for _, timestamp := range []int64{10, 20, 30} {
		e.insert(timestamp)
	}

	e.insert(20)

	requireEntryInvariants(t, e)
	if got, want := len(e.buckets), 3; got != want {
		t.Fatalf("buckets after repeating an older one = %d, want %d", got, want)
	}
	if got, want := e.buckets[1].count, 2; got != want {
		t.Fatalf("count of the repeated bucket = %d, want %d", got, want)
	}

	e.insert(15)

	requireEntryInvariants(t, e)
	if want := []int64{10, 15, 20, 20, 30}; !slices.Equal(e.expanded(), want) {
		t.Fatalf("events = %v, want %v", e.expanded(), want)
	}
}

// TestEntryPruneBranches covers every way prune can dispose of the expired
// prefix. Which branch runs is a memory decision, not a semantic one, so each
// case also asserts the resulting total.
func TestEntryPruneBranches(t *testing.T) {
	t.Run("nothing expired is a no-op", func(t *testing.T) {
		e := entryWithBuckets(4, 10, 20, 30)
		backingArray, capBefore := &e.buckets[0], cap(e.buckets)

		e.prune(5)

		requireEntryInvariants(t, e)
		if got, want := e.expanded(), []int64{10, 20, 30}; !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
		if &e.buckets[0] != backingArray || cap(e.buckets) != capBefore {
			t.Fatal("prune touched the backing array although nothing had expired")
		}
	})

	t.Run("fully expired small entry keeps its array", func(t *testing.T) {
		e := entryWithBuckets(4, 10, 20, 30)
		backingArray, capBefore := &e.buckets[0], cap(e.buckets)

		e.prune(30)

		requireEntryInvariants(t, e)
		if len(e.buckets) != 0 {
			t.Fatalf("buckets = %v, want empty", e.buckets)
		}
		if cap(e.buckets) != capBefore {
			t.Fatalf("cap = %d, want the array kept at %d for the imminent refill", cap(e.buckets), capBefore)
		}
		if &e.buckets[:1][0] != backingArray {
			t.Fatal("prune reallocated a small fully expired entry")
		}
	})

	t.Run("fully expired large entry releases its array", func(t *testing.T) {
		e := entryWithBuckets(entryReallocMinCap*2, 10, 20, 30)

		e.prune(30)

		requireEntryInvariants(t, e)
		if e.buckets != nil {
			t.Fatalf("buckets = %v, want nil so the large array is collected", e.buckets)
		}
	})

	// Both survivor-run cases give the entry a capacity of exactly twice its
	// survivors, which is the largest capacity that does not trip right-sizing,
	// so the branch under test is the one prune selects.
	t.Run("short survivor run is copied to the front", func(t *testing.T) {
		const survivors, expired = pruneCopyMaxLen, 8 // at the copy threshold.
		e := entryWithBuckets(2*survivors, sequence(0, expired+survivors)...)
		backingArray, capBefore := &e.buckets[0], cap(e.buckets)

		e.prune(expired - 1)

		requireEntryInvariants(t, e)
		if got, want := e.expanded(), sequence(expired, survivors); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
		if &e.buckets[0] != backingArray {
			t.Fatal("short survivor run was not copied to the front of the array")
		}
		if cap(e.buckets) != capBefore {
			t.Fatalf("cap = %d, want the full %d kept for later appends", cap(e.buckets), capBefore)
		}
	})

	t.Run("long survivor run is re-sliced forward", func(t *testing.T) {
		const survivors, expired = pruneCopyMaxLen + 1, 8 // just past the threshold.
		e := entryWithBuckets(2*survivors, sequence(0, expired+survivors)...)
		capBefore := cap(e.buckets)
		firstSurvivor := &e.buckets[expired]

		e.prune(expired - 1)

		requireEntryInvariants(t, e)
		if got, want := e.expanded(), sequence(expired, survivors); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
		if &e.buckets[0] != firstSurvivor {
			t.Fatal("long survivor run was copied, want a free forward re-slice")
		}
		if got, want := cap(e.buckets), capBefore-expired; got != want {
			t.Fatalf("cap = %d, want %d (the dropped prefix is given up)", got, want)
		}
	})

	t.Run("survivors much smaller than the array are right-sized", func(t *testing.T) {
		e := &entry{}
		for i := range int64(200) {
			e.insert(i)
		}
		capBefore := cap(e.buckets)

		e.prune(196)

		requireEntryInvariants(t, e)
		if got, want := e.expanded(), []int64{197, 198, 199}; !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
		if cap(e.buckets) >= capBefore {
			t.Fatalf("cap after prune = %d, want smaller than %d", cap(e.buckets), capBefore)
		}
	})
}

// TestGetOnFullyExpiredEntryWithoutStore exercises liveCount's worst case: a key
// whose buckets have all expired and that no Store or sweep has touched since.
// Get must sum the expired prefix away without mutating the entry.
func TestGetOnFullyExpiredEntryWithoutStore(t *testing.T) {
	const (
		nanosPerSecond  = int64(time.Second)
		baseEpoch       = 1_700_000_000 * nanosPerSecond
		buckets         = 5
		eventsPerBucket = 3
	)
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 300 * time.Second, EpochUnit: EpochInNanos})

	for b := range int64(buckets) {
		for range eventsPerBucket {
			c.Store(baseEpoch+b*nanosPerSecond, "quiet")
		}
	}

	// Another key drags the high-water mark past the window, expiring every
	// bucket of "quiet" without touching its entry.
	liveEpoch := baseEpoch + 400*nanosPerSecond
	c.Store(liveEpoch, "loud")

	if got := c.Get(liveEpoch, "quiet"); got != 0 {
		t.Fatalf("Get on a fully expired entry = %d, want 0", got)
	}
	if got, want := c.retainedEvents("quiet"), buckets*eventsPerBucket; got != want {
		t.Fatalf("retained events after Get = %d, want %d (Get must not mutate)", got, want)
	}
	if got, want := c.bucketBreadth("quiet"), buckets; got != want {
		t.Fatalf("retained buckets after Get = %d, want %d (Get must not mutate)", got, want)
	}

	if got := c.Store(liveEpoch, "quiet"); got != 1 {
		t.Fatalf("Store after full expiry = %d, want 1", got)
	}
	if got := c.bucketBreadth("quiet"); got != 1 {
		t.Fatalf("retained buckets after the Store = %d, want 1 (expired prefix not pruned)", got)
	}
}

// TestStoreSameBucketBoundedMemory pins the memory bound the run-length encoding
// buys: a single key receiving hundreds of events per second for longer than the
// window never holds more buckets than the window spans, while every event is
// still counted.
func TestStoreSameBucketBoundedMemory(t *testing.T) {
	const (
		nanosPerSecond  = int64(time.Second)
		baseEpoch       = 1_700_000_000 * nanosPerSecond
		eventsPerSecond = 500
		nanosPerEvent   = nanosPerSecond / eventsPerSecond
		windowSeconds   = 300
		events          = 200_000 // 400 seconds, comfortably past the window.
		inspectEvery    = 1_000
	)
	c := newTestCache(t, Config{
		Precision:  time.Second,
		WindowSize: windowSeconds * time.Second,
		EpochUnit:  EpochInNanos,
	})

	epochOf := func(i int) int64 { return baseEpoch + int64(i)*nanosPerEvent }
	secondOf := func(i int) int { return i / eventsPerSecond }
	// The generator emits eventsPerSecond events per bucket in order, so the
	// live count after event i is everything stored since the oldest bucket that
	// the cutoff at secondOf(i) still admits.
	liveAfter := func(i int) int {
		oldestAliveSecond := secondOf(i) - windowSeconds + 1
		if oldestAliveSecond <= 0 {
			return i + 1
		}
		return i + 1 - oldestAliveSecond*eventsPerSecond
	}

	for i := range events {
		if got, want := c.Store(epochOf(i), "hot"), liveAfter(i); got != want {
			t.Fatalf("Store #%d = %d, want %d", i+1, got, want)
		}
		if i%inspectEvery != 0 {
			continue
		}
		if got := c.bucketBreadth("hot"); got > windowSeconds {
			t.Fatalf("after %d events the key holds %d buckets, want at most %d", i+1, got, windowSeconds)
		}
	}

	if got, want := c.Get(epochOf(events-1), "hot"), liveAfter(events-1); got != want {
		t.Fatalf("Get = %d, want %d live events", got, want)
	}
	if got := c.bucketBreadth("hot"); got > windowSeconds {
		t.Fatalf("final bucket count = %d, want at most %d", got, windowSeconds)
	}
}

// requireEntryInvariants asserts the invariants documented on entry: buckets
// sorted strictly ascending, every count positive, and total equal to their sum.
func requireEntryInvariants(t *testing.T, e *entry) {
	t.Helper()

	sum := 0
	for i, b := range e.buckets {
		if i > 0 && b.timestamp <= e.buckets[i-1].timestamp {
			t.Fatalf(
				"bucket %d has timestamp %d, want strictly greater than %d",
				i, b.timestamp, e.buckets[i-1].timestamp,
			)
		}
		if b.count < 1 {
			t.Fatalf("bucket %d (timestamp %d) has count %d, want >= 1", i, b.timestamp, b.count)
		}
		sum += b.count
	}
	if e.total != sum {
		t.Fatalf("total = %d, want %d (the sum of the bucket counts)", e.total, sum)
	}
}

// expanded returns the entry's events as one timestamp per event, which is the
// representation the window semantics are defined in.
func (e *entry) expanded() []int64 {
	var out []int64
	for _, b := range e.buckets {
		for range b.count {
			out = append(out, b.timestamp)
		}
	}
	return out
}

// entryWithBuckets builds an entry whose backing array has the given capacity
// and holds one event in each of the given timestamps, so a test can pick the
// prune branch it wants to exercise.
func entryWithBuckets(capacity int, timestamps ...int64) *entry {
	e := &entry{buckets: make([]bucket, 0, capacity)}
	for _, timestamp := range timestamps {
		e.insert(timestamp)
	}
	return e
}

func sequence(start, length int) []int64 {
	out := make([]int64, length)
	for i := range out {
		out[i] = int64(start + i)
	}
	return out
}

func TestStoreSameBucketCountsEveryEvent(t *testing.T) {
	const (
		nanosPerSecond = int64(time.Second)
		baseEpoch      = 1_700_000_000 * nanosPerSecond
		halfSecond     = nanosPerSecond / 2
		events         = 10_000
	)
	c := newTestCache(t, Config{Precision: time.Second, WindowSize: 300 * time.Second, EpochUnit: EpochInNanos})

	for i := range events {
		epoch := baseEpoch
		if i%3 == 0 {
			epoch += halfSecond // same Precision bucket, different nanosecond.
		}
		if got, want := c.Store(epoch, "hot"), i+1; got != want {
			t.Fatalf("Store #%d = %d, want %d", i+1, got, want)
		}
	}
	if got := c.Get(baseEpoch, "hot"); got != events {
		t.Fatalf("Get = %d, want %d", got, events)
	}

	// Advancing past the window expires every same-bucket event at once.
	afterWindow := baseEpoch + 301*nanosPerSecond
	if got := c.Store(afterWindow, "hot"); got != 1 {
		t.Fatalf("Store after the window = %d, want 1", got)
	}
	if got := c.Get(afterWindow, "hot"); got != 1 {
		t.Fatalf("Get after the window = %d, want 1", got)
	}

	// The oldest bucket that survives the new cutoff (HW - WindowSize + 1s)
	// still accepts events, duplicates and out-of-order arrivals included.
	oldestAlive := baseEpoch + 2*nanosPerSecond
	for i, epoch := range []int64{oldestAlive, oldestAlive, afterWindow, oldestAlive} {
		if got, want := c.Store(epoch, "edge"), i+1; got != want {
			t.Fatalf("Store(%d, edge) = %d, want %d", epoch, got, want)
		}
	}
	if got := c.Get(afterWindow, "edge"); got != 4 {
		t.Fatalf("Get(edge) = %d, want 4", got)
	}
}
