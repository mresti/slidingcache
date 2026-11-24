package countminsketch

import (
	"strings"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

// TestNilClockIsTheWallClockInEpochUnit leaves Config.Clock nil, so the guard
// reads the Unix wall clock in the configured unit: an epoch taken from the
// same clock is accepted, one a day ahead is rejected.
func TestNilClockIsTheWallClockInEpochUnit(t *testing.T) {
	units := map[string]struct {
		unit   slidingcache.EpochUnit
		now    func() int64
		perDay int64
	}{
		"seconds": {slidingcache.EpochInSeconds, func() int64 { return time.Now().Unix() }, 86_400},
		"millis":  {slidingcache.EpochInMillis, func() int64 { return time.Now().UnixMilli() }, 86_400_000},
		"nanos":   {slidingcache.EpochInNanos, func() int64 { return time.Now().UnixNano() }, 86_400_000_000_000},
	}
	for name, u := range units {
		t.Run(name, func(t *testing.T) {
			c := newTestCache(t, Config{
				Precision:     time.Second,
				WindowSize:    time.Minute,
				EpochUnit:     u.unit,
				MaxFutureSkew: time.Minute,
			})
			now := u.now()
			requireReturn(t, "Store a day ahead", c.Store(now+u.perDay, "k"), slidingcache.FutureEvent)
			requireReturn(t, "Get a day ahead", c.Get(now+u.perDay, "k"), slidingcache.FutureEvent)
			requireReturn(t, "Store now", c.Store(now, "k"), 1)
		})
	}
}

// TestHighWaterCompareAndSwapRetriesAfterALostRace makes the clock store a
// newer event, so the mark moves between the load in Store and its
// compare-and-swap, which then fails and must keep the newer mark.
func TestHighWaterCompareAndSwapRetriesAfterALostRace(t *testing.T) {
	var c *Cache
	var reentered bool
	cfg := secondsConfig(0)
	cfg.MaxFutureSkew = time.Hour
	cfg.Clock = func() int64 {
		if !reentered {
			reentered = true
			c.Store(2_000, testKeys[1])
		}
		return 2_000
	}
	c = newTestCache(t, cfg, WithHashFunc(exactHash))

	requireReturn(t, "Store that lost the race for the mark", c.Store(1_990, testKeys[0]), 1)
	if hw, _ := c.HighWater(); hw != 2_000 {
		t.Fatalf("HighWater() = %d, want 2000: a lost race must not move the mark back", hw)
	}
	requireReturn(t, "event of the winning Store", c.Get(2_000, testKeys[1]), 1)
}

// TestLiveSlotIsNeverRetagged breaks I2 on purpose, which the code cannot do on
// its own, to show that a bucket landing on a slot still live for another
// bucket stops the cache instead of folding the two together.
func TestLiveSlotIsNeverRetagged(t *testing.T) {
	c := newTestCache(t, secondsConfig(0), WithHashFunc(exactHash))
	c.Store(100, testKeys[0])
	buckets := int64(len(c.window.slots))

	defer func() {
		message, _ := recover().(string)
		if !strings.Contains(message, "still holds live bucket 100") {
			t.Fatalf("recovered %q, want the live-slot panic", message)
		}
	}()
	c.window.record(100+buckets*c.precision, exactHash(testKeys[0]))
	t.Fatal("record into a slot live for another bucket did not panic")
}
