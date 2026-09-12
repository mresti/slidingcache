// Package guard wraps a slidingcache with an ingest-side clock-skew filter.
//
// The cache tracks a single global high-water mark: the most recent timestamp
// it has ever seen. One event with a future timestamp drags that mark forward
// for every key, and the cache then silently discards real-time ingest with -1
// until wall time catches up — 2h30m of blackout for a +3h jump, with the whole
// window wiped by the first sweep (measured in bench/RESULTS.md). The guard
// rejects those events before the cache ever sees them:
//
//	epoch > now + MaxFutureSkew → FutureRejected (-2), not stored
//	epoch < now - MaxPastSkew   → LateRejected (-1), not stored
//	otherwise                   → delegated to the cache unchanged
//
// The filter is opt-in: with Config.Enabled unset, New returns the wrapped
// cache itself and the guard costs nothing. When enabled, both Store and Get
// check the epoch against the guard's clock first; a rejected call never
// reaches the cache, so the high-water mark cannot be poisoned.
//
// The guard does not own the cache: it adds no goroutine and never closes it.
package guard

import (
	"errors"
	"fmt"
	"time"

	"github.com/mresti/slidingcache"
)

// DefaultMaxSkew is the tolerance applied on each side of the clock when
// Enabled is set but the corresponding Config field is left at zero.
const DefaultMaxSkew = 120 * time.Second

// Sentinel returns of the guard. LateRejected mirrors the cache's own -1.
const (
	// FutureRejected is returned by Store and Get when the epoch is further
	// than MaxFutureSkew ahead of the guard's clock. The event is discarded
	// without touching the cache, so it cannot drag the high-water mark
	// forward.
	FutureRejected = -2
	// LateRejected is returned when the epoch is further than MaxPastSkew
	// behind the guard's clock. The event is discarded even when the cache's
	// window would still accept it: the guard is the stricter boundary.
	LateRejected = -1
)

// Config configures a Guard.
type Config struct {
	// Enabled opts in to the skew filter. With the zero value, New returns
	// the cache unchanged: no wrapper, no clock reads, no overhead.
	Enabled bool
	// MaxFutureSkew bounds how far ahead of the guard's clock an epoch may
	// land. Zero selects DefaultMaxSkew; negative values are rejected by New.
	MaxFutureSkew time.Duration
	// MaxPastSkew bounds how far behind the guard's clock an epoch may land.
	// Zero selects DefaultMaxSkew; negative values are rejected by New.
	MaxPastSkew time.Duration
	// EpochUnit is the unit of the epochs passed to Store and Get and of the
	// value returned by Now. It converts the skews into that unit. Nil
	// selects EpochInNanos, matching the default Now and time.UnixNano.
	EpochUnit *slidingcache.EpochUnit
	// Now returns the current time in EpochUnit units, the same unit as the
	// epochs passed to Store and Get. Nil selects time.Now().UnixNano, which
	// matches the default EpochUnit. Supply it to control the clock in tests
	// or to run the guard on a cache configured with another EpochUnit.
	Now func() int64
}

func (c Config) validate() error {
	if c.MaxFutureSkew < 0 {
		return fmt.Errorf("guard: MaxFutureSkew must be >= 0, got %s", c.MaxFutureSkew)
	}
	if c.MaxPastSkew < 0 {
		return fmt.Errorf("guard: MaxPastSkew must be >= 0, got %s", c.MaxPastSkew)
	}
	if u := c.epochUnit(); u < slidingcache.EpochInMillis || u > slidingcache.EpochInSeconds {
		return fmt.Errorf("guard: invalid EpochUnit %d", u)
	}
	return nil
}

func (c Config) epochUnit() slidingcache.EpochUnit {
	if c.EpochUnit == nil {
		return slidingcache.EpochInNanos
	}
	return *c.EpochUnit
}

func (c Config) maxFuture() time.Duration {
	if c.MaxFutureSkew == 0 {
		return DefaultMaxSkew
	}
	return c.MaxFutureSkew
}

func (c Config) maxPast() time.Duration {
	if c.MaxPastSkew == 0 {
		return DefaultMaxSkew
	}
	return c.MaxPastSkew
}

// Guard is a skew-filtered view of a SlidingCache. Create it with New.
type Guard struct {
	cache slidingcache.SlidingCache
	now   func() int64
	// future and past are the skews pre-converted into the epoch unit, so the
	// hot path is two integer comparisons against one clock read.
	future int64
	past   int64
}

var _ slidingcache.SlidingCache = (*Guard)(nil)

// New returns a skew-filtered cache. When cfg.Enabled is unset it returns the
// cache itself unchanged; otherwise it returns a *Guard that rejects epochs
// outside the skews before delegating. The cache remains owned by the caller,
// including its Close.
func New(cache slidingcache.SlidingCache, cfg Config) (slidingcache.SlidingCache, error) {
	if cache == nil {
		return nil, errors.New("guard: New requires a non-nil cache")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return cache, nil
	}
	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixNano() }
	}
	unit := cfg.epochUnit()
	return &Guard{
		cache:  cache,
		now:    now,
		future: int64(cfg.maxFuture()) / nanosPerUnit(unit),
		past:   int64(cfg.maxPast()) / nanosPerUnit(unit),
	}, nil
}

func nanosPerUnit(u slidingcache.EpochUnit) int64 {
	switch u {
	case slidingcache.EpochInNanos:
		return 1
	case slidingcache.EpochInSeconds:
		return int64(time.Second)
	default:
		return int64(time.Millisecond)
	}
}

// Store records an event for key at epoch, or rejects it with FutureRejected
// or LateRejected when the epoch falls outside the skews. Rejected calls
// return the sentinel without consulting the cache.
func (g *Guard) Store(epoch int64, key string) int {
	now := g.now()
	if epoch > now+g.future {
		return FutureRejected
	}
	if epoch < now-g.past {
		return LateRejected
	}
	return g.cache.Store(epoch, key)
}

// Get returns the live count for key within the window covering epoch, or
// FutureRejected or LateRejected when the epoch falls outside the skews.
func (g *Guard) Get(epoch int64, key string) int {
	now := g.now()
	if epoch > now+g.future {
		return FutureRejected
	}
	if epoch < now-g.past {
		return LateRejected
	}
	return g.cache.Get(epoch, key)
}
