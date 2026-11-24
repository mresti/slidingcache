package countminsketch

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

var _ slidingcache.SlidingCache = (*Cache)(nil)

// Cache is an approximate, concurrency-safe sliding-window event counter. It
// implements slidingcache.SlidingCache; see the package documentation for how
// its estimates relate to the exact counts. A Cache must be created with New.
type Cache struct {
	precision  int64
	windowSize int64
	// bucketDiv is the bucket width expressed in epoch units, precomputed so
	// that mapping an epoch onto its bucket costs a single division.
	bucketDiv int64
	// minTimestamp and maxTimestamp bound the bucket timestamps Store and Get
	// accept, as Config.TimestampBits sets them.
	minTimestamp int64
	maxTimestamp int64

	// maxFutureSkew is Config.MaxFutureSkew in seconds, and zero when the future
	// guard is disabled. clock is nil exactly when maxFutureSkew is zero.
	maxFutureSkew int64
	clock         func() int64

	// hash is nil for the default maphash.String with seed, so the common case
	// pays no indirect call.
	hash slidingcache.HashFunc
	seed maphash.Seed

	highWater atomic.Int64
	rejects   rejectCounters

	mu       sync.Mutex
	counters lockedCounters
	window   *window
}

// rejectCounters count the outcomes decided before the lock is taken, so they
// are atomic.
type rejectCounters struct {
	outOfRange atomic.Uint64
	late       atomic.Uint64
	future     atomic.Uint64
	getLate    atomic.Uint64
	getFuture  atomic.Uint64
}

// lockedCounters count the outcomes decided under the lock, so plain adds
// suffice: the lock is already held and Stats takes it too.
type lockedCounters struct {
	accepted  uint64
	late      uint64
	saturated uint64
	getHit    uint64
	getMiss   uint64
}

// Option customizes a Cache at construction time. Options run inside New after
// cfg is validated; there is no post-construction mutation.
type Option func(*Cache) error

// WithHashFunc replaces the default key hash, maphash.String with a seed drawn
// per Cache. Passing a nil fn makes New return an error.
//
// Unlike in the root package, where the hash only routes keys to shards, here
// its quality decides correctness: two keys with the same 64-bit hash are
// counted as one key, and a weak hash, one whose bits are not well mixed,
// spreads keys over fewer counters and inflates every estimate. The function
// must be deterministic and must not change for the lifetime of the Cache.
func WithHashFunc(fn slidingcache.HashFunc) Option {
	return func(c *Cache) error {
		if fn == nil {
			return errors.New("countminsketch: WithHashFunc requires a non-nil HashFunc")
		}
		c.hash = fn
		return nil
	}
}

// New validates cfg, sizes the sketches, applies opts, and returns a ready Cache. Its memory is
// fixed here: one aggregate and a ring of ceil(WindowSize/Precision) slots, each
// of which allocates its sketch the first time an event lands in it. Any option
// that returns an error aborts construction.
func New(cfg Config, opts ...Option) (*Cache, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	agg, err := newAggregate(cfg)
	if err != nil {
		return nil, err
	}
	c := newCache(cfg, agg)
	if err := applyOptions(c, opts); err != nil {
		return nil, err
	}
	return c, nil
}

// newAggregate validates Epsilon and Delta and builds the window aggregate.
// Dimensions checks the memory of a sketch; the aggregate's cells are twice as
// wide, so on a 32-bit platform NewAggregate can still refuse sizes Dimensions
// accepted.
func newAggregate(cfg Config) (*countmin.Aggregate, error) {
	width, depth, err := cfg.dimensions()
	if err != nil {
		return nil, err
	}
	agg, err := countmin.NewAggregate(width, depth)
	if err != nil {
		return nil, fmt.Errorf("countminsketch: %w", err)
	}
	return agg, nil
}

func newCache(cfg Config, agg *countmin.Aggregate) *Cache {
	precision := int64(cfg.Precision / time.Second)
	windowSize := int64(cfg.WindowSize / time.Second)
	bits := uint(cfg.timestampBits())
	c := &Cache{
		precision:     precision,
		windowSize:    windowSize,
		bucketDiv:     bucketDivisor(cfg.Precision, cfg.EpochUnit),
		minTimestamp:  -1 << bits,
		maxTimestamp:  1<<bits - 1,
		maxFutureSkew: int64(cfg.MaxFutureSkew / time.Second),
		clock:         cfg.futureClock(),
		seed:          maphash.MakeSeed(),
	}
	unobserved := noObservedHighWater(windowSize)
	c.highWater.Store(unobserved)
	c.window = newWindow(cfg.bucketCount(), precision, agg, cutoffFor(unobserved, windowSize))
	return c
}

func applyOptions(c *Cache, opts []Option) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}
	return nil
}

// noObservedHighWater is the high-water mark of a Cache on which Store has never
// been called. It is the smallest mark whose cutoff is representable, so a fresh
// cache treats every usable timestamp as alive while keeping cutoffFor a plain
// subtraction.
func noObservedHighWater(windowSize int64) int64 {
	return math.MinInt64 + windowSize
}

// Store records an event for keyInHash at the window containing epoch and
// returns the resulting estimate for the key, at least 1. The checks run in the
// order of the root package's Store, so the same events are rejected the same
// way:
//
//   - an epoch whose bucket timestamp is outside the range Config.TimestampBits
//     sets returns slidingcache.LateEvent without touching the high-water mark;
//   - with Config.MaxFutureSkew set, a bucket timestamp further than the skew
//     ahead of Config.Clock returns slidingcache.FutureEvent, again without
//     touching the mark;
//   - an event already expired relative to the mark returns
//     slidingcache.LateEvent and stores nothing.
func (c *Cache) Store(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !c.inRange(timestamp) {
		c.rejects.outOfRange.Add(1)
		return slidingcache.LateEvent
	}
	// Only an event beyond the mark can advance it, so the future guard, and
	// with it the clock, stays out of the steady-state path.
	highWater := c.highWater.Load()
	if timestamp > highWater {
		if c.maxFutureSkew > 0 && c.isFuture(timestamp) {
			c.rejects.future.Add(1)
			return slidingcache.FutureEvent
		}
		highWater = c.advanceHighWater(timestamp, highWater)
	}
	if timestamp <= cutoffFor(highWater, c.windowSize) {
		c.rejects.late.Add(1)
		return slidingcache.LateEvent
	}
	return c.storeLocked(timestamp, c.hashOf(keyInHash))
}

// storeLocked re-derives the cutoff under the lock, where it cannot be stale:
// a concurrent Store may have advanced the high-water mark since the fast
// reject in Store, turning an apparently live timestamp into an expired one.
func (c *Cache) storeLocked(timestamp int64, h uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := cutoffFor(c.highWater.Load(), c.windowSize)
	if timestamp <= cutoff {
		c.counters.late++
		return slidingcache.LateEvent
	}
	c.window.expireTo(cutoff)
	estimate, saturated := c.window.record(timestamp, h)
	c.counters.accepted++
	if saturated {
		c.counters.saturated++
	}
	return clampToInt(estimate)
}

// Get returns the estimated live count for keyInHash within the sliding window
// covering epoch, 0 when no event in the window shares the key's counters. It
// rejects epochs as Store does, with slidingcache.LateEvent for one outside the
// range or the live window and slidingcache.FutureEvent for one too far ahead of
// the clock. Get reads the high-water mark but never advances it.
func (c *Cache) Get(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !c.inRange(timestamp) {
		c.rejects.getLate.Add(1)
		return slidingcache.LateEvent
	}
	highWater := c.highWater.Load()
	if timestamp > highWater && c.maxFutureSkew > 0 && c.isFuture(timestamp) {
		c.rejects.getFuture.Add(1)
		return slidingcache.FutureEvent
	}
	cutoff := cutoffFor(highWater, c.windowSize)
	if timestamp <= cutoff {
		c.rejects.getLate.Add(1)
		return slidingcache.LateEvent
	}
	return c.countLocked(cutoff, c.hashOf(keyInHash))
}

// countLocked expires the window to the cutoff Get computed before the lock,
// as the root package counts against it; a newer state already applied by a
// concurrent Store is kept.
func (c *Cache) countLocked(cutoff int64, h uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.window.expireTo(cutoff)
	estimate := c.window.estimate(h)
	if estimate > 0 {
		c.counters.getHit++
	} else {
		c.counters.getMiss++
	}
	return clampToInt(estimate)
}

// HighWater reports the high-water mark, in seconds of bucket timestamp, and
// whether a Store has ever advanced it. Until then the mark is a sentinel below
// every usable epoch and ok is false; the value returned alongside it is
// meaningless. As in the root package, the mark is always seconds truncated to
// Precision, whatever Config.EpochUnit is, and a Store advances it before the
// window check, so ok can be true even when that Store was rejected as late.
func (c *Cache) HighWater() (hw int64, ok bool) {
	hw = c.highWater.Load()
	return hw, hw != noObservedHighWater(c.windowSize)
}

// Stats is a point-in-time snapshot of the counters a Cache keeps for every
// outcome Store and Get can have. Every counter is monotonic for the life of
// the Cache, so consumers take deltas between two calls to get rates.
//
// Accepted + Late + Future + OutOfRange is the number of Store calls that had
// completed when the snapshot was taken, and GetHit + GetMiss + GetLate +
// GetFuture the number of Get calls: every call increments exactly one of them.
// Saturated is extra, a subset of Accepted.
type Stats struct {
	// Accepted counts the Store calls that recorded an event.
	Accepted uint64
	// Late counts the Store calls rejected as late, with a bucket timestamp at or
	// below HW - WindowSize. They returned -1 and stored nothing.
	Late uint64
	// Future counts the Store calls rejected as too far ahead of the clock. They
	// returned -2 and left the high-water mark untouched.
	Future uint64
	// OutOfRange counts the Store calls rejected because the bucket timestamp is
	// outside the range Config.TimestampBits sets. They returned -1 and left the
	// high-water mark untouched.
	OutOfRange uint64
	// GetHit counts the Get calls whose estimate was above 0. Collisions make
	// it count some absent keys too.
	GetHit uint64
	// GetMiss counts the Get calls whose estimate was 0: no event in the window
	// shares the key's counters, so the key certainly has no live event.
	GetMiss uint64
	// GetLate counts the Get calls that returned -1, because the queried epoch
	// is outside the live window or outside the accepted range.
	GetLate uint64
	// GetFuture counts the Get calls rejected as too far ahead of the clock.
	GetFuture uint64
	// Saturated counts the accepted Store calls that met a counter already at
	// 2^32-1 events in its bucket, which therefore did not grow. A non-zero
	// value means estimates for the hottest keys may be low.
	Saturated uint64
}

// Stats returns a snapshot of the cache's counters. It is safe to call
// concurrently with Store and Get; it reads the atomic counters first and then
// takes the lock briefly for the rest, so a call racing with traffic may count
// it in one group and not in the other. It suits metrics, not synchronization.
func (c *Cache) Stats() Stats {
	stats := Stats{
		OutOfRange: c.rejects.outOfRange.Load(),
		Late:       c.rejects.late.Load(),
		Future:     c.rejects.future.Load(),
		GetLate:    c.rejects.getLate.Load(),
		GetFuture:  c.rejects.getFuture.Load(),
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stats.Accepted = c.counters.accepted
	stats.Late += c.counters.late
	stats.Saturated = c.counters.saturated
	stats.GetHit = c.counters.getHit
	stats.GetMiss = c.counters.getMiss
	return stats
}

// Close releases nothing, because a Cache owns no goroutine: expiry happens
// inside Store and Get. It exists for parity with the root package, is
// idempotent, and always returns nil.
func (*Cache) Close() error {
	return nil
}

func (c *Cache) inRange(timestamp int64) bool {
	return timestamp >= c.minTimestamp && timestamp <= c.maxTimestamp
}

func (c *Cache) hashOf(key string) uint64 {
	if c.hash == nil {
		return maphash.String(c.seed, key)
	}
	return c.hash(key)
}

// clampToInt converts an estimate to the int the interface returns. The
// aggregate's cells are 64-bit, so on a 32-bit platform, or after 2^63 events,
// the estimate can exceed int; it saturates rather than wrapping negative,
// which would read as a sentinel.
func clampToInt(estimate uint64) int {
	if estimate > math.MaxInt {
		return math.MaxInt
	}
	return int(estimate)
}
