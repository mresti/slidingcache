package countminsketch

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
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

	// highWater is the one mark of the whole cache, shared by every shard, as in
	// the root package.
	highWater atomic.Int64

	shards []*shard
	// shardBits is log2(len(shards)), for countmin.ShardOf.
	shardBits uint
}

// Option customizes a Cache at construction time. Options run inside New after
// cfg is validated; there is no post-construction mutation.
type Option func(*Cache) error

// WithHashFunc replaces the default key hash, maphash.String with a seed drawn
// per Cache. Passing a nil fn makes New return an error.
//
// Unlike in the root package, where the hash only routes keys to shards, here
// the same hash picks both the shard and the columns, and its quality decides
// correctness: two keys with the same 64-bit hash are
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

// New validates cfg, sizes the window, applies opts, and returns a ready Cache.
//
// Epsilon and Delta size one sketch, whose columns are divided among the
// shards. Each shard holds a ring of ceil(WindowSize/Precision) slot sketches of
// 32-bit cells, their aggregate of 64-bit cells, and one 8-byte timestamp per
// slot: about Shards x (buckets x columns x rows x 4 + buckets x 8 + columns x
// rows x 8) bytes, with columns the shard's share. New estimates that before
// allocating anything and returns an error when it exceeds
// Config.MaxMemoryBytes; otherwise it allocates all of it here, and nothing the
// window does afterwards allocates or grows. Any option that returns an error
// aborts construction.
func New(cfg Config, opts ...Option) (*Cache, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	layout, err := cfg.layout()
	if err != nil {
		return nil, err
	}
	if err := layout.checkMemory(cfg.maxMemoryBytes()); err != nil {
		return nil, err
	}
	c, err := newCache(cfg, layout)
	if err != nil {
		return nil, err
	}
	if err := applyOptions(c, opts); err != nil {
		return nil, err
	}
	return c, nil
}

func newCache(cfg Config, layout windowLayout) (*Cache, error) {
	precision := int64(cfg.Precision / time.Second)
	windowSize := int64(cfg.WindowSize / time.Second)
	timestampBits := uint(cfg.timestampBits())
	c := &Cache{
		precision:     precision,
		windowSize:    windowSize,
		bucketDiv:     bucketDivisor(cfg.Precision, cfg.EpochUnit),
		minTimestamp:  -1 << timestampBits,
		maxTimestamp:  1<<timestampBits - 1,
		maxFutureSkew: int64(cfg.MaxFutureSkew / time.Second),
		clock:         cfg.futureClock(),
		seed:          maphash.MakeSeed(),
	}
	unobserved := noObservedHighWater(windowSize)
	c.highWater.Store(unobserved)
	shards, err := newShards(layout, precision, cutoffFor(unobserved, windowSize))
	if err != nil {
		return nil, fmt.Errorf("countminsketch: %w", err)
	}
	c.shards = shards
	c.shardBits = uint(bits.TrailingZeros(uint(layout.shards)))
	return c, nil
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
// way, and each is counted on the key's shard:
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
		c.shardOf(keyInHash).rejects.outOfRange.Add(1)
		return slidingcache.LateEvent
	}
	// Only an event beyond the mark can advance it, so the future guard, and
	// with it the clock, stays out of the steady-state path.
	highWater := c.highWater.Load()
	if timestamp > highWater {
		if c.maxFutureSkew > 0 && c.isFuture(timestamp) {
			c.shardOf(keyInHash).rejects.future.Add(1)
			return slidingcache.FutureEvent
		}
		highWater = c.advanceHighWater(timestamp, highWater)
	}
	if timestamp <= cutoffFor(highWater, c.windowSize) {
		c.shardOf(keyInHash).rejects.late.Add(1)
		return slidingcache.LateEvent
	}
	// The pre-lock check above is only a fast reject; the shard re-derives the
	// cutoff from the global mark under its lock.
	h := c.hashOf(keyInHash)
	return c.shardFor(h).store(timestamp, h, &c.highWater, c.windowSize)
}

// Get returns the estimated live count for keyInHash within the sliding window
// covering epoch, 0 when no event in the window shares the key's counters. It
// rejects epochs as Store does, with slidingcache.LateEvent for one outside the
// range or the live window and slidingcache.FutureEvent for one too far ahead of
// the clock. Get reads the high-water mark but never advances it.
func (c *Cache) Get(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !c.inRange(timestamp) {
		c.shardOf(keyInHash).rejects.getLate.Add(1)
		return slidingcache.LateEvent
	}
	highWater := c.highWater.Load()
	if timestamp > highWater && c.maxFutureSkew > 0 && c.isFuture(timestamp) {
		c.shardOf(keyInHash).rejects.getFuture.Add(1)
		return slidingcache.FutureEvent
	}
	cutoff := cutoffFor(highWater, c.windowSize)
	if timestamp <= cutoff {
		c.shardOf(keyInHash).rejects.getLate.Add(1)
		return slidingcache.LateEvent
	}
	h := c.hashOf(keyInHash)
	return c.shardFor(h).count(cutoff, h)
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
	// Saturated counts the accepted Store calls whose smallest counter in the
	// bucket was already at 2^32-1 events, so that no counter grew. A non-zero
	// value means estimates for the hottest keys may be low.
	Saturated uint64
}

// Stats returns a snapshot of the cache's counters, summed over the shards. It
// is safe to call concurrently with Store and Get. It visits the shards in turn,
// reading each one's atomic counters and then taking its lock briefly for the
// rest, so the snapshot is not atomic: a call racing with traffic may be
// counted in one group and not in the other, or in a shard visited late and
// not in one visited early. It suits metrics, not synchronization.
func (c *Cache) Stats() Stats {
	var stats Stats
	for _, s := range c.shards {
		s.addTo(&stats)
	}
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

// shardOf hashes key to find its shard, for the reject paths, which need the
// shard and not the hash.
func (c *Cache) shardOf(key string) *shard {
	return c.shardFor(c.hashOf(key))
}

// shardFor selects the shard of a hash. ShardOf has its own salt, so the shard
// says nothing about the columns the same hash takes within it.
func (c *Cache) shardFor(h uint64) *shard {
	return c.shards[countmin.ShardOf(h, c.shardBits)]
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
