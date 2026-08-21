// Package slidingcache provides a sharded, concurrency-safe sliding-window
// event counter.
//
// The cache records events per key and reports, for any given moment, how many
// of a key's events still fall inside a fixed-length sliding window. It is
// designed for high write throughput, out-of-order tolerance, and a small,
// bounded memory footprint even under churn of many short-lived keys.
//
// # Window semantics
//
// All timestamps are converted to seconds and truncated to Precision. The cache
// keeps a single global high-water mark (HW), defined as the maximum epoch (in
// seconds) ever observed by Store. An event with truncated timestamp t is alive
// if and only if:
//
//	t > HW - WindowSize
//
// Because HW is always greater than or equal to the epoch E of any individual
// call (Store advances HW to at least E before evaluating liveness), this rule
// is at least as inclusive as the intuitive "t >= E - WindowSize" window
// anchored at the call itself. The count for a key is the number of its events
// with a live timestamp.
//
// Events that share a truncated timestamp are counted individually but stored
// once, as a bucket holding their number, so a key's memory is bounded by
// WindowSize/Precision buckets regardless of its event rate. A bucket is a
// single 8-byte word packing the timestamp and the count.
//
// # Out-of-order and late events
//
// Events may arrive in any order. An older-but-still-live event stored after a
// newer one is inserted in timestamp order and counted normally. An event that
// is already expired relative to the current high-water mark (t <= HW -
// WindowSize) is rejected: it is not stored, the key's existing events are left
// untouched, and Store returns -1.
//
// # High-water mark hygiene
//
// The high-water mark only ever moves forward, and it is not clamped, so an
// epoch in the wrong unit can drag the window forward with it. Store and Get
// reject outright, with -1 and without touching HW, any epoch whose bucket
// timestamp falls outside +-2^43 seconds (about +-278,000 years), which is the
// range a bucket word represents. That absorbs the grossest unit mistake:
// nanoseconds passed to a cache configured for seconds land near 1.7e18
// seconds and are rejected rather than bricking the cache.
//
// A wrong unit that still lands inside the range is not detectable and remains
// destructive: milliseconds passed to a cache configured for seconds advance HW
// by a factor of a thousand, to the year 55,000, and make every subsequent real
// event look late, permanently. There is no reset; the only recovery is to build
// a new Cache with New. Callers must therefore feed epochs of a single,
// consistent unit matching Config.EpochUnit.
//
// Negative epochs are supported: truncation to a Precision bucket rounds toward
// negative infinity, so bucket boundaries are uniform on both sides of zero. A
// Cache that has never seen a Store starts with its high-water mark below every
// usable epoch, so the first event is always accepted regardless of its sign.
//
// # Naming
//
// A more idiomatic Go naming would be an interface named SlidingWindowCounter
// with methods Add(epoch, key) int and Count(epoch, key) int. The SlidingCache
// interface with Store/Get is kept here for compatibility.
package slidingcache

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// SlidingCache records events per key and reports live counts within a sliding
// window. See the package documentation for the exact window semantics.
type SlidingCache interface {
	// Store records an event for keyInHash at the window containing epoch.
	// If the key already has live events, increments and returns the new count.
	// Otherwise stores 1 and returns 1.
	//
	// If the event is already expired relative to the high-water mark
	// (t <= HW - WindowSize after conversion and truncation), it is not stored
	// and Store returns -1 to signal "late event, not stored". Store also
	// returns -1, without advancing the high-water mark, for an epoch whose
	// bucket timestamp falls outside +-2^43 seconds (about +-278,000 years).
	Store(epoch int64, keyInHash string) int
	// Get returns the live count for keyInHash within the sliding window
	// covering epoch. Returns 0 if the key does not exist.
	//
	// If epoch itself falls outside the live window (t <= HW - WindowSize after
	// conversion and truncation), or its bucket timestamp falls outside +-2^43
	// seconds, Get returns -1 to signal "queried epoch is outside the live
	// window".
	Get(epoch int64, keyInHash string) int
}

var _ SlidingCache = (*Cache)(nil)

// EpochUnit identifies the unit of the epoch arguments passed to Store and Get.
type EpochUnit int

const (
	// EpochInMillis treats epoch arguments as milliseconds since an arbitrary
	// base. It is the zero value, so a Config that omits EpochUnit uses
	// milliseconds, matching time.Now().UnixMilli().
	EpochInMillis EpochUnit = iota
	// EpochInNanos treats epoch arguments as nanoseconds since an arbitrary base.
	EpochInNanos
	// EpochInSeconds treats epoch arguments as seconds since an arbitrary base.
	EpochInSeconds
)

func (u EpochUnit) valid() bool {
	return u >= EpochInMillis && u <= EpochInSeconds
}

// Default configuration values applied when the corresponding Config field is
// left at its zero value.
const (
	defaultShards      = 16
	maxShards          = 1 << 20
	mapCompactMinPeak  = 1024
	entryReallocMinCap = 64
)

// Config configures a Cache. Precision and WindowSize are durations that must be
// whole, positive multiples of one second.
type Config struct {
	// Precision is the bucket granularity to which timestamps are truncated when
	// stored. Must be >= 1s and an exact multiple of time.Second.
	Precision time.Duration
	// WindowSize is the total sliding-window length. Must be >= Precision and an
	// exact multiple of time.Second.
	WindowSize time.Duration
	// EpochUnit is the unit of the epoch arguments passed to Store and Get. Its
	// zero value is EpochInMillis, so a Config that omits this field interprets
	// epochs as milliseconds (time.Now().UnixMilli()).
	EpochUnit EpochUnit
	// Shards is the number of internal shards. It is rounded up to a power of
	// two and must not exceed 1<<20. When zero, a sensible default is used.
	Shards int
	// SweepInterval is the period of the background janitor that prunes expired
	// events and compacts shards. It may be sub-second (useful in tests). When
	// zero, it defaults to WindowSize.
	SweepInterval time.Duration
}

func (c Config) validate() error {
	if err := validateWholeSeconds("Precision", c.Precision); err != nil {
		return err
	}
	if err := validateWholeSeconds("WindowSize", c.WindowSize); err != nil {
		return err
	}
	if c.WindowSize < c.Precision {
		return fmt.Errorf("slidingcache: WindowSize (%s) must be >= Precision (%s)", c.WindowSize, c.Precision)
	}
	if !c.EpochUnit.valid() {
		return fmt.Errorf("slidingcache: invalid EpochUnit %d", c.EpochUnit)
	}
	if c.Shards < 0 || c.Shards > maxShards {
		return fmt.Errorf("slidingcache: Shards must be between 0 and %d, got %d", maxShards, c.Shards)
	}
	if c.SweepInterval < 0 {
		return fmt.Errorf("slidingcache: SweepInterval must be >= 0, got %s", c.SweepInterval)
	}
	return nil
}

// validateWholeSeconds rejects durations that are not positive, whole-second
// values so that timestamps map cleanly onto the int64-seconds internal clock
// without silent rounding.
func validateWholeSeconds(name string, d time.Duration) error {
	if d < time.Second {
		return fmt.Errorf("slidingcache: %s must be >= 1s, got %s", name, d)
	}
	if d%time.Second != 0 {
		return fmt.Errorf("slidingcache: %s must be a whole multiple of 1s, got %s", name, d)
	}
	return nil
}

func (c Config) shardCount() int {
	if c.Shards <= 0 {
		return defaultShards
	}
	return c.Shards
}

func (c Config) sweepInterval() time.Duration {
	if c.SweepInterval <= 0 {
		return c.WindowSize
	}
	return c.SweepInterval
}

// Cache is a sharded, concurrency-safe sliding-window event counter. It
// implements SlidingCache and additionally exposes Close to stop its background
// janitor. A Cache must be created with New and released with Close.
type Cache struct {
	precision  int64
	windowSize int64
	// bucketDiv is the bucket width expressed in epoch units, precomputed so
	// that mapping an epoch onto its bucket costs a single division.
	bucketDiv int64
	unit      EpochUnit

	shards    []*shard
	shardMask uint64

	highWater atomic.Int64

	hash HashFunc

	sweepEvery time.Duration
	done       chan struct{}
	closeOnce  sync.Once
	janitor    sync.WaitGroup
}

// HashFunc maps a key to a 64-bit hash used to assign the key to a shard. It
// must be deterministic for the lifetime of the Cache: the hash quality
// determines how evenly keys spread across shards, and changing the mapping
// after keys are stored would misroute lookups to the wrong shard. The default
// is FNV-1a; override it with WithHashFunc.
type HashFunc func(key string) uint64

// Option customizes a Cache at construction time. Options run inside New after
// cfg is validated; there is no post-construction mutation.
type Option func(*Cache) error

// WithHashFunc overrides the default FNV-1a key hash. Passing a nil fn makes New
// return an error. The supplied function must be deterministic and must not be
// changed for the lifetime of the Cache, since altering key-to-shard routing
// after events are stored would misroute later operations.
func WithHashFunc(fn HashFunc) Option {
	return func(c *Cache) error {
		if fn == nil {
			return errors.New("slidingcache: WithHashFunc requires a non-nil HashFunc")
		}
		c.hash = fn
		return nil
	}
}

// New validates cfg, applies opts, and returns a running Cache. The returned
// Cache owns a background janitor goroutine that must be released with Close.
// Any option that returns an error aborts construction.
func New(cfg Config, opts ...Option) (*Cache, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	shardCount := roundUpPow2(cfg.shardCount())
	c := &Cache{
		precision:  int64(cfg.Precision / time.Second),
		windowSize: int64(cfg.WindowSize / time.Second),
		bucketDiv:  bucketDivisor(cfg.Precision, cfg.EpochUnit),
		unit:       cfg.EpochUnit,
		shards:     newShards(shardCount),
		shardMask:  shardMaskOf(shardCount),
		sweepEvery: cfg.sweepInterval(),
		done:       make(chan struct{}),
	}
	c.highWater.Store(noObservedHighWater(c.windowSize))
	if err := applyOptions(c, opts); err != nil {
		return nil, err
	}
	c.startJanitor()
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

// lateEvent is the sentinel returned when a timestamp falls outside the live
// window (t <= HW - WindowSize).
const lateEvent = -1

// inBucketRange reports whether a bucket timestamp fits in the high bits of a
// bucket word. Timestamps outside the range are rejected at the API boundary:
// packing one would overflow into the sign bit, and letting it through would
// also move the high-water mark to a value no real event can reach.
func inBucketRange(timestamp int64) bool {
	return timestamp >= minBucketTimestamp && timestamp <= maxBucketTimestamp
}

// noObservedHighWater is the high-water mark of a Cache on which Store has never
// been called. It is the smallest mark whose cutoff is representable, so a fresh
// cache treats every usable timestamp as alive while keeping the invariant that
// makes cutoffFor a plain subtraction.
func noObservedHighWater(windowSize int64) int64 {
	return math.MinInt64 + windowSize
}

// Store records an event for keyInHash at the window containing epoch and
// returns the resulting live count for the key. An event that is already
// expired relative to the current high-water mark is not stored; Store then
// returns -1 to signal "late event, not stored".
//
// An epoch whose bucket timestamp is not representable (beyond about +-278,000
// years from the epoch) is rejected the same way, before the high-water mark is
// consulted, so it cannot drag the window with it.
func (c *Cache) Store(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !inBucketRange(timestamp) {
		return lateEvent
	}
	if timestamp <= cutoffFor(c.advanceHighWater(timestamp), c.windowSize) {
		return lateEvent
	}
	// The pre-lock check above is only a fast reject; the shard re-derives the
	// cutoff under its lock, where it cannot be stale.
	return c.shardFor(keyInHash).store(keyInHash, timestamp, &c.highWater, c.windowSize)
}

// Get returns the live count for keyInHash within the sliding window covering
// epoch, or 0 if the key does not exist. If epoch itself falls outside the live
// window (t <= HW - WindowSize), or its bucket timestamp is not representable,
// Get returns -1. Get reads the current high-water mark but does not advance it.
func (c *Cache) Get(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !inBucketRange(timestamp) {
		return lateEvent
	}
	cutoff := c.cutoff()
	if timestamp <= cutoff {
		return lateEvent
	}
	return c.shardFor(keyInHash).count(keyInHash, cutoff)
}

// Close stops the background janitor. It is idempotent and safe to call
// concurrently. Close always returns nil.
func (c *Cache) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.janitor.Wait()
	})
	return nil
}

// bucket maps an epoch onto the first second of the Precision bucket containing
// it. Converting to seconds and then truncating is two floor divisions, which
// pushes the function past the inliner's budget on the Store and Get hot paths;
// dividing once by the precomputed bucket width is equivalent, because
// floor(floor(x/a)/b) equals floor(x/(a*b)) for positive a and b.
func (c *Cache) bucket(epoch int64) int64 {
	return floorDiv(epoch, c.bucketDiv) * c.precision
}

// bucketDivisor expresses precision in the given epoch unit. Precision is
// validated to be a whole number of seconds, so it divides exactly by the
// nanoseconds spanned by any supported unit and the result cannot overflow: it
// is at most the precision itself, in nanoseconds.
func bucketDivisor(precision time.Duration, unit EpochUnit) int64 {
	return int64(precision) / nanosPerUnit(unit)
}

func nanosPerUnit(unit EpochUnit) int64 {
	switch unit {
	case EpochInNanos:
		return 1
	case EpochInSeconds:
		return int64(time.Second)
	default:
		return int64(time.Millisecond)
	}
}

// floorDiv divides rounding toward negative infinity, unlike Go's "/", which
// rounds toward zero. Rounding toward zero would make the buckets straddling
// zero twice as wide and would map negative epochs onto later buckets than
// their true position.
func floorDiv(numerator, positiveDivisor int64) int64 {
	quotient := numerator / positiveDivisor
	if numerator%positiveDivisor < 0 {
		quotient--
	}
	return quotient
}

// cutoff returns the current expiry boundary: a timestamp is alive if and only
// if it is strictly greater than the returned value.
func (c *Cache) cutoff() int64 {
	return cutoffFor(c.highWater.Load(), c.windowSize)
}

// cutoffFor derives the expiry boundary from a high-water mark. The subtraction
// needs no underflow guard: the mark starts at noObservedHighWater and only ever
// rises, so it is never below math.MinInt64 + windowSize.
func cutoffFor(highWater, windowSize int64) int64 {
	return highWater - windowSize
}

// advanceHighWater raises the global high-water mark to at least timestamp using
// a compare-and-swap loop and returns the resulting high-water mark.
func (c *Cache) advanceHighWater(timestamp int64) int64 {
	for {
		current := c.highWater.Load()
		if timestamp <= current {
			return current
		}
		if c.highWater.CompareAndSwap(current, timestamp) {
			return timestamp
		}
	}
}

// shardFor selects the shard for key. The default FNV-1a path is kept as an
// inlinable direct call; a custom hash is only invoked when one was installed
// via WithHashFunc, so the common case pays no indirect-call cost.
func (c *Cache) shardFor(key string) *shard {
	if c.hash == nil {
		return c.shards[fnv1a(key)&c.shardMask]
	}
	return c.shards[c.hash(key)&c.shardMask]
}

func (c *Cache) startJanitor() {
	c.janitor.Add(1)
	go c.runJanitor()
}

func (c *Cache) runJanitor() {
	defer c.janitor.Done()
	ticker := time.NewTicker(c.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.sweep()
		}
	}
}

func (c *Cache) sweep() {
	cutoff := c.cutoff()
	for _, s := range c.shards {
		s.sweep(cutoff)
	}
}
