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
// single 8-byte word packing the timestamp and the count; how the word splits
// between the two, and therefore the range of epochs the cache accepts, follows
// from Config.CountBits.
//
// # Out-of-order and late events
//
// Events may arrive in any order. An older-but-still-live event stored after a
// newer one is inserted in timestamp order and counted normally. An event that
// is already expired relative to the current high-water mark (t <= HW -
// WindowSize) is rejected: it is not stored, the key's existing events are left
// untouched, and Store returns -1.
//
// # Future events
//
// A single event dated far ahead of real time used to be enough to expire
// everything: it advanced the high-water mark with it, and every real event
// that followed looked late until the clock caught up. Config.MaxFutureSkew
// closes that hole. With a non-zero skew, an event whose bucket timestamp lies
// more than MaxFutureSkew ahead of Config.Clock is rejected with FutureEvent,
// and the high-water mark is left where it was; Get applies the same rule to
// the epoch it is queried with, and never advances the mark in any case.
//
// The guard is free in steady state: only an event beyond the current mark can
// advance it, so only such an event consults the clock. A continuous in-order
// stream reads the clock about once per Precision bucket, not once per Store.
//
// An event ahead of the clock but inside the skew is accepted and does advance
// the mark, so the skew bounds how far one misdated event can drag the window.
// A clock that steps backwards (NTP) only tightens the guard while it is
// behind: the high-water mark never moves back. Callers whose epochs are not
// Unix-based must inject Config.Clock on their own time base.
//
// # High-water mark hygiene
//
// The high-water mark only ever moves forward, and it is not clamped, so an
// epoch in the wrong unit can drag the window forward with it. Store and Get
// reject outright, with -1 and without touching HW, any epoch whose bucket
// timestamp falls outside +-2^(63-CountBits) seconds (+-2^43 seconds, about
// +-278,000 years, with the default CountBits of 20), which is the range a
// bucket word represents. That absorbs the grossest unit mistake:
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
	// bucket timestamp falls outside the range a bucket word represents:
	// +-2^(63-CountBits) seconds, +-2^43 seconds with the default CountBits.
	//
	// On a Cache configured with Config.MaxFutureSkew, an event further than
	// that skew ahead of Config.Clock is rejected with -2, again without
	// advancing the high-water mark.
	Store(epoch int64, keyInHash string) int
	// Get returns the live count for keyInHash within the sliding window
	// covering epoch. Returns 0 if the key does not exist.
	//
	// If epoch itself falls outside the live window (t <= HW - WindowSize after
	// conversion and truncation), or its bucket timestamp falls outside
	// +-2^(63-CountBits) seconds, Get returns -1 to signal "queried epoch is
	// outside the live window". An epoch further than Config.MaxFutureSkew
	// ahead of Config.Clock yields -2.
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
	// MaxFutureSkew is the furthest a bucket timestamp may lie ahead of Clock()
	// before Store and Get reject it with FutureEvent. Zero disables the check,
	// which is the behavior of every version before v1.3.0. Must be >= 0 and an
	// exact multiple of time.Second.
	//
	// An event that lies ahead of the clock but inside the skew is accepted and
	// does advance the high-water mark, so the skew is also the furthest the
	// window can be dragged forward by a single misdated event. Keep it small: a
	// few minutes covers clock drift between producers without giving away much
	// of the window.
	MaxFutureSkew time.Duration
	// Clock returns the current epoch in EpochUnit. It is consulted only when
	// MaxFutureSkew > 0, and only for an event that would advance the high-water
	// mark, so a steady stream of in-order events calls it about once per
	// Precision bucket rather than once per Store.
	//
	// A nil Clock means the Unix wall clock in EpochUnit
	// (time.Now().UnixNano(), UnixMilli() or Unix()). Callers whose epochs are
	// not Unix-based must inject a Clock on the same base, or the guard would
	// compare two unrelated time lines. Setting Clock without MaxFutureSkew is
	// rejected by New as dead configuration.
	Clock func() int64
	// CountBits is the width, in bits, of the per-event count packed into a
	// bucket word; the remaining 63-CountBits bits hold the bucket timestamp in
	// seconds. Zero selects the default of 20. Any other value must be between 8
	// and 24.
	//
	// The choice trades events per bucket against the range of epochs the cache
	// accepts:
	//
	//	CountBits | events per bucket before spill | timestamp bits | Unix-seconds epochs usable until
	//	----------+-------------------------------+----------------+---------------------------------
	//	8         | 255                           | 55             | year ~1.1e9
	//	12        | 4,095                         | 51             | year ~7.1e7
	//	16        | 65,535                        | 47             | year ~4.5e6
	//	20 (dflt) | 1,048,575                     | 43             | year 280,707
	//	24        | 16,777,215                    | 39             | year 19,391
	//
	// The range is expressed in seconds of bucket timestamp whatever the
	// EpochUnit, and it is symmetric around the epoch: a cache accepts bucket
	// timestamps within +-2^(63-CountBits) seconds. An epoch outside it makes
	// Store and Get return -1 without advancing the high-water mark, the same
	// rejection an unrepresentable timestamp has always received.
	//
	// The upper bound of 24 keeps at least 39 timestamp bits, so every epoch that
	// an int64 of nanoseconds can express (up to 2262-04-11) still fits with room
	// to spare. It is where the line is drawn because past it the timestamp range
	// erodes toward dates real deployments reach: 32 count bits would stop at
	// 2038-01-19. Below the default, the timestamp range gained is one nobody
	// needs while the spills bought are real: a bucket that outgrows its count
	// continues into further words with the same timestamp, which is correct but
	// costs memory. A key taking 20,000 events per bucket needs 79 words per
	// bucket at CountBits=8, about 185 KB over a 300-bucket window, against 2.3 KB
	// at 16 or 20.
	CountBits int
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
	if c.CountBits != 0 && (c.CountBits < minCountBits || c.CountBits > maxCountBits) {
		return fmt.Errorf(
			"slidingcache: CountBits must be 0 (default %d) or between %d and %d, got %d",
			defaultCountBits, minCountBits, maxCountBits, c.CountBits,
		)
	}
	if c.SweepInterval < 0 {
		return fmt.Errorf("slidingcache: SweepInterval must be >= 0, got %s", c.SweepInterval)
	}
	return c.validateFutureGuard()
}

// validateFutureGuard rejects a future guard that cannot work: a skew the
// second-resolution internal clock cannot express, and a Clock that nothing
// would ever call.
func (c Config) validateFutureGuard() error {
	if c.MaxFutureSkew < 0 {
		return fmt.Errorf("slidingcache: MaxFutureSkew must be >= 0, got %s", c.MaxFutureSkew)
	}
	if c.MaxFutureSkew%time.Second != 0 {
		return fmt.Errorf(
			"slidingcache: MaxFutureSkew must be a whole multiple of 1s, got %s", c.MaxFutureSkew,
		)
	}
	if c.Clock != nil && c.MaxFutureSkew == 0 {
		return errors.New("slidingcache: Clock requires MaxFutureSkew > 0; without a skew it is never consulted")
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

func (c Config) countBits() int {
	if c.CountBits == 0 {
		return defaultCountBits
	}
	return c.CountBits
}

func (c Config) shardCount() int {
	if c.Shards <= 0 {
		return defaultShards
	}
	return c.Shards
}

// futureClock returns the clock the future guard consults, or nil when the
// guard is disabled and no clock would ever be called.
func (c Config) futureClock() func() int64 {
	switch {
	case c.MaxFutureSkew == 0:
		return nil
	case c.Clock != nil:
		return c.Clock
	default:
		return unixClock(c.EpochUnit)
	}
}

// unixClock returns the wall clock expressed in unit, used when Config.Clock is
// left nil.
func unixClock(unit EpochUnit) func() int64 {
	switch unit {
	case EpochInNanos:
		return func() int64 { return time.Now().UnixNano() }
	case EpochInSeconds:
		return func() int64 { return time.Now().Unix() }
	default:
		return func() int64 { return time.Now().UnixMilli() }
	}
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

	// layout is the bucket word layout chosen by Config.CountBits. Store and Get
	// use it to reject unrepresentable timestamps; the shards hold their own copy
	// for the packing itself.
	// maxFutureSkew is Config.MaxFutureSkew in seconds, and zero when the future
	// guard is disabled. clock is nil exactly when maxFutureSkew is zero; the
	// guard is the only caller and checks the skew first.
	maxFutureSkew int64
	clock         func() int64

	layout    bucketLayout
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
	layout := newBucketLayout(cfg.countBits())
	c := &Cache{
		precision:  int64(cfg.Precision / time.Second),
		windowSize: int64(cfg.WindowSize / time.Second),
		bucketDiv:  bucketDivisor(cfg.Precision, cfg.EpochUnit),
		unit:       cfg.EpochUnit,

		maxFutureSkew: int64(cfg.MaxFutureSkew / time.Second),
		clock:         cfg.futureClock(),

		layout:     layout,
		shards:     newShards(shardCount, layout),
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

// Sentinels returned by Store and Get in place of a count.
const (
	// LateEvent is returned when a timestamp falls outside the live window
	// (t <= HW - WindowSize) or outside the range a bucket word represents.
	LateEvent = -1
	// FutureEvent is returned when a timestamp lies further ahead of Clock()
	// than MaxFutureSkew allows. It is only ever returned by a Cache configured
	// with a non-zero MaxFutureSkew.
	FutureEvent = -2
)

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
// returns LateEvent to signal "late event, not stored".
//
// When Config.MaxFutureSkew is set, an event whose bucket timestamp lies
// further than the skew ahead of Config.Clock is not stored either: Store
// returns FutureEvent and leaves the high-water mark untouched, so a misdated
// event cannot expire the live window. See the package documentation.
//
// An epoch whose bucket timestamp is not representable (beyond
// +-2^(63-CountBits) seconds from the epoch, about +-278,000 years with the
// default CountBits) is rejected the same way, before the high-water mark is
// consulted, so it cannot drag the window with it: packing such a timestamp
// would overflow into the sign bit, and letting it through would move the
// high-water mark to a value no real event can reach.
func (c *Cache) Store(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !c.layout.inRange(timestamp) {
		return LateEvent
	}
	// Only an event beyond the mark can advance it, so the future guard, and
	// with it the clock, stays out of the steady-state path.
	highWater := c.highWater.Load()
	if timestamp > highWater {
		if c.maxFutureSkew > 0 && c.isFuture(timestamp) {
			return FutureEvent
		}
		highWater = c.advanceHighWater(timestamp, highWater)
	}
	if timestamp <= cutoffFor(highWater, c.windowSize) {
		return LateEvent
	}
	// The pre-lock check above is only a fast reject; the shard re-derives the
	// cutoff under its lock, where it cannot be stale.
	return c.shardFor(keyInHash).store(keyInHash, timestamp, &c.highWater, c.windowSize)
}

// Get returns the live count for keyInHash within the sliding window covering
// epoch, or 0 if the key does not exist. If epoch itself falls outside the live
// window (t <= HW - WindowSize), or its bucket timestamp is not representable,
// Get returns LateEvent; if it lies further than Config.MaxFutureSkew ahead of
// Config.Clock, Get returns FutureEvent. Get reads the current high-water mark
// but does not advance it.
func (c *Cache) Get(epoch int64, keyInHash string) int {
	timestamp := c.bucket(epoch)
	if !c.layout.inRange(timestamp) {
		return LateEvent
	}
	highWater := c.highWater.Load()
	if timestamp > highWater && c.maxFutureSkew > 0 && c.isFuture(timestamp) {
		return FutureEvent
	}
	cutoff := cutoffFor(highWater, c.windowSize)
	if timestamp <= cutoff {
		return LateEvent
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

// isFuture reports whether timestamp lies further ahead of the clock than
// MaxFutureSkew allows. The clock reading is truncated to a bucket so that the
// comparison happens in the same units on both sides.
//
// Callers must guard the call with maxFutureSkew > 0, which keeps a cache
// without the feature to a branch rather than a call, and must only reach it
// for a timestamp beyond the high-water mark: that is what keeps the clock out
// of the steady-state path, and it also means a clock that steps backwards can
// only tighten the guard for new marks, never push the high-water mark back.
func (c *Cache) isFuture(timestamp int64) bool {
	return timestamp > c.bucket(c.clock())+c.maxFutureSkew
}

// advanceHighWater raises the global high-water mark to at least timestamp using
// a compare-and-swap loop and returns the resulting high-water mark. current is
// the caller's already-loaded value of the mark, so the hot path loads it once
// and only a lost race pays for a reload.
func (c *Cache) advanceHighWater(timestamp, current int64) int64 {
	for {
		if timestamp <= current {
			return current
		}
		if c.highWater.CompareAndSwap(current, timestamp) {
			return timestamp
		}
		current = c.highWater.Load()
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
