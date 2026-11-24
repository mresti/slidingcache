package countminsketch

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

const (
	// defaultEpsilon and defaultDelta size the sketches of a Config that leaves
	// Epsilon and Delta zero: 2719 columns by 5 rows, an error of at most 0.1% of
	// the window's events with 99% probability.
	defaultEpsilon = 0.001
	defaultDelta   = 0.01

	// defaultTimestampBits matches the range of the root package's default
	// CountBits of 20, so both caches accept the same epochs out of the box.
	defaultTimestampBits = 43
	// minTimestampBits and maxTimestampBits bound Config.TimestampBits. The
	// minimum mirrors the root package's widest CountBits of 24. The maximum is
	// the arithmetic ceiling: with a wider range bucket() and HW - WindowSize
	// could overflow int64.
	minTimestampBits = 39
	maxTimestampBits = 62

	// maxBuckets bounds the ring of sketches, which New allocates whole.
	maxBuckets = 1 << 20

	// maxShards matches the root package's bound on Config.Shards, and
	// defaultMaxShards its default, which a narrow sketch lowers.
	maxShards        = 1 << 20
	defaultMaxShards = 16
	// minShardWidth is the fewest columns a shard may have when there is more
	// than one. It guards against a cliff, not a per-shard guarantee: simulated
	// with Zipf 1.1 and 1.5 and 100 hot keys, at 4 columns per shard or fewer
	// tens of shards have 10-50% of their keys above Epsilon times the window's
	// events; from 6 to 12 columns the degradation is gradual, and the tails
	// match those of an unsharded sketch from about 40 columns per shard.
	minShardWidth = 8

	// defaultMaxMemoryBytes is the most memory New reserves when
	// Config.MaxMemoryBytes is zero.
	defaultMaxMemoryBytes = 1 << 30
)

// Config configures a Cache. The window fields and Shards follow the same rules
// as slidingcache.Config; the sketch fields replace its CountBits and
// SweepInterval, which have no meaning here.
type Config struct {
	// Precision is the bucket granularity to which timestamps are truncated.
	// Must be >= 1s and an exact multiple of time.Second.
	Precision time.Duration
	// WindowSize is the total sliding-window length. Must be >= Precision and an
	// exact multiple of time.Second. The window holds ceil(WindowSize/Precision)
	// buckets, which must not exceed 2^20.
	WindowSize time.Duration
	// Shards is the number of internal shards, each an independently locked
	// partition of the keys with its own share of the sketch's columns. It is
	// rounded up to a power of two and must be between 0 and 1<<20. Zero selects
	// the largest power of two up to 16 that leaves every shard at least 8
	// columns, and 1 when no power of two does. An explicit value that leaves
	// fewer than 8 columns per shard makes New return an error.
	//
	// Shards only buys concurrency: the columns are divided among the shards, so
	// accuracy and memory are set by Epsilon and Delta. More shards add memory
	// only by rounding each shard's columns up and by 8 bytes per bucket per
	// shard: with the default Epsilon, 256 shards take about 7% more than 16.
	// With fewer than about 40 columns per shard, e/(Epsilon*Shards), the keys
	// that share a shard with a very hot key see fatter tails than with one
	// shard.
	Shards int
	// EpochUnit is the unit of the epoch arguments passed to Store and Get. Its
	// zero value is slidingcache.EpochInMillis.
	EpochUnit slidingcache.EpochUnit
	// MaxFutureSkew is the furthest a bucket timestamp may lie ahead of Clock()
	// before Store and Get reject it with slidingcache.FutureEvent. Zero disables
	// the check. Must be >= 0 and an exact multiple of time.Second.
	MaxFutureSkew time.Duration
	// Clock returns the current epoch in EpochUnit. It is consulted only when
	// MaxFutureSkew > 0, and only for a timestamp beyond the high-water mark. A
	// nil Clock means the Unix wall clock in EpochUnit; setting Clock without
	// MaxFutureSkew is rejected by New as dead configuration.
	Clock func() int64
	// Epsilon bounds the overestimate as a fraction of the events in the window.
	// Zero selects 0.001. Any other value must lie strictly between 0 and 1. The
	// sketch width is ceil(e/Epsilon), so memory grows as 1/Epsilon.
	Epsilon float64
	// Delta is the probability that an estimate exceeds the bound Epsilon sets.
	// Zero selects 0.01. Any other value must lie strictly between 0 and 1. The
	// sketch depth is ceil(ln(1/Delta)), at most 16 rows.
	Delta float64
	// TimestampBits sets the range of bucket timestamps, in seconds, that Store
	// and Get accept: [-2^TimestampBits, 2^TimestampBits - 1]. Anything outside
	// is rejected with slidingcache.LateEvent without touching the high-water
	// mark, which absorbs gross unit mistakes such as nanoseconds fed to a cache
	// configured for seconds. Zero selects 43, the root package's default range.
	// Any other value must be between 39 and 62; 39 to 55 are the ranges of the
	// root package's CountBits 24 to 8.
	TimestampBits int
	// DisableRangeGuard widens the accepted range to the arithmetic ceiling,
	// the same as TimestampBits 62: epochs beyond it are still rejected, because
	// their bucket timestamps cannot be computed without overflowing int64. It
	// cannot be combined with a non-zero TimestampBits.
	DisableRangeGuard bool
	// MaxMemoryBytes is the most memory New may reserve for the window. Zero
	// selects 1 GiB; a negative value is rejected. New estimates the memory from
	// the other fields before allocating anything and returns an error when the
	// estimate exceeds this cap, so a larger configuration must raise it
	// explicitly. The estimate counts the cells and per-bucket timestamps, which
	// dominate, and ignores the fixed overhead of a few hundred bytes per shard.
	MaxMemoryBytes int64
}

func (c Config) validate() error {
	if err := validateWholeSeconds("Precision", c.Precision); err != nil {
		return err
	}
	if err := validateWholeSeconds("WindowSize", c.WindowSize); err != nil {
		return err
	}
	if c.WindowSize < c.Precision {
		return fmt.Errorf("countminsketch: WindowSize (%s) must be >= Precision (%s)", c.WindowSize, c.Precision)
	}
	if buckets := c.bucketCount(); buckets > maxBuckets {
		return fmt.Errorf(
			"countminsketch: WindowSize/Precision needs %d buckets, above the maximum of %d", buckets, maxBuckets,
		)
	}
	if c.Shards < 0 || c.Shards > maxShards {
		return fmt.Errorf("countminsketch: Shards must be between 0 and %d, got %d", maxShards, c.Shards)
	}
	if c.MaxMemoryBytes < 0 {
		return fmt.Errorf(
			"countminsketch: MaxMemoryBytes must be >= 0 (0 selects %d), got %d",
			defaultMaxMemoryBytes, c.MaxMemoryBytes,
		)
	}
	if !validEpochUnit(c.EpochUnit) {
		return fmt.Errorf("countminsketch: invalid EpochUnit %d", c.EpochUnit)
	}
	if err := c.validateRange(); err != nil {
		return err
	}
	return c.validateFutureGuard()
}

// validEpochUnit accepts exactly the units the root package defines; its own
// validity check is unexported.
func validEpochUnit(unit slidingcache.EpochUnit) bool {
	switch unit {
	case slidingcache.EpochInMillis, slidingcache.EpochInNanos, slidingcache.EpochInSeconds:
		return true
	default:
		return false
	}
}

func (c Config) validateRange() error {
	if c.DisableRangeGuard && c.TimestampBits != 0 {
		return fmt.Errorf(
			"countminsketch: DisableRangeGuard cannot be combined with TimestampBits (%d)", c.TimestampBits,
		)
	}
	if c.TimestampBits != 0 && (c.TimestampBits < minTimestampBits || c.TimestampBits > maxTimestampBits) {
		return fmt.Errorf(
			"countminsketch: TimestampBits must be 0 (default %d) or between %d and %d, got %d",
			defaultTimestampBits, minTimestampBits, maxTimestampBits, c.TimestampBits,
		)
	}
	return nil
}

// validateFutureGuard rejects a future guard that cannot work: a skew the
// second-resolution internal clock cannot express, and a Clock that nothing
// would ever call.
func (c Config) validateFutureGuard() error {
	if c.MaxFutureSkew < 0 {
		return fmt.Errorf("countminsketch: MaxFutureSkew must be >= 0, got %s", c.MaxFutureSkew)
	}
	if c.MaxFutureSkew%time.Second != 0 {
		return fmt.Errorf("countminsketch: MaxFutureSkew must be a whole multiple of 1s, got %s", c.MaxFutureSkew)
	}
	if c.Clock != nil && c.MaxFutureSkew == 0 {
		return errors.New("countminsketch: Clock requires MaxFutureSkew > 0; without a skew it is never consulted")
	}
	return nil
}

// validateWholeSeconds rejects durations that are not positive, whole-second
// values so that timestamps map cleanly onto the int64-seconds internal clock
// without silent rounding.
func validateWholeSeconds(name string, d time.Duration) error {
	if d < time.Second {
		return fmt.Errorf("countminsketch: %s must be >= 1s, got %s", name, d)
	}
	if d%time.Second != 0 {
		return fmt.Errorf("countminsketch: %s must be a whole multiple of 1s, got %s", name, d)
	}
	return nil
}

// bucketCount is the number of Precision buckets a window spans, rounded up so
// that a WindowSize that is not a multiple of Precision still fits every live
// bucket. Both durations are validated whole seconds, so the sum cannot
// overflow.
func (c Config) bucketCount() int64 {
	precision := int64(c.Precision / time.Second)
	windowSize := int64(c.WindowSize / time.Second)
	return (windowSize + precision - 1) / precision
}

func (c Config) timestampBits() int {
	switch {
	case c.DisableRangeGuard:
		return maxTimestampBits
	case c.TimestampBits == 0:
		return defaultTimestampBits
	default:
		return c.TimestampBits
	}
}

// dimensions returns the width and depth of the whole sketch, before its columns
// are divided among the shards.
func (c Config) dimensions() (width, depth int, err error) {
	delta := c.Delta
	if delta == 0 {
		delta = defaultDelta
	}
	width, depth, err = countmin.Dimensions(c.epsilon(), delta)
	if err != nil {
		return 0, 0, fmt.Errorf("countminsketch: invalid Epsilon %v or Delta %v: %w", c.Epsilon, c.Delta, err)
	}
	return width, depth, nil
}

func (c Config) epsilon() float64 {
	if c.Epsilon == 0 {
		return defaultEpsilon
	}
	return c.Epsilon
}

// layout sizes the window: Epsilon and Delta give the whole sketch, whose
// columns are divided among the shards, each holding a ring of one slot per
// bucket. It expects a validated Config.
func (c Config) layout() (windowLayout, error) {
	totalWidth, depth, err := c.dimensions()
	if err != nil {
		return windowLayout{}, err
	}
	shards, err := c.shardCount(totalWidth)
	if err != nil {
		return windowLayout{}, err
	}
	return windowLayout{
		shards:     shards,
		shardWidth: ceilDiv(totalWidth, shards),
		depth:      depth,
		buckets:    int(c.bucketCount()),
	}, nil
}

// shardCount resolves Config.Shards to a power of two that leaves every shard at
// least minShardWidth of the totalWidth columns, or explains why it cannot.
func (c Config) shardCount(totalWidth int) (int, error) {
	if c.Shards == 0 {
		return defaultShardCount(totalWidth), nil
	}
	shards := roundUpPow2(c.Shards)
	if shards > 1 && ceilDiv(totalWidth, shards) < minShardWidth {
		return 0, fmt.Errorf(
			"countminsketch: Epsilon %v gives %d columns, %d per shard with %d Shards, below the minimum of %d; "+
				"use fewer Shards or a smaller Epsilon",
			c.epsilon(), totalWidth, ceilDiv(totalWidth, shards), shards, minShardWidth,
		)
	}
	return shards, nil
}

// defaultShardCount is the largest power of two up to defaultMaxShards that
// leaves every shard minShardWidth columns, so a default never makes New fail.
func defaultShardCount(totalWidth int) int {
	for shards := defaultMaxShards; shards > 1; shards /= 2 {
		if ceilDiv(totalWidth, shards) >= minShardWidth {
			return shards
		}
	}
	return 1
}

// maxMemoryBytes expects a validated Config, where MaxMemoryBytes is not
// negative.
func (c Config) maxMemoryBytes() uint64 {
	if c.MaxMemoryBytes <= 0 {
		return defaultMaxMemoryBytes
	}
	return uint64(c.MaxMemoryBytes)
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
func unixClock(unit slidingcache.EpochUnit) func() int64 {
	switch unit {
	case slidingcache.EpochInNanos:
		return func() int64 { return time.Now().UnixNano() }
	case slidingcache.EpochInSeconds:
		return func() int64 { return time.Now().Unix() }
	default:
		return func() int64 { return time.Now().UnixMilli() }
	}
}

// roundUpPow2 rounds n up to a power of two, as the root package rounds Shards.
func roundUpPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// ceilDiv divides positive integers rounding up.
func ceilDiv(numerator, divisor int) int {
	return (numerator + divisor - 1) / divisor
}
