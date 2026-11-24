package countminsketch

import (
	"errors"
	"fmt"
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

	// maxBuckets bounds the ring of sketches, which New preallocates whole.
	maxBuckets = 1 << 20
)

// Config configures a Cache. The window fields follow the same rules as
// slidingcache.Config; the sketch fields replace its CountBits, Shards and
// SweepInterval, which have no meaning here.
type Config struct {
	// Precision is the bucket granularity to which timestamps are truncated.
	// Must be >= 1s and an exact multiple of time.Second.
	Precision time.Duration
	// WindowSize is the total sliding-window length. Must be >= Precision and an
	// exact multiple of time.Second. The window holds ceil(WindowSize/Precision)
	// buckets, which must not exceed 2^20.
	WindowSize time.Duration
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

func (c Config) dimensions() (width, depth int, err error) {
	epsilon, delta := c.Epsilon, c.Delta
	if epsilon == 0 {
		epsilon = defaultEpsilon
	}
	if delta == 0 {
		delta = defaultDelta
	}
	width, depth, err = countmin.Dimensions(epsilon, delta)
	if err != nil {
		return 0, 0, fmt.Errorf("countminsketch: invalid Epsilon %v or Delta %v: %w", c.Epsilon, c.Delta, err)
	}
	return width, depth, nil
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
