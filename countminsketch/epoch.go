package countminsketch

import (
	"time"

	"github.com/mresti/slidingcache"
)

// The functions in this file mirror the root package's window arithmetic line
// for line, because the differential tests require both caches to map every
// epoch onto the same bucket and to move the high-water mark identically. The
// root package keeps them unexported; sharing them would mean an internal
// package that both depend on, which is left for a later change.

// bucket maps an epoch onto the first second of the Precision bucket containing
// it. Dividing once by the precomputed bucket width is equivalent to converting
// to seconds and then truncating, because floor(floor(x/a)/b) equals
// floor(x/(a*b)) for positive a and b.
//
// For seconds epochs within one Precision of math.MinInt64 the product wraps to
// a value near math.MaxInt64, which every accepted range rejects: that is why
// TimestampBits stops at 62.
func (c *Cache) bucket(epoch int64) int64 {
	return floorDiv(epoch, c.bucketDiv) * c.precision
}

// bucketDivisor expresses precision in the given epoch unit. Precision is a
// whole number of seconds, so it divides exactly by the nanoseconds spanned by
// any supported unit.
func bucketDivisor(precision time.Duration, unit slidingcache.EpochUnit) int64 {
	return int64(precision) / nanosPerUnit(unit)
}

func nanosPerUnit(unit slidingcache.EpochUnit) int64 {
	switch unit {
	case slidingcache.EpochInNanos:
		return 1
	case slidingcache.EpochInSeconds:
		return int64(time.Second)
	default:
		return int64(time.Millisecond)
	}
}

// floorDiv divides rounding toward negative infinity, unlike Go's "/", so the
// buckets straddling zero are as wide as every other.
func floorDiv(numerator, positiveDivisor int64) int64 {
	quotient := numerator / positiveDivisor
	if numerator%positiveDivisor < 0 {
		quotient--
	}
	return quotient
}

// cutoffFor derives the expiry boundary from a high-water mark: a timestamp is
// alive if and only if it is strictly greater. The subtraction needs no guard:
// the mark starts at noObservedHighWater and only ever rises.
func cutoffFor(highWater, windowSize int64) int64 {
	return highWater - windowSize
}

// isFuture reports whether timestamp lies further ahead of the clock than
// MaxFutureSkew allows, comparing in buckets on both sides. Callers guard it
// with maxFutureSkew > 0 and reach it only for a timestamp beyond the mark.
func (c *Cache) isFuture(timestamp int64) bool {
	return timestamp > c.bucket(c.clock())+c.maxFutureSkew
}

// advanceHighWater raises the high-water mark to at least timestamp with a
// compare-and-swap loop and returns the resulting mark. current is the
// caller's already-loaded value, so only a lost race pays for a reload.
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
