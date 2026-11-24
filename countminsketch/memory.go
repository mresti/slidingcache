package countminsketch

import (
	"fmt"
	"math"
	"math/bits"
)

// windowLayout is the size of everything New allocates: shards rings of buckets
// slots, each slot a sketch of shardWidth columns by depth rows.
type windowLayout struct {
	shards     int
	shardWidth int
	depth      int
	buckets    int
}

// The bytes of each kind of allocation in a shard: a 32-bit cell per column,
// row and slot, a 64-bit timestamp per slot, and a 64-bit aggregate cell per
// column and row.
const (
	slotCellBytes      = 4
	timestampBytes     = 8
	aggregateCellBytes = 8
)

// checkMemory rejects a layout whose estimate exceeds limit, before anything is
// allocated. The limit is also capped at math.MaxInt, the most a 32-bit
// platform can address; on a 64-bit one MaxMemoryBytes is already below it.
func (l windowLayout) checkMemory(limit uint64) error {
	limit = min(limit, math.MaxInt)
	estimate, ok := l.memoryBytes()
	if !ok {
		return fmt.Errorf(
			"countminsketch: the window needs more than 2^64 bytes, above the cap of %d; %s", limit, memoryAdvice,
		)
	}
	if estimate > limit {
		return fmt.Errorf(
			"countminsketch: the window needs about %d bytes, above the cap of %d; %s", estimate, limit, memoryAdvice,
		)
	}
	return nil
}

const memoryAdvice = "raise MaxMemoryBytes, or use a larger Epsilon or Precision, or a smaller WindowSize"

// memoryBytes estimates the bytes New allocates for l:
//
//	shards * (buckets*shardWidth*depth*4 + buckets*8 + shardWidth*depth*8)
//
// in uint64 on every platform. ok is false when the estimate overflows. The
// bounds of Dimensions and maxBuckets keep a valid Config below 2^59 bytes
// today; the check keeps the estimate from wrapping to a small number should
// those bounds ever grow.
func (l windowLayout) memoryBytes() (estimate uint64, ok bool) {
	var m checkedArithmetic
	cells := m.mul(size(l.shardWidth), size(l.depth))
	buckets := size(l.buckets)
	perShard := m.add(
		m.add(m.mul(m.mul(buckets, cells), slotCellBytes), m.mul(buckets, timestampBytes)),
		m.mul(cells, aggregateCellBytes),
	)
	estimate = m.mul(size(l.shards), perShard)
	return estimate, !m.overflowed
}

// size converts a dimension of the layout, always positive, to uint64.
func size(n int) uint64 {
	return uint64(max(n, 0))
}

// checkedArithmetic multiplies and adds uint64 values, remembering whether any
// step overflowed, so a whole formula is written once and checked once.
type checkedArithmetic struct {
	overflowed bool
}

func (m *checkedArithmetic) mul(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	m.overflowed = m.overflowed || hi != 0
	return lo
}

func (m *checkedArithmetic) add(a, b uint64) uint64 {
	sum, carry := bits.Add64(a, b, 0)
	m.overflowed = m.overflowed || carry != 0
	return sum
}
