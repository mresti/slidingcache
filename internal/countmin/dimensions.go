package countmin

import (
	"fmt"
	"math"
)

// MaxDepth is the largest number of rows a sketch may have. Sixteen rows already
// bring delta down to about 1e-7, and the fixed bound lets the row salts live in
// an array.
const MaxDepth = 16

// Cell sizes in bytes, used to check that a structure's memory fits in an int.
const (
	sketchCellBytes    = 4
	aggregateCellBytes = 8
)

// Dimensions returns the width and depth of a sketch whose estimates exceed the
// true count by more than epsilon times the total with probability at most
// delta: width = ceil(e/epsilon) and depth = max(ceil(ln(1/delta)), 1). Both
// parameters must lie strictly between 0 and 1, and the result must fit within
// MaxDepth rows, math.MaxUint32 columns and an int's worth of bytes.
func Dimensions(epsilon, delta float64) (width, depth int, err error) {
	if !isOpenUnitInterval(epsilon) {
		return 0, 0, fmt.Errorf("countmin: epsilon %v is not strictly between 0 and 1", epsilon)
	}
	if !isOpenUnitInterval(delta) {
		return 0, 0, fmt.Errorf("countmin: delta %v is not strictly between 0 and 1", delta)
	}
	exactWidth := math.Ceil(math.E / epsilon)
	if exactWidth > math.MaxUint32 {
		return 0, 0, fmt.Errorf("countmin: epsilon %v needs width %v, above %d",
			epsilon, exactWidth, uint64(math.MaxUint32))
	}
	exactDepth := max(math.Ceil(math.Log(1/delta)), 1)
	if exactDepth > MaxDepth {
		return 0, 0, fmt.Errorf("countmin: delta %v needs depth %v, above %d", delta, exactDepth, MaxDepth)
	}
	if err := checkMemory(uint64(exactWidth), uint64(exactDepth), sketchCellBytes, math.MaxInt); err != nil {
		return 0, 0, err
	}
	return int(exactWidth), int(exactDepth), nil
}

// isOpenUnitInterval reports whether x is in (0, 1). It is written so that NaN,
// which fails every comparison, is rejected.
func isOpenUnitInterval(x float64) bool {
	return x > 0 && x < 1
}

// shape holds validated dimensions. A slot and its aggregate share one, and
// comparing two shapes is how a mismatched slot is caught.
type shape struct {
	width uint64
	depth int
}

// validateShape checks explicit dimensions for a structure whose cells take
// cellBytes each.
func validateShape(width, depth int, cellBytes uint64) (shape, error) {
	if width < 1 || uint64(width) > math.MaxUint32 {
		return shape{}, fmt.Errorf("countmin: width %d is not between 1 and %d", width, uint64(math.MaxUint32))
	}
	if depth < 1 || depth > MaxDepth {
		return shape{}, fmt.Errorf("countmin: depth %d is not between 1 and %d", depth, MaxDepth)
	}
	sh := shape{width: uint64(width), depth: depth}
	if err := checkMemory(sh.width, uint64(depth), cellBytes, math.MaxInt); err != nil {
		return shape{}, err
	}
	return sh, nil
}

// checkMemory rejects dimensions whose cells would take more than limit bytes.
// With width <= math.MaxUint32, depth <= MaxDepth and 8-byte cells the product is
// below 2^39, so it cannot overflow; only a 32-bit int can be exceeded. limit is
// a parameter so that case can be tested on any platform.
func checkMemory(width, depth, cellBytes, limit uint64) error {
	if bytes := width * depth * cellBytes; bytes > limit {
		return fmt.Errorf("countmin: %d x %d cells need %d bytes, above %d", width, depth, bytes, limit)
	}
	return nil
}
