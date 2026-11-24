package countmin

import (
	"hash/maphash"
	"math"
)

// Sketch is a count-min sketch with 32-bit counters. An estimate is never below
// the true count while the item's counters are below math.MaxUint32; a counter
// that reaches it stays there instead of wrapping, so an item counted beyond it
// is underestimated rather than reported near zero.
//
// The zero value is not usable; build one with New or NewSized. A Sketch is not
// safe for concurrent use.
type Sketch struct {
	// cells holds depth rows of width counters, row r at [r*width, (r+1)*width).
	cells []uint32
	shape
	// total is the sum of every count added, kept apart from the cells because
	// it saturates at math.MaxUint64 rather than at a cell's limit.
	total uint64
	seed  maphash.Seed
}

// New returns an empty sketch sized by Dimensions for epsilon and delta.
func New(epsilon, delta float64) (*Sketch, error) {
	width, depth, err := Dimensions(epsilon, delta)
	if err != nil {
		return nil, err
	}
	return NewSized(width, depth)
}

// NewSized returns an empty sketch of exactly width columns and depth rows.
// width must be between 1 and math.MaxUint32 and depth between 1 and MaxDepth.
func NewSized(width, depth int) (*Sketch, error) {
	sh, err := validateShape(width, depth, sketchCellBytes)
	if err != nil {
		return nil, err
	}
	return &Sketch{
		cells: make([]uint32, width*depth),
		shape: sh,
		seed:  maphash.MakeSeed(),
	}, nil
}

// Add counts one occurrence of item.
func (s *Sketch) Add(item []byte) {
	s.AddN(item, 1)
}

// AddN counts n occurrences of item. Adding zero changes nothing.
func (s *Sketch) AddN(item []byte, n uint64) {
	if n == 0 {
		return
	}
	s.addHash(maphash.Bytes(s.seed, item), n)
}

// AddHash counts one occurrence of the item whose hash the caller has already
// computed, so a key hashed once can be counted without hashing it again. The
// hash must be well mixed, as maphash or a strong 64-bit hash is: the columns are
// derived from it directly.
func (s *Sketch) AddHash(h uint64) {
	s.addHash(h, 1)
}

// Count returns the estimated number of occurrences of item: never less than the
// true count unless a counter has saturated.
func (s *Sketch) Count(item []byte) uint64 {
	return s.CountHash(maphash.Bytes(s.seed, item))
}

// CountHash returns the estimate for the item whose hash the caller computed, as
// passed to AddHash.
func (s *Sketch) CountHash(h uint64) uint64 {
	return minCell(s.cells, h, s.width, s.depth)
}

// Total returns the sum of every count added since the sketch was built or last
// reset, saturating at math.MaxUint64.
func (s *Sketch) Total() uint64 {
	return s.total
}

// Reset empties the sketch, keeping its dimensions and seed, without allocating.
func (s *Sketch) Reset() {
	clear(s.cells)
	s.total = 0
}

// addHash applies the update rule for n occurrences of h.
func (s *Sketch) addHash(h, n uint64) {
	cells, width := s.cells, s.width
	base := uint64(0)
	for _, salt := range rowSalts[:s.depth] {
		raiseCell(&cells[base+column(h, salt, width)], n)
		base += width
	}
	s.total = saturatingAdd(s.total, n)
}

// raiseCell is the classic count-min update of one row: the cell grows by n,
// saturating at math.MaxUint32. The comparisons come before the addition so a
// large n cannot wrap the cell; for the n of 1 that AddHash passes, the first
// one folds away once inlined.
func raiseCell(cell *uint32, n uint64) {
	if n >= math.MaxUint32 {
		*cell = math.MaxUint32
		return
	}
	step := uint32(n)
	if step >= math.MaxUint32-*cell {
		*cell = math.MaxUint32
		return
	}
	*cell += step
}

// minCell returns the smallest of h's cells across depth rows of width columns.
// It serves both cell types, which share the layout.
func minCell[C uint32 | uint64](cells []C, h, width uint64, depth int) uint64 {
	estimate := uint64(math.MaxUint64)
	base := uint64(0)
	for _, salt := range rowSalts[:depth] {
		estimate = min(estimate, uint64(cells[base+column(h, salt, width)]))
		base += width
	}
	return estimate
}

// saturatingAdd returns a+b, or math.MaxUint64 when the sum would wrap.
func saturatingAdd(a, b uint64) uint64 {
	if sum := a + b; sum >= a {
		return sum
	}
	return math.MaxUint64
}
