package countmin

import (
	"fmt"
	"math"
)

// Aggregate is the cell-wise sum of a set of sketches, the slots of a sliding
// window: counting into a slot through Add mirrors the count here, Sub removes a
// slot that expires, and AddSketch adds one back when the window is rebuilt.
// Because the slots share its dimensions and the column of a hash depends only
// on the hash, the estimate for the whole window is the minimum of a single set
// of cells, at the cost of one sketch's worth of work.
//
// It maintains one invariant: after any sequence of calls, each of its cells
// equals the sum of the same cell over the slots added and not subtracted.
// Cells are 64-bit, so the sum of many saturated 32-bit slot cells still fits.
//
// The zero value is not usable; build one with NewAggregate. An Aggregate is
// not safe for concurrent use, and neither are its slots.
type Aggregate struct {
	cells []uint64
	shape
}

// NewAggregate returns an empty aggregate of exactly width columns and depth
// rows, with the same bounds as NewSized.
func NewAggregate(width, depth int) (*Aggregate, error) {
	sh, err := validateShape(width, depth, aggregateCellBytes)
	if err != nil {
		return nil, err
	}
	return &Aggregate{cells: make([]uint64, width*depth), shape: sh}, nil
}

// NewSlot returns an empty sketch with the aggregate's dimensions. Each slot has
// its own maphash seed, so items reach the aggregate's hash space only through
// hashes the caller shares: count into a slot with Aggregate.Add, not Sketch.Add.
func (a *Aggregate) NewSlot() *Sketch {
	return newSketch(a.shape, len(a.cells))
}

// Add counts one occurrence of h in slot and the same occurrence in the
// aggregate, in one pass over the rows since both share the cell indexes, and
// returns the aggregate's estimate for h afterwards.
//
// A row whose slot cell has already saturated is left unchanged in both, so the
// invariant still holds, and saturated reports that it happened; the slot's
// Total counts the occurrence regardless. slot must have the aggregate's
// dimensions: anything else is a programming error, and Add panics.
func (a *Aggregate) Add(slot *Sketch, h uint64) (estimate uint64, saturated bool) {
	if slot.shape != a.shape {
		panic("countmin: Aggregate.Add given a slot whose dimensions differ from the aggregate's")
	}
	slotCells, aggCells, width := slot.cells, a.cells, a.width
	estimate = math.MaxUint64
	base := uint64(0)
	for _, salt := range rowSalts[:a.depth] {
		j := base + column(h, salt, width)
		if !raiseMirrored(&slotCells[j], &aggCells[j]) {
			saturated = true
		}
		estimate = min(estimate, aggCells[j])
		base += width
	}
	slot.total = saturatingAdd(slot.total, 1)
	return estimate, saturated
}

// raiseMirrored is the classic count-min update of one row for a slot and its
// aggregate: both cells grow by one, unless the slot cell is saturated, in which
// case neither moves and it reports false.
func raiseMirrored(slotCell *uint32, aggCell *uint64) bool {
	if *slotCell == math.MaxUint32 {
		return false
	}
	*slotCell++
	*aggCell++
	return true
}

// Count returns the aggregate's estimate for h: the smallest of its cells.
func (a *Aggregate) Count(h uint64) uint64 {
	return minCell(a.cells, h, a.width, a.depth)
}

// Sub subtracts every cell of slot from the aggregate, which is how a slot leaves
// the window. slot's counts must have been added to the aggregate before, through
// Add or AddSketch; Sub does not check, and subtracting a slot that was never
// added wraps the cells. It returns an error, changing nothing, when the
// dimensions differ.
func (a *Aggregate) Sub(slot *Sketch) error {
	if err := a.checkSlot(slot); err != nil {
		return err
	}
	aggCells := a.cells[:len(slot.cells)]
	for j, c := range slot.cells {
		aggCells[j] -= uint64(c)
	}
	return nil
}

// AddSketch adds every cell of slot to the aggregate, which is how the window is
// rebuilt from the slots that survive. It returns an error, changing nothing,
// when the dimensions differ.
func (a *Aggregate) AddSketch(slot *Sketch) error {
	if err := a.checkSlot(slot); err != nil {
		return err
	}
	aggCells := a.cells[:len(slot.cells)]
	for j, c := range slot.cells {
		aggCells[j] += uint64(c)
	}
	return nil
}

// Reset zeroes every cell of the aggregate without allocating.
func (a *Aggregate) Reset() {
	clear(a.cells)
}

// checkSlot reports, as an error, a slot whose dimensions differ from the
// aggregate's, for the bulk operations where a mismatch is recoverable.
func (a *Aggregate) checkSlot(slot *Sketch) error {
	if slot.shape != a.shape {
		return fmt.Errorf("countmin: slot is %d x %d, aggregate is %d x %d", slot.width, slot.depth, a.width, a.depth)
	}
	return nil
}
