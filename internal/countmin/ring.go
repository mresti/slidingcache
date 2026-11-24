package countmin

import (
	"fmt"
	"math"
)

// Ring is the storage of a sliding window: a fixed number of slot sketches and
// an aggregate holding their cell-wise sum, so the estimate for the whole window
// is the minimum of a single set of cells. Counting into a slot through Add
// mirrors the count in the aggregate, and Expire removes a slot from it.
//
// Every slot shares the aggregate's dimensions and the column of a hash depends
// only on the hash, so all of them live in one flat array of 32-bit cells, slot
// s at [s*width*depth, (s+1)*width*depth), next to one array of 64-bit aggregate
// cells: two allocations whatever the number of slots, and no per-slot object
// for the garbage collector to scan. The 64-bit aggregate cells hold the sum of
// many saturated slot cells without wrapping.
//
// It maintains one invariant: after any sequence of calls, each aggregate cell
// equals the sum of the same cell over every slot.
//
// A slot index outside [0, Slots()) is a programming error, and every method
// that takes one panics on it. The zero value is not usable; build one with
// NewRing. A Ring is not safe for concurrent use.
type Ring struct {
	slots []uint32
	agg   []uint64
	shape
	cellsPerSlot int
	slotCount    int
}

// NewRing returns a ring of slots empty sketches of exactly width columns and
// depth rows, with the same bounds on the dimensions as NewSized. It allocates
// every slot up front and refuses, rather than wraps, a ring whose cells would
// not fit in an int's worth of bytes.
func NewRing(width, depth, slots int) (*Ring, error) {
	sh, err := validateShape(width, depth, aggregateCellBytes)
	if err != nil {
		return nil, err
	}
	if slots < 1 {
		return nil, fmt.Errorf("countmin: a ring needs at least 1 slot, got %d", slots)
	}
	cellsPerSlot := width * depth
	if err := checkRingMemory(cellsPerSlot, slots, math.MaxInt); err != nil {
		return nil, err
	}
	return &Ring{
		slots:        make([]uint32, cellsPerSlot*slots),
		agg:          make([]uint64, cellsPerSlot),
		shape:        sh,
		cellsPerSlot: cellsPerSlot,
		slotCount:    slots,
	}, nil
}

// checkRingMemory rejects a ring whose slot cells would take more than limit
// bytes. The division keeps the check itself from overflowing, whatever the
// number of slots; validateShape already bounded a slot's bytes. limit is a
// parameter so the 32-bit case can be tested on any platform.
func checkRingMemory(cellsPerSlot, slots, limit int) error {
	slotBytes := cellsPerSlot * sketchCellBytes
	if slots > limit/slotBytes {
		return fmt.Errorf("countmin: %d slots of %d cells need more than %d bytes", slots, cellsPerSlot, limit)
	}
	return nil
}

// Slots returns the number of slots in the ring.
func (r *Ring) Slots() int {
	return r.slotCount
}

// Add counts one occurrence of h in slot and the same occurrence in the
// aggregate, in one pass over the rows since both share the cell indexes, and
// returns the aggregate's estimate for h afterwards.
//
// A row whose slot cell has already saturated is left unchanged in both, so the
// invariant still holds, and saturated reports that it happened.
func (r *Ring) Add(slot int, h uint64) (estimate uint64, saturated bool) {
	slotCells, aggCells, width := r.slotCells(slot), r.agg, r.width
	estimate = math.MaxUint64
	base := uint64(0)
	for _, salt := range rowSalts[:r.depth] {
		j := base + column(h, salt, width)
		if !raiseMirrored(&slotCells[j], &aggCells[j]) {
			saturated = true
		}
		estimate = min(estimate, aggCells[j])
		base += width
	}
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
func (r *Ring) Count(h uint64) uint64 {
	return minCell(r.agg, h, r.width, r.depth)
}

// Expire subtracts every cell of slot from the aggregate and empties the slot,
// which is how a slot leaves the window and becomes ready for reuse.
func (r *Ring) Expire(slot int) {
	slotCells := r.slotCells(slot)
	aggCells := r.agg[:len(slotCells)]
	for j, c := range slotCells {
		aggCells[j] -= uint64(c)
	}
	clear(slotCells)
}

// Reset empties every slot and the aggregate without allocating.
func (r *Ring) Reset() {
	clear(r.slots)
	clear(r.agg)
}

// AppendSlotCells appends a copy of slot's cells to dst and returns the result,
// laid out as a sketch's: row i at [i*width, (i+1)*width). It is a read-only
// view for checking the invariant from outside the package; a dst with room
// for them makes it allocation-free.
func (r *Ring) AppendSlotCells(dst []uint32, slot int) []uint32 {
	return append(dst, r.slotCells(slot)...)
}

// AppendAggregateCells appends a copy of the aggregate's cells to dst and
// returns the result, with the layout of AppendSlotCells.
func (r *Ring) AppendAggregateCells(dst []uint64) []uint64 {
	return append(dst, r.agg...)
}

// slotCells returns slot's cells, capped so that appending cannot reach the
// next slot. The explicit check is what makes an out-of-range slot panic on
// every platform: on a 32-bit int, slot*cellsPerSlot could wrap back into range.
func (r *Ring) slotCells(slot int) []uint32 {
	if uint(slot) >= uint(r.slotCount) {
		panicSlotOutOfRange(slot, r.slotCount)
	}
	start := slot * r.cellsPerSlot
	end := start + r.cellsPerSlot
	return r.slots[start:end:end]
}

func panicSlotOutOfRange(slot, slotCount int) {
	panic(fmt.Sprintf("countmin: slot %d is outside the ring's [0, %d)", slot, slotCount))
}
