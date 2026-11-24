package countmin

import "testing"

// splitmix64 turns a small id into a well-mixed 64-bit hash. Tests that assert
// numbers feed these fixed hashes to AddHash and CountHash, because a sketch's
// maphash seed is random and cannot be fixed.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

func mustSketch(t testing.TB, width, depth int) *Sketch {
	t.Helper()
	s, err := NewSized(width, depth)
	if err != nil {
		t.Fatalf("NewSized(%d, %d): %v", width, depth, err)
	}
	return s
}

func mustRing(t testing.TB, width, depth, slots int) *Ring {
	t.Helper()
	r, err := NewRing(width, depth, slots)
	if err != nil {
		t.Fatalf("NewRing(%d, %d, %d): %v", width, depth, slots, err)
	}
	return r
}

// cellIndexes returns the flat index of h's cell in every row, through the same
// column function the sketch uses.
func cellIndexes(h, width uint64, depth int) []uint64 {
	indexes := make([]uint64, depth)
	for r := range depth {
		indexes[r] = uint64(r)*width + column(h, rowSalts[r], width)
	}
	return indexes
}

// assertAggregateIsSumOfSlots checks the ring's invariant cell by cell: every
// aggregate cell equals the sum of that cell over the slots.
func assertAggregateIsSumOfSlots(t testing.TB, r *Ring) {
	t.Helper()
	for j, got := range r.agg {
		var want uint64
		for s := range r.slotCount {
			want += uint64(r.slots[s*r.cellsPerSlot+j])
		}
		if got != want {
			t.Fatalf("aggregate cell %d = %d, want sum of slots %d", j, got, want)
		}
	}
}

// slotCell returns cell j of slot, for tests that plant or read one directly.
func slotCell(r *Ring, slot int, j uint64) *uint32 {
	return &r.slots[uint64(slot*r.cellsPerSlot)+j]
}

// plantSlotCell sets cell j of slot to value and moves the aggregate cell by the
// same amount, so the invariant still holds.
func plantSlotCell(r *Ring, slot int, j uint64, value uint32) {
	cell := slotCell(r, slot, j)
	r.agg[j] = r.agg[j] - uint64(*cell) + uint64(value)
	*cell = value
}

// hashSharingOnlyRow returns the first splitmix64 hash, searching from id 1000
// on, whose cell is partner's in row and differs from partner's in every other
// row of a width x depth sketch.
func hashSharingOnlyRow(t testing.TB, partner, width uint64, depth, row int) uint64 {
	t.Helper()
	return hashSharingRows(t, partner, width, depth, func(r int) bool { return r == row })
}

// hashSharingRows returns the first splitmix64 hash, searching from id 1000 on,
// whose cell is partner's in exactly the rows for which shared is true.
func hashSharingRows(t testing.TB, partner, width uint64, depth int, shared func(row int) bool) uint64 {
	t.Helper()
	want := cellIndexes(partner, width, depth)
	for id := uint64(1000); id < 1_000_000; id++ {
		h := splitmix64(id)
		got := cellIndexes(h, width, depth)
		matches := true
		for r := range depth {
			if (got[r] == want[r]) != shared(r) {
				matches = false
				break
			}
		}
		if matches {
			return h
		}
	}
	t.Fatalf("no hash shares the wanted rows with %#x at %dx%d", partner, width, depth)
	return 0
}

// classicSlots is the classic count-min reference for a Ring: one Sketch per
// slot, counted through AddHash. AddHash uses the hash as the Ring does, row r
// at [r*width, (r+1)*width) and column(h, rowSalts[r], width) within it, so
// each Sketch's cells line up with its slot's cells index for index.
type classicSlots struct {
	slots []*Sketch
}

func newClassicSlots(t testing.TB, width, depth, slots int) *classicSlots {
	t.Helper()
	c := &classicSlots{}
	for range slots {
		c.slots = append(c.slots, mustSketch(t, width, depth))
	}
	return c
}

func (c *classicSlots) add(slot int, h uint64) {
	c.slots[slot].AddHash(h)
}

func (c *classicSlots) expire(slot int) {
	c.slots[slot].Reset()
}

func (c *classicSlots) reset() {
	for _, s := range c.slots {
		s.Reset()
	}
}

// aggregateCells sums every slot's cells, the aggregate a classic Ring holds.
func (c *classicSlots) aggregateCells() []uint64 {
	sum := make([]uint64, len(c.slots[0].cells))
	for _, s := range c.slots {
		for j, cell := range s.cells {
			sum[j] += uint64(cell)
		}
	}
	return sum
}

// estimate is the classic window estimate: the smallest aggregate cell of h.
func (c *classicSlots) estimate(h uint64) uint64 {
	s := c.slots[0]
	return minCell(c.aggregateCells(), h, s.width, s.depth)
}
