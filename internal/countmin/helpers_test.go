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
