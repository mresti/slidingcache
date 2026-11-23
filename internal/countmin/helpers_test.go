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

func mustAggregate(t testing.TB, width, depth int) *Aggregate {
	t.Helper()
	a, err := NewAggregate(width, depth)
	if err != nil {
		t.Fatalf("NewAggregate(%d, %d): %v", width, depth, err)
	}
	return a
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

// assertAggregateIsSumOfSlots checks invariant I1 cell by cell: every aggregate
// cell equals the sum of that cell over the live slots.
func assertAggregateIsSumOfSlots(t testing.TB, a *Aggregate, live []*Sketch) {
	t.Helper()
	for j, got := range a.cells {
		var want uint64
		for _, slot := range live {
			want += uint64(slot.cells[j])
		}
		if got != want {
			t.Fatalf("aggregate cell %d = %d, want sum of live slots %d", j, got, want)
		}
	}
}
