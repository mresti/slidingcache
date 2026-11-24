package countmin

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// cellsWith returns width x depth zero cells with the given cells set, the
// whole of a slot or an aggregate whose every other cell must be zero.
func cellsWith[C uint32 | uint64](width uint64, depth int, set map[uint64]C) []C {
	cells := make([]C, width*uint64(depth))
	for j, c := range set {
		cells[j] = c
	}
	return cells
}

func requireCells[C uint32 | uint64](t *testing.T, step, what string, got, want []C) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: %s cells = %v, want %v", step, what, got, want)
	}
}

// TestRingAddRaisesOnlyTheRowsAtTheSlotMinimum follows every cell of a slot
// while x, which shares row 0 with y and no other row, is counted after y. The
// rows at the minimum climb, in the slot and in the aggregate, while the row
// above it waits; once they catch up, all of them move together.
func TestRingAddRaisesOnlyTheRowsAtTheSlotMinimum(t *testing.T) {
	const width, depth, slot = 8, 3, 1
	r := mustRing(t, width, depth, 2)
	y := splitmix64(1)
	x := hashSharingOnlyRow(t, y, width, depth, 0)
	ys, xs := cellIndexes(y, width, depth), cellIndexes(x, width, depth)
	r.Add(slot, y)
	r.Add(slot, y)

	steps := []struct {
		name     string
		x        [depth]uint32
		estimate uint64
	}{
		{"first x: the rows at 0 rise, the shared row at 2 stays", [depth]uint32{2, 1, 1}, 1},
		{"second x: the rows at 1 rise, the shared row at 2 stays", [depth]uint32{2, 2, 2}, 2},
		{"third x: every row is at the minimum and rises", [depth]uint32{3, 3, 3}, 3},
	}
	for _, s := range steps {
		estimate, saturated := r.Add(slot, x)

		want := map[uint64]uint32{ys[1]: 2, ys[2]: 2}
		for i, j := range xs {
			want[j] = s.x[i]
		}
		requireCells(t, s.name, "slot", r.AppendSlotCells(nil, slot), cellsWith(width, depth, want))
		wantAggregate := make(map[uint64]uint64, len(want))
		for j, c := range want {
			wantAggregate[j] = uint64(c)
		}
		requireCells(t, s.name, "aggregate", r.AppendAggregateCells(nil), cellsWith(width, depth, wantAggregate))
		if estimate != s.estimate || saturated {
			t.Fatalf("%s: Add = (%d, %t), want (%d, false)", s.name, estimate, saturated, s.estimate)
		}
		assertAggregateIsSumOfSlots(t, r)
	}
	if got := r.Count(y); got != 2 {
		t.Fatalf("Count(y) = %d, want its 2 adds: y's own rows were never raised by x", got)
	}
}

// TestRingAddFindsTheMinimumInTheLastRow is the case where the last row alone
// holds the minimum: x shares row 0 of 2 with y, counted twice, so x's cells
// are 2 and 0 and only row 1 may rise.
func TestRingAddFindsTheMinimumInTheLastRow(t *testing.T) {
	const width, depth, slot = 8, 2, 0
	r := mustRing(t, width, depth, 1)
	y := splitmix64(1)
	x := hashSharingOnlyRow(t, y, width, depth, 0)
	ys, xs := cellIndexes(y, width, depth), cellIndexes(x, width, depth)
	r.Add(slot, y)
	r.Add(slot, y)

	if estimate, saturated := r.Add(slot, x); estimate != 1 || saturated {
		t.Fatalf("Add(x) = (%d, %t), want (1, false)", estimate, saturated)
	}
	requireCells(t, "x after y twice", "slot", r.AppendSlotCells(nil, slot),
		cellsWith(width, depth, map[uint64]uint32{xs[0]: 2, xs[1]: 1, ys[1]: 2}))
	requireCells(t, "x after y twice", "aggregate", r.AppendAggregateCells(nil),
		cellsWith(width, depth, map[uint64]uint64{xs[0]: 2, xs[1]: 1, ys[1]: 2}))
	assertAggregateIsSumOfSlots(t, r)
}

// TestRingAddTakesTheMinimumFromTheSlotNotTheAggregate is the case a minimum
// read from the aggregate gets wrong. y is hot in slot 0 and shares row 0 with
// x. Counting x once in slot 1 must raise every row of slot 1, the shared one
// too, although the aggregate is already high there: once slot 0 expires that
// row holds only what slot 1 put in it.
func TestRingAddTakesTheMinimumFromTheSlotNotTheAggregate(t *testing.T) {
	const width, depth = 8, 3
	r := mustRing(t, width, depth, 2)
	y := splitmix64(1)
	x := hashSharingOnlyRow(t, y, width, depth, 0)
	ys, xs := cellIndexes(y, width, depth), cellIndexes(x, width, depth)
	for range 4 {
		r.Add(0, y)
	}

	if estimate, _ := r.Add(1, x); estimate != 1 {
		t.Fatalf("Add(x) = %d, want 1", estimate)
	}
	requireCells(t, "x in slot 1", "slot 1", r.AppendSlotCells(nil, 1),
		cellsWith(width, depth, map[uint64]uint32{xs[0]: 1, xs[1]: 1, xs[2]: 1}))
	requireCells(t, "x in slot 1", "aggregate", r.AppendAggregateCells(nil),
		cellsWith(width, depth, map[uint64]uint64{ys[0]: 5, ys[1]: 4, ys[2]: 4, xs[1]: 1, xs[2]: 1}))

	r.Expire(0)

	requireCells(t, "slot 0 expired", "aggregate", r.AppendAggregateCells(nil),
		cellsWith(width, depth, map[uint64]uint64{xs[0]: 1, xs[1]: 1, xs[2]: 1}))
	if got := r.Count(x); got != 1 {
		t.Fatalf("Count(x) after slot 0 expired = %d, want 1", got)
	}
	assertAggregateIsSumOfSlots(t, r)
}

// overestimates feeds the same stream to a ring and to the classic reference,
// checks both pointwise bounds for every distinct key, and returns the mean
// overestimate of each over those keys.
func overestimates(
	t *testing.T, width, depth, slots, eventsPerSlot int, draw func() uint64,
) (conservative, classic float64) {
	t.Helper()
	r := mustRing(t, width, depth, slots)
	ref := newClassicSlots(t, width, depth, slots)
	exact := make(map[uint64]uint64)
	for slot := range slots {
		for range eventsPerSlot {
			h := splitmix64(draw())
			r.Add(slot, h)
			ref.add(slot, h)
			exact[h]++
		}
	}
	var sumConservative, sumClassic uint64
	for h, want := range exact {
		got, upper := r.Count(h), ref.estimate(h)
		if got < want || got > upper {
			t.Fatalf("Count = %d, want between the exact %d and the classic %d", got, want, upper)
		}
		sumConservative += got - want
		sumClassic += upper - want
	}
	keys := float64(len(exact))
	return float64(sumConservative) / keys, float64(sumClassic) / keys
}

// TestConservativeUpdateCutsTheOverestimateOfAUniformStream compares the mean
// overestimate with the classic update's on a ring loaded well beyond its
// width: 4,000 keys drawn uniformly, 8 slots of 64 x 4, at about 4 and about 40
// events per column per slot. The measured ratios for these seeds are 0.55 to
// 0.57 at the lower load and 0.51 at the higher; 0.65 leaves room for others.
func TestConservativeUpdateCutsTheOverestimateOfAUniformStream(t *testing.T) {
	const width, depth, slots, keys, maxRatio = 64, 4, 8, 4_000, 0.65
	for _, eventsPerSlot := range []int{237, 2_370} {
		for seed := uint64(1); seed <= 5; seed++ {
			rng := rand.New(rand.NewPCG(seed, uint64(eventsPerSlot)))
			conservative, classic := overestimates(t, width, depth, slots, eventsPerSlot,
				func() uint64 { return rng.Uint64N(keys) })
			if ratio := conservative / classic; ratio > maxRatio {
				t.Errorf("%d events per slot, seed %d: mean overestimate %.2f, classic %.2f, ratio %.3f above %.2f",
					eventsPerSlot, seed, conservative, classic, ratio, maxRatio)
			}
		}
	}
}

// TestConservativeUpdateLowersTheOverestimateOfAZipfStream is the skewed
// counterpart, Zipf with s = 1.1 over 4,000 keys at about 4 events per column
// per slot, where the gain per key is smaller: the measured ratios for these
// seeds are 0.62 to 0.65, so only 0.8 is asserted.
func TestConservativeUpdateLowersTheOverestimateOfAZipfStream(t *testing.T) {
	const width, depth, slots, keys, eventsPerSlot, maxRatio = 64, 4, 8, 4_000, 237, 0.8
	for seed := uint64(1); seed <= 5; seed++ {
		zipf := rand.NewZipf(rand.New(rand.NewPCG(seed, 11)), 1.1, 1, keys-1)
		conservative, classic := overestimates(t, width, depth, slots, eventsPerSlot, zipf.Uint64)
		if ratio := conservative / classic; ratio > maxRatio {
			t.Errorf("seed %d: mean overestimate %.2f, classic %.2f, ratio %.3f above %.2f",
				seed, conservative, classic, ratio, maxRatio)
		}
	}
}
