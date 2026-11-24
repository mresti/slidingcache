package countmin

import (
	"fmt"
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
// while x, which shares one row with y and no other row, is counted after y.
// The rows at the minimum climb, in the slot and in the aggregate, while the
// shared row above it waits; once they catch up, all of them move together. At
// depth 5 each row in turn is the shared one, which pins every row's index,
// minimum and raise of the unrolled update; depths 4, 6 and MaxDepth pin the
// generic update on both sides of it.
func TestRingAddRaisesOnlyTheRowsAtTheSlotMinimum(t *testing.T) {
	cases := []struct{ depth, sharedRow int }{{3, 0}, {4, 3}, {6, 5}, {MaxDepth, MaxDepth - 1}}
	for row := range 5 {
		cases = append(cases, struct{ depth, sharedRow int }{5, row})
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("depth=%d/shared_row=%d", c.depth, c.sharedRow), func(t *testing.T) {
			const width, slot = 8, 1
			r := mustRing(t, width, c.depth, 2)
			y := splitmix64(1)
			x := hashSharingOnlyRow(t, y, width, c.depth, c.sharedRow)
			ys, xs := cellIndexes(y, width, c.depth), cellIndexes(x, width, c.depth)
			r.Add(slot, y)
			r.Add(slot, y)

			steps := []struct {
				name           string
				shared, others uint32
				estimate       uint64
			}{
				{"first x: the rows at 0 rise, the shared row at 2 stays", 2, 1, 1},
				{"second x: the rows at 1 rise, the shared row at 2 stays", 2, 2, 2},
				{"third x: every row is at the minimum and rises", 3, 3, 3},
			}
			for _, s := range steps {
				estimate, saturated := r.Add(slot, x)

				want := make(map[uint64]uint32)
				for i, j := range ys {
					if i != c.sharedRow {
						want[j] = 2
					}
				}
				for i, j := range xs {
					want[j] = s.others
					if i == c.sharedRow {
						want[j] = s.shared
					}
				}
				requireSlotAndAggregate(t, s.name, r, slot, width, c.depth, want)
				if estimate != s.estimate || saturated {
					t.Fatalf("%s: Add = (%d, %t), want (%d, false)", s.name, estimate, saturated, s.estimate)
				}
				assertAggregateIsSumOfSlots(t, r)
			}
			if got := r.Count(y); got != 2 {
				t.Fatalf("Count(y) = %d, want its 2 adds: y's own rows were never raised by x", got)
			}
		})
	}
}

// requireSlotAndAggregate checks every cell of a ring with a single non-empty
// slot: that slot holds want, the aggregate the same counts, every other cell 0.
func requireSlotAndAggregate(
	t *testing.T, step string, r *Ring, slot int, width uint64, depth int, want map[uint64]uint32,
) {
	t.Helper()
	requireCells(t, step, "slot", r.AppendSlotCells(nil, slot), cellsWith(width, depth, want))
	wantAggregate := make(map[uint64]uint64, len(want))
	for j, c := range want {
		wantAggregate[j] = uint64(c)
	}
	requireCells(t, step, "aggregate", r.AppendAggregateCells(nil), cellsWith(width, depth, wantAggregate))
}

// TestRingAddFindsTheMinimumInItsOnlyRow is the case where one row alone holds
// the minimum: x shares every other row with y, counted twice, so x's cells are
// 2 except a 0 in that row, and only that row may rise. At depth 5 each row in
// turn, which pins every row's part in the minimum of the unrolled update; at
// the other depths the last row, so a path that ignored rows past the fifth
// would get the cells wrong. Narrower widths keep x easy to find when it must
// share many rows.
func TestRingAddFindsTheMinimumInItsOnlyRow(t *testing.T) {
	type minimumCase struct {
		width         uint64
		depth, minRow int
	}
	cases := []minimumCase{{8, 2, 1}, {8, 4, 3}, {4, 6, 5}, {2, MaxDepth, MaxDepth - 1}}
	for row := range 5 {
		cases = append(cases, minimumCase{8, 5, row})
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%dx%d/minimum_row=%d", c.width, c.depth, c.minRow), func(t *testing.T) {
			const slot = 0
			width := c.width
			r := mustRing(t, int(width), c.depth, 1)
			y := splitmix64(1)
			x := hashSharingRows(t, y, width, c.depth, func(row int) bool { return row != c.minRow })
			ys, xs := cellIndexes(y, width, c.depth), cellIndexes(x, width, c.depth)
			r.Add(slot, y)
			r.Add(slot, y)

			if estimate, saturated := r.Add(slot, x); estimate != 1 || saturated {
				t.Fatalf("Add(x) = (%d, %t), want (1, false)", estimate, saturated)
			}
			want := map[uint64]uint32{ys[c.minRow]: 2}
			for i, j := range xs {
				want[j] = 2
				if i == c.minRow {
					want[j] = 1
				}
			}
			requireSlotAndAggregate(t, "x after y twice", r, slot, width, c.depth, want)
			assertAggregateIsSumOfSlots(t, r)
		})
	}
}

// TestRingAddTakesTheMinimumFromTheSlotNotTheAggregate is the case a minimum
// read from the aggregate gets wrong. y is hot in slot 0 and shares row 0 with
// x. Counting x once in slot 1 must raise every row of slot 1, the shared one
// too, although the aggregate is already high there: once slot 0 expires that
// row holds only what slot 1 put in it.
func TestRingAddTakesTheMinimumFromTheSlotNotTheAggregate(t *testing.T) {
	for _, depth := range []int{3, 5} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			const width = 8
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
			wantSlot1 := make(map[uint64]uint32)
			wantAggregate := map[uint64]uint64{ys[0]: 5}
			for i, j := range xs {
				wantSlot1[j] = 1
				if i > 0 {
					wantAggregate[ys[i]] = 4
					wantAggregate[j] = 1
				}
			}
			requireCells(t, "x in slot 1", "slot 1",
				r.AppendSlotCells(nil, 1), cellsWith(width, depth, wantSlot1))
			requireCells(t, "x in slot 1", "aggregate",
				r.AppendAggregateCells(nil), cellsWith(width, depth, wantAggregate))

			r.Expire(0)

			requireSlotAndAggregate(t, "slot 0 expired", r, 1, width, depth, wantSlot1)
			if got := r.Count(x); got != 1 {
				t.Fatalf("Count(x) after slot 0 expired = %d, want 1", got)
			}
			assertAggregateIsSumOfSlots(t, r)
		})
	}
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
