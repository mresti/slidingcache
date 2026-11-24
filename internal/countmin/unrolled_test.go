package countmin

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

// twinRings holds two rings of the same shape fed the same operations: unrolled
// counts through Add, which unrolls depth 5, and generic through addGeneric,
// the loop every other depth runs. After every operation both must agree on
// everything observable.
type twinRings struct {
	unrolled, generic *Ring
}

func newTwinRings(t *testing.T, width, depth, slots int) twinRings {
	return twinRings{unrolled: mustRing(t, width, depth, slots), generic: mustRing(t, width, depth, slots)}
}

func (tw twinRings) add(t *testing.T, slot int, h uint64) {
	t.Helper()
	estimate, saturated := tw.unrolled.Add(slot, h)
	wantEstimate, wantSaturated := tw.generic.addGeneric(slot, h)
	if estimate != wantEstimate || saturated != wantSaturated {
		t.Fatalf("Add(%d, %#x) = (%d, %t), the generic update returns (%d, %t)",
			slot, h, estimate, saturated, wantEstimate, wantSaturated)
	}
	tw.requireSameCells(t, fmt.Sprintf("Add(%d, %#x)", slot, h))
}

func (tw twinRings) expire(t *testing.T, slot int) {
	t.Helper()
	tw.unrolled.Expire(slot)
	tw.generic.Expire(slot)
	tw.requireSameCells(t, fmt.Sprintf("Expire(%d)", slot))
}

// plant sets h's cells in slot to values, row by row, in both rings.
func (tw twinRings) plant(slot int, h uint64, values []uint32) {
	for i, j := range cellIndexes(h, tw.unrolled.width, tw.unrolled.depth) {
		plantSlotCell(tw.unrolled, slot, j, values[i])
		plantSlotCell(tw.generic, slot, j, values[i])
	}
}

func (tw twinRings) requireSameCells(t *testing.T, op string) {
	t.Helper()
	for j, got := range tw.unrolled.slots {
		if want := tw.generic.slots[j]; got != want {
			t.Fatalf("after %s: slot cell %d = %d, the generic update holds %d", op, j, got, want)
		}
	}
	for j, got := range tw.unrolled.agg {
		if want := tw.generic.agg[j]; got != want {
			t.Fatalf("after %s: aggregate cell %d = %d, the generic update holds %d", op, j, got, want)
		}
	}
	assertAggregateIsSumOfSlots(t, tw.unrolled)
}

// depth5Plantings returns the cell values, one per row, planted for a key
// before it is counted: the saturation boundary with every row, one row or
// none at the top, and the minimum in each row alone, one below the others
// and far below them.
func depth5Plantings() [][]uint32 {
	const top = math.MaxUint32
	plantings := [][]uint32{
		{top, top, top, top, top},
		{top - 1, top - 1, top - 1, top - 1, top - 1},
	}
	for row := range 5 {
		plantings = append(plantings,
			valuesWithOneRow(row, top, 3),
			valuesWithOneRow(row, top-1, top),
			valuesWithOneRow(row, 2, 3),
			valuesWithOneRow(row, 2, 9),
		)
	}
	return plantings
}

func valuesWithOneRow(row int, value, others uint32) []uint32 {
	values := []uint32{others, others, others, others, others}
	values[row] = value
	return values
}

// TestRingAddAtDepth5MatchesTheGenericUpdate checks the unrolled depth 5
// against the generic loop on a ring narrow enough for keys to collide in
// every combination of rows. Before each burst of random adds and expiries it
// plants one of depth5Plantings under a random key and counts that key, so the
// saturation boundary and each row's minimum go through both implementations.
func TestRingAddAtDepth5MatchesTheGenericUpdate(t *testing.T) {
	const width, depth, slots, keys, burst = 4, 5, 3, 24, 40
	for _, seed := range []uint64{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x5eed))
			tw := newTwinRings(t, width, depth, slots)
			for _, values := range depth5Plantings() {
				slot, h := rng.IntN(slots), splitmix64(rng.Uint64N(keys))
				tw.plant(slot, h, values)
				tw.add(t, slot, h)
				for range burst {
					if rng.IntN(8) == 0 {
						tw.expire(t, rng.IntN(slots))
						continue
					}
					tw.add(t, rng.IntN(slots), splitmix64(rng.Uint64N(keys)))
				}
			}
		})
	}
}
