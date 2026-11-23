package countmin

import (
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// fillSlots counts events random keys into random slots through a.Add.
func fillSlots(a *Aggregate, slots []*Sketch, rng *rand.Rand, keys, events uint64) {
	for range events {
		a.Add(slots[rng.IntN(len(slots))], splitmix64(rng.Uint64N(keys)))
	}
}

func newSlots(a *Aggregate, n int) []*Sketch {
	slots := make([]*Sketch, n)
	for i := range slots {
		slots[i] = a.NewSlot()
	}
	return slots
}

func TestNewSlotSharesTheAggregateDimensions(t *testing.T) {
	a := mustAggregate(t, 37, 6)
	slot := a.NewSlot()
	if slot.width != 37 || slot.depth != 6 || len(slot.cells) != len(a.cells) {
		t.Fatalf("slot is %d x %d with %d cells, want 37 x 6 with %d",
			slot.width, slot.depth, len(slot.cells), len(a.cells))
	}
	if slot.Total() != 0 {
		t.Fatalf("new slot Total = %d, want 0", slot.Total())
	}
}

func TestAggregateIsTheSumOfItsSlots(t *testing.T) {
	a := mustAggregate(t, 40, 4)
	slots := newSlots(a, 5)
	fillSlots(a, slots, rand.New(rand.NewPCG(5, 6)), 300, 10_000)
	assertAggregateIsSumOfSlots(t, a, slots)

	var total uint64
	for _, slot := range slots {
		total += slot.Total()
	}
	if total != 10_000 {
		t.Fatalf("slot totals sum to %d, want 10000", total)
	}
}

func TestSubUndoesAddSketch(t *testing.T) {
	a := mustAggregate(t, 40, 4)
	slots := newSlots(a, 2)
	fillSlots(a, slots[:1], rand.New(rand.NewPCG(7, 8)), 200, 3_000)
	onlyA := slices.Clone(a.cells)

	scratch := mustAggregate(t, 40, 4)
	fillSlots(scratch, slots[1:], rand.New(rand.NewPCG(9, 10)), 200, 3_000)
	if err := a.AddSketch(slots[1]); err != nil {
		t.Fatal(err)
	}
	assertAggregateIsSumOfSlots(t, a, slots)
	if err := a.Sub(slots[1]); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.cells, onlyA) {
		t.Fatal("(A+B)-B differs from A")
	}
}

func TestRebuildMatchesIncrementalAggregate(t *testing.T) {
	a := mustAggregate(t, 40, 4)
	slots := newSlots(a, 6)
	fillSlots(a, slots, rand.New(rand.NewPCG(11, 12)), 300, 8_000)
	if err := a.Sub(slots[0]); err != nil {
		t.Fatal(err)
	}
	slots[0].Reset()

	rebuilt := mustAggregate(t, 40, 4)
	for _, slot := range slots {
		if err := rebuilt.AddSketch(slot); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(rebuilt.cells, a.cells) {
		t.Fatal("aggregate rebuilt with AddSketch differs from the incremental one")
	}
}

func TestAddReturnsTheUpdatedEstimate(t *testing.T) {
	a := mustAggregate(t, 16, 3)
	slots := newSlots(a, 3)
	rng := rand.New(rand.NewPCG(13, 14))
	for range 2_000 {
		h := splitmix64(rng.Uint64N(100))
		estimate, saturated := a.Add(slots[rng.IntN(len(slots))], h)
		if saturated {
			t.Fatal("Add reported saturation far from the cell limit")
		}
		if got := a.Count(h); got != estimate {
			t.Fatalf("Add returned %d, Count right after = %d", estimate, got)
		}
	}
}

func TestSingleKeyGrowsByOnePerAdd(t *testing.T) {
	a := mustAggregate(t, 64, 5)
	slots := newSlots(a, 2)
	h := splitmix64(42)
	for want := uint64(1); want <= 10; want++ {
		estimate, _ := a.Add(slots[want%2], h)
		if estimate != want {
			t.Fatalf("estimate after %d adds = %d", want, estimate)
		}
	}
	if slots[0].CountHash(h) != 5 || slots[1].CountHash(h) != 5 {
		t.Fatalf("slot counts = %d and %d, want 5 each", slots[0].CountHash(h), slots[1].CountHash(h))
	}
}

func TestAggregateAddStopsAtASaturatedSlotCell(t *testing.T) {
	const width, depth = 16, 3
	a := mustAggregate(t, width, depth)
	slot, other := a.NewSlot(), a.NewSlot()
	h := splitmix64(99)
	a.Add(other, h)
	saturatedCell := cellIndexes(h, width, depth)[0]
	slot.cells[saturatedCell] = math.MaxUint32
	a.cells[saturatedCell] += math.MaxUint32
	before := slices.Clone(a.cells)

	_, saturated := a.Add(slot, h)

	if !saturated {
		t.Fatal("Add into a saturated slot cell did not report saturation")
	}
	if slot.cells[saturatedCell] != math.MaxUint32 || a.cells[saturatedCell] != before[saturatedCell] {
		t.Fatalf("saturated row moved: slot %d, aggregate %d -> %d",
			slot.cells[saturatedCell], before[saturatedCell], a.cells[saturatedCell])
	}
	for _, j := range cellIndexes(h, width, depth)[1:] {
		if a.cells[j] != before[j]+1 {
			t.Fatalf("unsaturated row cell %d = %d, want %d", j, a.cells[j], before[j]+1)
		}
	}
	if slot.Total() != 1 {
		t.Fatalf("slot Total = %d, want 1: the event still counts", slot.Total())
	}
	assertAggregateIsSumOfSlots(t, a, []*Sketch{slot, other})
}

func TestAggregateReset(t *testing.T) {
	a := mustAggregate(t, 16, 3)
	slots := newSlots(a, 2)
	fillSlots(a, slots, rand.New(rand.NewPCG(15, 16)), 50, 500)
	a.Reset()
	for j, c := range a.cells {
		if c != 0 {
			t.Fatalf("cell %d = %d after Reset, want 0", j, c)
		}
	}
	if got := a.Count(splitmix64(1)); got != 0 {
		t.Fatalf("Count after Reset = %d, want 0", got)
	}
}

func TestMismatchedSlotsAreRejected(t *testing.T) {
	a := mustAggregate(t, 16, 3)
	for _, other := range []*Sketch{mustSketch(t, 17, 3), mustSketch(t, 16, 4)} {
		if err := a.Sub(other); err == nil {
			t.Errorf("Sub of a %d x %d slot into 16 x 3 succeeded", other.width, other.depth)
		}
		if err := a.AddSketch(other); err == nil {
			t.Errorf("AddSketch of a %d x %d slot into 16 x 3 succeeded", other.width, other.depth)
		}
		assertAddPanics(t, a, other)
	}
	assertAggregateIsSumOfSlots(t, a, nil)
}

func assertAddPanics(t *testing.T, a *Aggregate, slot *Sketch) {
	t.Helper()
	defer func() {
		msg, ok := recover().(string)
		if !ok || !strings.Contains(msg, "dimensions") {
			t.Errorf("Add with a %d x %d slot: recovered %q, want a dimension panic", slot.width, slot.depth, msg)
		}
	}()
	a.Add(slot, splitmix64(1))
}
