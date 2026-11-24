package countmin

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// fillSlots counts events random keys into random slots among the given ones.
func fillSlots(r *Ring, slots []int, rng *rand.Rand, keys, events uint64) {
	for range events {
		r.Add(slots[rng.IntN(len(slots))], splitmix64(rng.Uint64N(keys)))
	}
}

func everySlot(r *Ring) []int {
	slots := make([]int, r.Slots())
	for i := range slots {
		slots[i] = i
	}
	return slots
}

func requireAllZero[C uint32 | uint64](t *testing.T, what string, cells []C) {
	t.Helper()
	for j, c := range cells {
		if c != 0 {
			t.Fatalf("%s cell %d = %d, want 0", what, j, c)
		}
	}
}

func TestNewRingAllocatesEverySlotEmpty(t *testing.T) {
	r := mustRing(t, 37, 6, 5)
	if r.Slots() != 5 {
		t.Fatalf("Slots() = %d, want 5", r.Slots())
	}
	for slot := range r.Slots() {
		cells := r.AppendSlotCells(nil, slot)
		if len(cells) != 37*6 {
			t.Fatalf("slot %d has %d cells, want 37 x 6", slot, len(cells))
		}
		requireAllZero(t, "slot "+strconv.Itoa(slot), cells)
	}
	aggregate := r.AppendAggregateCells(nil)
	if len(aggregate) != 37*6 {
		t.Fatalf("aggregate has %d cells, want 37 x 6", len(aggregate))
	}
	requireAllZero(t, "aggregate", aggregate)
	if got := r.Count(splitmix64(1)); got != 0 {
		t.Fatalf("Count on a new ring = %d, want 0", got)
	}
}

func TestNewRingRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name                string
		width, depth, slots int
	}{
		{"zero width", 0, 5, 1},
		{"negative width", -1, 5, 1},
		{"zero depth", 10, 0, 1},
		{"depth above MaxDepth", 10, MaxDepth + 1, 1},
		{"zero slots", 10, 5, 0},
		{"negative slots", 10, 5, -1},
		{"slot cells overflowing an int", 2719, 5, math.MaxInt},
		{"one more slot than an int's worth of bytes", 1, 1, math.MaxInt/sketchCellBytes + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewRing(tt.width, tt.depth, tt.slots)
			if err == nil || r != nil {
				t.Fatalf("NewRing(%d, %d, %d) = (%v, %v), want an error and no ring",
					tt.width, tt.depth, tt.slots, r, err)
			}
			if !strings.HasPrefix(err.Error(), "countmin: ") {
				t.Fatalf("error %q lacks the package prefix", err)
			}
		})
	}
}

// TestRingMemoryLimitOfA32BitInt exercises the size check with the limit a
// 32-bit int imposes, where realistic windows can already exceed it.
func TestRingMemoryLimitOfA32BitInt(t *testing.T) {
	const maxInt32 = math.MaxInt32
	if err := checkRingMemory(2719*5, 1<<20, maxInt32); err == nil {
		t.Fatal("2^20 slots of 2719 x 5 cells fit in a 32-bit int, want an error")
	}
	if err := checkRingMemory(1<<20, 1<<9, maxInt32); err == nil {
		t.Fatal("2^31 bytes fit in a 32-bit int, want an error")
	}
	if err := checkRingMemory(1<<20, 1<<9-1, maxInt32); err != nil {
		t.Fatalf("2^31 - 2^22 bytes rejected for a 32-bit int: %v", err)
	}
}

func TestRingAggregateIsTheSumOfItsSlots(t *testing.T) {
	r := mustRing(t, 40, 4, 5)
	fillSlots(r, everySlot(r), rand.New(rand.NewPCG(5, 6)), 300, 10_000)
	assertAggregateIsSumOfSlots(t, r)
}

func TestRingAddReturnsTheUpdatedEstimate(t *testing.T) {
	r := mustRing(t, 16, 3, 3)
	rng := rand.New(rand.NewPCG(13, 14))
	for range 2_000 {
		h := splitmix64(rng.Uint64N(100))
		estimate, saturated := r.Add(rng.IntN(r.Slots()), h)
		if saturated {
			t.Fatal("Add reported saturation far from the cell limit")
		}
		if got := r.Count(h); got != estimate {
			t.Fatalf("Add returned %d, Count right after = %d", estimate, got)
		}
	}
}

func TestRingSingleKeyGrowsByOnePerAdd(t *testing.T) {
	const width, depth = 64, 5
	r := mustRing(t, width, depth, 2)
	h := splitmix64(42)
	for want := uint64(1); want <= 10; want++ {
		estimate, _ := r.Add(int(want%2), h)
		if estimate != want {
			t.Fatalf("estimate after %d adds = %d", want, estimate)
		}
	}
	for slot := range r.Slots() {
		cells := r.AppendSlotCells(nil, slot)
		for _, j := range cellIndexes(h, width, depth) {
			if cells[j] != 5 {
				t.Fatalf("slot %d cell %d = %d, want 5", slot, j, cells[j])
			}
		}
	}
}

// TestRingAddSaturatesOnlyAtTheSlotMinimum plants h's cells in one slot, over
// another slot that adds 7 to each aggregate cell, and checks that saturation
// is decided by the smallest of h's slot cells alone.
func TestRingAddSaturatesOnlyAtTheSlotMinimum(t *testing.T) {
	const width, depth, slot, below = 16, 3, 1, 7
	const top = math.MaxUint32
	tests := []struct {
		name          string
		planted, want [depth]uint32
		saturated     bool
	}{
		{
			name:    "minimum one below the top is raised",
			planted: [depth]uint32{top - 1, top - 1, top},
			want:    [depth]uint32{top, top, top},
		},
		{
			name:      "minimum at the top moves nothing",
			planted:   [depth]uint32{top, top, top},
			want:      [depth]uint32{top, top, top},
			saturated: true,
		},
		{
			name:    "a row at the top above a lower minimum",
			planted: [depth]uint32{top, 5, 6},
			want:    [depth]uint32{top, 6, 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mustRing(t, width, depth, 2)
			h := splitmix64(99)
			rows := cellIndexes(h, width, depth)
			for i, j := range rows {
				plantSlotCell(r, 0, j, below)
				plantSlotCell(r, slot, j, tt.planted[i])
			}

			estimate, saturated := r.Add(slot, h)

			if saturated != tt.saturated {
				t.Fatalf("saturated = %t, want %t", saturated, tt.saturated)
			}
			wantEstimate := uint64(math.MaxUint64)
			for i, j := range rows {
				if got := *slotCell(r, slot, j); got != tt.want[i] {
					t.Fatalf("row %d slot cell = %d, want %d", i, got, tt.want[i])
				}
				wantEstimate = min(wantEstimate, uint64(tt.want[i])+below)
			}
			if estimate != wantEstimate || r.Count(h) != wantEstimate {
				t.Fatalf("estimate = %d, Count = %d, want %d", estimate, r.Count(h), wantEstimate)
			}
			assertAggregateIsSumOfSlots(t, r)
		})
	}
}

// TestRingAddRaisesTheLastCountBeforeSaturation pins the boundary of
// saturation: a slot cell one below the maximum still takes the event, mirrored
// in the aggregate, and only the next Add finds it saturated.
func TestRingAddRaisesTheLastCountBeforeSaturation(t *testing.T) {
	const width, depth, slot = 16, 1, 1
	r := mustRing(t, width, depth, 2)
	h := splitmix64(7)
	cell := cellIndexes(h, width, depth)[0]
	*slotCell(r, slot, cell) = math.MaxUint32 - 1
	r.agg[cell] = math.MaxUint32 - 1

	if estimate, saturated := r.Add(slot, h); saturated || estimate != math.MaxUint32 {
		t.Fatalf("Add at MaxUint32-1 = (%d, %t), want (%d, false)", estimate, saturated, uint64(math.MaxUint32))
	}
	if *slotCell(r, slot, cell) != math.MaxUint32 {
		t.Fatalf("slot cell = %d after the last raise, want %d", *slotCell(r, slot, cell), uint64(math.MaxUint32))
	}

	if estimate, saturated := r.Add(slot, h); !saturated || estimate != math.MaxUint32 {
		t.Fatalf("Add at MaxUint32 = (%d, %t), want (%d, true)", estimate, saturated, uint64(math.MaxUint32))
	}
	if *slotCell(r, slot, cell) != math.MaxUint32 || r.agg[cell] != math.MaxUint32 {
		t.Fatalf("saturated Add moved the cells: slot %d, aggregate %d", *slotCell(r, slot, cell), r.agg[cell])
	}
	assertAggregateIsSumOfSlots(t, r)
}

func TestRingExpireUndoesTheAddsOfItsSlot(t *testing.T) {
	r := mustRing(t, 40, 4, 2)
	fillSlots(r, []int{0}, rand.New(rand.NewPCG(7, 8)), 200, 3_000)
	onlyA := r.AppendAggregateCells(nil)
	slotA := r.AppendSlotCells(nil, 0)

	fillSlots(r, []int{1}, rand.New(rand.NewPCG(9, 10)), 200, 3_000)
	assertAggregateIsSumOfSlots(t, r)
	r.Expire(1)

	if !slices.Equal(r.AppendAggregateCells(nil), onlyA) {
		t.Fatal("(A+B)-B differs from A")
	}
	if !slices.Equal(r.AppendSlotCells(nil, 0), slotA) {
		t.Fatal("expiring slot 1 changed slot 0")
	}
	requireAllZero(t, "expired slot", r.AppendSlotCells(nil, 1))
}

// TestRingExpireForgetsTheSlotEntirely interleaves adds over every slot, then
// compares the ring after expiring one slot with a ring that never saw that
// slot's adds.
func TestRingExpireForgetsTheSlotEntirely(t *testing.T) {
	expired, never := mustRing(t, 40, 4, 6), mustRing(t, 40, 4, 6)
	rng := rand.New(rand.NewPCG(11, 12))
	for range 8_000 {
		slot, h := rng.IntN(6), splitmix64(rng.Uint64N(300))
		expired.Add(slot, h)
		if slot != 0 {
			never.Add(slot, h)
		}
	}
	expired.Expire(0)
	if !slices.Equal(expired.slots, never.slots) || !slices.Equal(expired.agg, never.agg) {
		t.Fatal("ring after Expire differs from one that never counted into the slot")
	}
}

func TestRingExpiredSlotCountsFromZero(t *testing.T) {
	r := mustRing(t, 64, 5, 2)
	h := splitmix64(7)
	for range 3 {
		r.Add(0, h)
	}
	r.Expire(0)
	if estimate, _ := r.Add(0, h); estimate != 1 {
		t.Fatalf("estimate after reusing an expired slot = %d, want 1", estimate)
	}
}

func TestRingReset(t *testing.T) {
	r := mustRing(t, 16, 3, 2)
	fillSlots(r, everySlot(r), rand.New(rand.NewPCG(15, 16)), 50, 500)
	r.Reset()
	requireAllZero(t, "ring slots", r.slots)
	requireAllZero(t, "aggregate", r.AppendAggregateCells(nil))
	if got := r.Count(splitmix64(1)); got != 0 {
		t.Fatalf("Count after Reset = %d, want 0", got)
	}
}

func TestRingAppendCellsCopiesAfterDst(t *testing.T) {
	const width, depth = 16, 3
	r := mustRing(t, width, depth, 2)
	h := splitmix64(3)
	r.Add(1, h)

	slotCells := r.AppendSlotCells([]uint32{9}, 1)
	aggCells := r.AppendAggregateCells([]uint64{9})
	if len(slotCells) != 1+width*depth || slotCells[0] != 9 || len(aggCells) != 1+width*depth || aggCells[0] != 9 {
		t.Fatal("cells were not appended after the existing elements of dst")
	}
	for _, j := range cellIndexes(h, width, depth) {
		if slotCells[1+j] != 1 || aggCells[1+j] != 1 {
			t.Fatalf("cell %d = %d in the slot and %d in the aggregate, want 1", j, slotCells[1+j], aggCells[1+j])
		}
		slotCells[1+j], aggCells[1+j] = 0, 0
	}
	if r.Count(h) != 1 || r.AppendSlotCells(nil, 1)[cellIndexes(h, width, depth)[0]] != 1 {
		t.Fatal("writing to the appended cells changed the ring")
	}
}

func TestRingPanicsOnASlotOutsideTheRing(t *testing.T) {
	r := mustRing(t, 16, 3, 4)
	operations := map[string]func(slot int){
		"Add":             func(slot int) { r.Add(slot, splitmix64(1)) },
		"Expire":          func(slot int) { r.Expire(slot) },
		"AppendSlotCells": func(slot int) { r.AppendSlotCells(nil, slot) },
	}
	for name, op := range operations {
		for _, slot := range []int{-1, r.Slots(), math.MaxInt, math.MinInt} {
			assertSlotPanics(t, name, slot, op)
		}
	}
	assertAggregateIsSumOfSlots(t, r)
}

func assertSlotPanics(t *testing.T, name string, slot int, op func(int)) {
	t.Helper()
	defer func() {
		msg, ok := recover().(string)
		if !ok || !strings.Contains(msg, "outside the ring") {
			t.Errorf("%s(slot %d): recovered %q, want the out-of-range panic", name, slot, msg)
		}
	}()
	op(slot)
}
