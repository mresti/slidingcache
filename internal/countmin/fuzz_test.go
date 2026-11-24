package countmin

import (
	"strconv"
	"testing"
)

// FuzzSketchAgainstExactCounts drives a small sketch through maphash-hashed
// items. Each byte picks one of sixteen items and, from its high bits, how many
// occurrences to add; only properties that hold for every seed are asserted.
func FuzzSketchAgainstExactCounts(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{0xff, 0x0f, 0x10, 0x00, 0x00, 0x7a})
	f.Add([]byte("a long stream of mostly distinct bytes to collide in eight cells"))
	f.Fuzz(func(t *testing.T, ops []byte) {
		s := mustSketch(t, 8, 2)
		exact := make(map[string]uint64)
		var total uint64
		for _, op := range ops {
			item := strconv.Itoa(int(op & 0x0f))
			n := uint64(op >> 4)
			s.AddN([]byte(item), n)
			exact[item] += n
			total += n
		}
		if got := s.Total(); got != total {
			t.Fatalf("Total = %d, want %d", got, total)
		}
		for item, want := range exact {
			got := s.Count([]byte(item))
			if got < want || got > total {
				t.Fatalf("Count(%s) = %d, want between %d and Total %d", item, got, want, total)
			}
		}
	})
}

// fuzzWindow is a sliding window in miniature: a ring of slots, the exact
// count of every key in every slot, and the classic reference fed the same
// operations.
type fuzzWindow struct {
	ring    *Ring
	exact   []map[uint64]uint64
	classic *classicSlots
	current int
	// slotCells and aggCells are reused buffers for the ring's cells.
	slotCells []uint32
	aggCells  []uint64
}

const fuzzWindowSlots = 4

// fuzzShapes are the ring shapes FuzzRingWindow picks from: 8x3 and 4x6 run
// the generic update below and above the unrolled depth 5, and the narrow 4x5
// runs the unrolled one; at width 4, 16 keys collide in every combination of
// rows.
var fuzzShapes = []struct{ width, depth int }{
	{8, 3},
	{4, 5},
	{4, 6},
}

// Prefixes of a FuzzRingWindow input that select each of fuzzShapes.
const (
	fuzzShape8x3 = 0
	fuzzShape4x5 = 1
	fuzzShape4x6 = 2
)

func newFuzzWindow(t *testing.T, width, depth int) *fuzzWindow {
	w := &fuzzWindow{
		ring:    mustRing(t, width, depth, fuzzWindowSlots),
		classic: newClassicSlots(t, width, depth, fuzzWindowSlots),
	}
	for range fuzzWindowSlots {
		w.exact = append(w.exact, make(map[uint64]uint64))
	}
	return w
}

func (w *fuzzWindow) add(h uint64) {
	w.ring.Add(w.current, h)
	w.classic.add(w.current, h)
	w.exact[w.current][h]++
}

// expire advances to the next slot, dropping the oldest one from the window.
func (w *fuzzWindow) expire() {
	w.current = (w.current + 1) % fuzzWindowSlots
	w.ring.Expire(w.current)
	w.classic.expire(w.current)
	clear(w.exact[w.current])
}

func (w *fuzzWindow) reset() {
	w.ring.Reset()
	w.classic.reset()
	for _, counts := range w.exact {
		clear(counts)
	}
}

// assertCountsBoundLiveEvents checks the count-min guarantee against the exact
// model: no estimate below a key's live events, none above all live events.
func (w *fuzzWindow) assertCountsBoundLiveEvents(t *testing.T) {
	t.Helper()
	live := make(map[uint64]uint64)
	var total uint64
	for _, counts := range w.exact {
		for h, c := range counts {
			live[h] += c
			total += c
		}
	}
	for h, want := range live {
		if got := w.ring.Count(h); got < want || got > total {
			t.Fatalf("Count = %d, want between the %d live events of the key and the %d of all keys", got, want, total)
		}
	}
}

// assertCellsBelowClassicAndSummed reads the ring through its public views
// only: every slot cell is at most the classic slot's, every aggregate cell is
// the sum of the slots' and at most the classic aggregate's.
func (w *fuzzWindow) assertCellsBelowClassicAndSummed(t *testing.T) {
	t.Helper()
	w.aggCells = w.ring.AppendAggregateCells(w.aggCells[:0])
	sum := make([]uint64, len(w.aggCells))
	for slot, classic := range w.classic.slots {
		w.slotCells = w.ring.AppendSlotCells(w.slotCells[:0], slot)
		for j, cell := range w.slotCells {
			if cell > classic.cells[j] {
				t.Fatalf("slot %d cell %d = %d, above the classic %d", slot, j, cell, classic.cells[j])
			}
			sum[j] += uint64(cell)
		}
	}
	classicAggregate := w.classic.aggregateCells()
	for j, got := range w.aggCells {
		if got != sum[j] {
			t.Fatalf("aggregate cell %d = %d, the slots sum to %d", j, got, sum[j])
		}
		if got > classicAggregate[j] {
			t.Fatalf("aggregate cell %d = %d, above the classic %d", j, got, classicAggregate[j])
		}
	}
}

// FuzzRingWindow drives a ring of slots through adds, expiries and resets. The
// first byte picks one of fuzzShapes; in each later byte the top two bits
// choose the operation and the low four the key. After every operation it
// checks properties only: estimates at least the exact live count, cells at
// most the classic update's, and the aggregate the sum of the slots.
func FuzzRingWindow(f *testing.F) {
	for _, shape := range []byte{fuzzShape8x3, fuzzShape4x5, fuzzShape4x6} {
		f.Add([]byte{shape, 0x01, 0x02, 0x80, 0x03, 0xc0, 0x01})
		f.Add([]byte{shape, 0x00, 0x00, 0x00, 0x80, 0x80, 0x80, 0x80, 0x00})
		f.Add([]byte{shape, 0x05, 0x45, 0x85, 0xc5, 0x05, 0x45, 0x80, 0x80})
		// Keys 0 and 6 share exactly row 1, at 8x3, 4x5 and 4x6: 0 is hot in
		// slot 0, 6 is counted once in slot 1, then slot 0 expires. A minimum
		// read from the aggregate would leave the shared row of slot 1 at 0
		// and count 6 as 0.
		f.Add([]byte{shape, 0x00, 0x00, 0x00, 0x80, 0x06, 0x80, 0x80, 0x80})
	}
	// Every key once at 4x5, then an expiry and a few keys again: the rows
	// collide in many combinations at once.
	f.Add([]byte{
		fuzzShape4x5, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x80, 0x0f, 0x0e, 0x0d,
	})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 {
			return
		}
		shape := fuzzShapes[int(ops[0])%len(fuzzShapes)]
		w := newFuzzWindow(t, shape.width, shape.depth)
		for _, op := range ops[1:] {
			switch op >> 6 {
			case 0, 1:
				w.add(splitmix64(uint64(op & 0x0f)))
			case 2:
				w.expire()
			default:
				w.reset()
			}
			w.assertCellsBelowClassicAndSummed(t)
			w.assertCountsBoundLiveEvents(t)
		}
	})
}
