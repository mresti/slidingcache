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

// fuzzWindow is a sliding window in miniature: a ring of slots and the exact
// count of every key in every slot.
type fuzzWindow struct {
	ring    *Ring
	exact   []map[uint64]uint64
	current int
}

const fuzzWindowSlots = 4

func newFuzzWindow(t *testing.T) *fuzzWindow {
	w := &fuzzWindow{ring: mustRing(t, 8, 3, fuzzWindowSlots)}
	for range fuzzWindowSlots {
		w.exact = append(w.exact, make(map[uint64]uint64))
	}
	return w
}

func (w *fuzzWindow) add(h uint64) {
	w.ring.Add(w.current, h)
	w.exact[w.current][h]++
}

// expire advances to the next slot, dropping the oldest one from the window.
func (w *fuzzWindow) expire() {
	w.current = (w.current + 1) % fuzzWindowSlots
	w.ring.Expire(w.current)
	clear(w.exact[w.current])
}

func (w *fuzzWindow) reset() {
	w.ring.Reset()
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

// FuzzRingWindow drives a ring of slots through adds, expiries and resets. The
// top two bits of each byte choose the operation and the low four the key.
func FuzzRingWindow(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x80, 0x03, 0xc0, 0x01})
	f.Add([]byte{0x00, 0x00, 0x00, 0x80, 0x80, 0x80, 0x80, 0x00})
	f.Add([]byte{0x05, 0x45, 0x85, 0xc5, 0x05, 0x45, 0x80, 0x80})
	f.Fuzz(func(t *testing.T, ops []byte) {
		w := newFuzzWindow(t)
		for _, op := range ops {
			switch op >> 6 {
			case 0, 1:
				w.add(splitmix64(uint64(op & 0x0f)))
			case 2:
				w.expire()
			default:
				w.reset()
			}
			assertAggregateIsSumOfSlots(t, w.ring)
			w.assertCountsBoundLiveEvents(t)
		}
	})
}
