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

// fuzzRing is a sliding window in miniature: a ring of slots, an aggregate over
// all of them, and the exact count of every key in every slot.
type fuzzRing struct {
	agg     *Aggregate
	slots   []*Sketch
	exact   []map[uint64]uint64
	current int
}

const fuzzRingSlots = 4

func newFuzzRing(t *testing.T) *fuzzRing {
	r := &fuzzRing{agg: mustAggregate(t, 8, 3)}
	for range fuzzRingSlots {
		r.slots = append(r.slots, r.agg.NewSlot())
		r.exact = append(r.exact, make(map[uint64]uint64))
	}
	return r
}

func (r *fuzzRing) add(h uint64) {
	r.agg.Add(r.slots[r.current], h)
	r.exact[r.current][h]++
}

// expire advances to the next slot, dropping the oldest one from the window.
func (r *fuzzRing) expire(t *testing.T) {
	r.current = (r.current + 1) % fuzzRingSlots
	if err := r.agg.Sub(r.slots[r.current]); err != nil {
		t.Fatal(err)
	}
	r.slots[r.current].Reset()
	clear(r.exact[r.current])
}

func (r *fuzzRing) rebuild(t *testing.T) {
	r.agg.Reset()
	for _, slot := range r.slots {
		if err := r.agg.AddSketch(slot); err != nil {
			t.Fatal(err)
		}
	}
}

func (r *fuzzRing) assertCountsCoverLiveEvents(t *testing.T) {
	t.Helper()
	live := make(map[uint64]uint64)
	for _, counts := range r.exact {
		for h, c := range counts {
			live[h] += c
		}
	}
	for h, want := range live {
		if got := r.agg.Count(h); got < want {
			t.Fatalf("Count = %d below the %d live events", got, want)
		}
	}
}

// FuzzAggregateWindow drives a ring of slots through adds, expiries and rebuilds.
// The top two bits of each byte choose the operation and the low four the key.
func FuzzAggregateWindow(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x80, 0x03, 0xc0, 0x01})
	f.Add([]byte{0x00, 0x00, 0x00, 0x80, 0x80, 0x80, 0x80, 0x00})
	f.Add([]byte{0x05, 0x45, 0x85, 0xc5, 0x05, 0x45, 0x80, 0x80})
	f.Fuzz(func(t *testing.T, ops []byte) {
		r := newFuzzRing(t)
		for _, op := range ops {
			switch op >> 6 {
			case 0, 1:
				r.add(splitmix64(uint64(op & 0x0f)))
			case 2:
				r.expire(t)
			default:
				r.rebuild(t)
			}
			assertAggregateIsSumOfSlots(t, r.agg, r.slots)
			r.assertCountsCoverLiveEvents(t)
		}
	})
}
