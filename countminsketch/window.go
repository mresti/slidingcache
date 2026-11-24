package countminsketch

import (
	"fmt"

	"github.com/mresti/slidingcache/internal/countmin"
)

// window is the sliding window of sketches: a ring with one slot per Precision
// bucket and an aggregate holding the cell-wise sum of the live slots, so an
// estimate for the whole window reads a single sketch's worth of cells.
//
// It keeps three invariants, which the tests check after every operation:
//
//	I1  agg is the cell-wise sum of the live slots.
//	I2  a slot is live if and only if applied < timestamp <= HW, and it sits
//	    at index(timestamp).
//	I3  applied never decreases.
//
// I2 is what makes the ring big enough: live timestamps are multiples of
// Precision in (HW - WindowSize, HW], at most len(slots) consecutive ones, so no
// two live buckets share an index.
//
// A window is not safe for concurrent use; the Cache guards it with its mutex.
type window struct {
	slots     []slot
	agg       *countmin.Aggregate
	precision int64
	// applied is the cutoff whose expiry has already been applied: every slot
	// at or below it has left the aggregate.
	applied int64
}

// slot is one Precision bucket of the window. Its sketch is allocated the first
// time the slot is used and kept for every later bucket that maps onto it.
type slot struct {
	timestamp int64
	sketch    *countmin.Sketch
	live      bool
}

func newWindow(bucketCount, precision int64, agg *countmin.Aggregate, applied int64) *window {
	return &window{
		slots:     make([]slot, bucketCount),
		agg:       agg,
		precision: precision,
		applied:   applied,
	}
}

// expireTo removes from the aggregate every live slot at or below cutoff. A
// cutoff at or below the one already applied changes nothing, so a Get holding
// a stale cutoff sees the newer state, which is a valid ordering of the two.
//
// It scans the whole ring rather than the distance between applied and cutoff:
// applied starts near math.MinInt64, where that distance would overflow.
func (w *window) expireTo(cutoff int64) {
	if cutoff <= w.applied {
		return
	}
	for i := range w.slots {
		if s := &w.slots[i]; s.live && s.timestamp <= cutoff {
			w.expire(s)
		}
	}
	w.applied = cutoff
}

func (w *window) expire(s *slot) {
	if err := w.agg.Sub(s.sketch); err != nil {
		panic(fmt.Sprintf("countminsketch: slot does not match the window aggregate: %v", err))
	}
	s.sketch.Reset()
	s.live = false
}

// record counts h in the bucket at timestamp and returns the window estimate
// for h afterwards. timestamp must be above the applied cutoff, which Store
// guarantees by expiring to its own cutoff first.
func (w *window) record(timestamp int64, h uint64) (estimate uint64, saturated bool) {
	return w.agg.Add(w.liveSlot(timestamp).sketch, h)
}

func (w *window) estimate(h uint64) uint64 {
	return w.agg.Count(h)
}

// liveSlot returns the slot holding timestamp, making it live on first use. A
// live slot tagged with another timestamp would mean I2 is broken; retagging it
// would silently fold two buckets together, so it panics instead.
func (w *window) liveSlot(timestamp int64) *slot {
	s := &w.slots[w.index(timestamp)]
	if s.live {
		if s.timestamp != timestamp {
			panic(fmt.Sprintf("countminsketch: slot for bucket %d still holds live bucket %d", timestamp, s.timestamp))
		}
		return s
	}
	if s.sketch == nil {
		s.sketch = w.agg.NewSlot()
	}
	s.timestamp = timestamp
	s.live = true
	return s
}

// index maps a bucket timestamp onto its slot. Bucket timestamps are multiples
// of precision, so the division is exact; the modulo rounds toward negative
// infinity so negative timestamps keep consecutive buckets on consecutive slots.
func (w *window) index(timestamp int64) int {
	n := int64(len(w.slots))
	i := (timestamp / w.precision) % n
	if i < 0 {
		i += n
	}
	return int(i)
}
