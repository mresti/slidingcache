package countminsketch

import (
	"fmt"
	"math"

	"github.com/mresti/slidingcache/internal/countmin"
)

// window is the sliding window of sketches: a countmin.Ring with one slot per
// Precision bucket, whose aggregate holds the cell-wise sum of the live slots,
// so an estimate for the whole window reads a single sketch's worth of cells.
// The window adds the bucket timestamp each slot holds, which is the only
// per-slot state: 8 bytes, no pointer.
//
// It keeps three invariants, which the tests check after every operation:
//
//	I1  the ring's aggregate is the cell-wise sum of the live slots, and every
//	    cell of a dead slot is zero.
//	I2  a live slot's timestamp lies in (applied, applied + WindowSize] and at
//	    or below HW, and the slot sits at index(timestamp).
//	I3  applied never decreases.
//
// applied is HW - WindowSize right after an operation on the window, and lags
// behind it while other shards advance the mark. I2 is what makes the ring big
// enough either way: live timestamps are multiples of Precision in a span of
// WindowSize, at most ring.Slots() consecutive ones, so no two live buckets
// share an index.
//
// A window is not safe for concurrent use; its shard guards it with a mutex.
type window struct {
	ring *countmin.Ring
	// timestamps holds the bucket timestamp of each live slot, and deadSlot
	// for every other.
	timestamps []int64
	precision  int64
	// ringSpan is ring.Slots() buckets of precision seconds: a cutoff that far
	// past applied may expire any slot, so expireTo scans the whole ring.
	ringSpan int64
	// applied is the cutoff whose expiry has already been applied: every slot
	// at or below it has left the aggregate.
	applied int64
	// live counts the slots whose timestamp is not deadSlot.
	live int
}

// deadSlot marks a slot holding no bucket. It can never be a bucket
// timestamp: the accepted range tops out at +-2^62.
const deadSlot = math.MinInt64

func newWindow(ring *countmin.Ring, precision, applied int64) window {
	timestamps := make([]int64, ring.Slots())
	for i := range timestamps {
		timestamps[i] = deadSlot
	}
	return window{
		ring:       ring,
		timestamps: timestamps,
		precision:  precision,
		ringSpan:   int64(len(timestamps)) * precision,
		applied:    applied,
	}
}

// expireTo removes from the aggregate every live slot at or below cutoff. A
// cutoff at or below the one already applied changes nothing, so a Get holding
// a stale cutoff sees the newer state, which is a valid ordering of the two.
//
// Its cost follows the distance from applied to cutoff, not the size of the
// ring. A window with no live slot only records the cutoff, which is also how
// a fresh window, whose applied is math.MinInt64, skips the walk from there.
// When the distance reaches ringSpan every slot may be due, so it scans the
// ring once. Otherwise it visits only the buckets in (applied, cutoff], each at
// its index, and that walk misses no live slot at or below cutoff: after any
// operation every live timestamp lies in (applied, applied + WindowSize] (I2),
// at most ring.Slots() consecutive buckets, so no two live buckets share an
// index and each one in (applied, cutoff] is found at its own index.
func (w *window) expireTo(cutoff int64) {
	if cutoff <= w.applied {
		return
	}
	switch {
	case w.live == 0:
	case w.spanCoversRing(cutoff):
		w.expireWholeRing(cutoff)
	default:
		w.expireSpan(cutoff)
	}
	w.applied = cutoff
}

// spanCoversRing reports whether cutoff lies at least ringSpan past applied.
// The int64 subtraction wraps, as Go defines it, to a negative value exactly
// when the distance is 2^63 or more, as it is from the math.MinInt64 of a
// fresh window; the caller ensures cutoff > applied.
func (w *window) spanCoversRing(cutoff int64) bool {
	distance := cutoff - w.applied
	return distance < 0 || distance >= w.ringSpan
}

func (w *window) expireWholeRing(cutoff int64) {
	for i := range w.timestamps {
		w.expireIfDue(i, cutoff)
	}
}

// expireSpan walks the buckets in (applied, cutoff], stopping early once no
// slot is live. It runs only when cutoff is above applied by less than
// ringSpan, so cutoff is not the math.MinInt64 of a mark never observed: it is
// HW - WindowSize for a real mark, at least -2^62 - WindowSize, and neither
// the first bucket nor the steps of the walk can overflow.
func (w *window) expireSpan(cutoff int64) {
	first := (floorDiv(w.applied, w.precision) + 1) * w.precision
	for timestamp := first; timestamp <= cutoff && w.live > 0; timestamp += w.precision {
		w.expireIfDue(w.index(timestamp), cutoff)
	}
}

// expireIfDue expires slot i if it is live at or below cutoff. The walk checks
// the timestamp too, so it can never expire a slot the whole-ring scan would
// keep.
func (w *window) expireIfDue(i int, cutoff int64) {
	if timestamp := w.timestamps[i]; timestamp != deadSlot && timestamp <= cutoff {
		w.expire(i)
	}
}

func (w *window) expire(i int) {
	w.ring.Expire(i)
	w.timestamps[i] = deadSlot
	w.live--
}

// record counts h in the bucket at timestamp and returns the window estimate
// for h afterwards. timestamp must be above the applied cutoff, which Store
// guarantees by expiring to its own cutoff first.
func (w *window) record(timestamp int64, h uint64) (estimate uint64, saturated bool) {
	return w.ring.Add(w.liveSlot(timestamp), h)
}

func (w *window) estimate(h uint64) uint64 {
	return w.ring.Count(h)
}

// liveSlot returns the index of the slot holding timestamp, making it live on
// first use. A live slot tagged with another timestamp would mean I2 is
// broken; retagging it would silently fold two buckets together, so it panics
// instead.
func (w *window) liveSlot(timestamp int64) int {
	i := w.index(timestamp)
	switch held := w.timestamps[i]; held {
	case timestamp:
	case deadSlot:
		w.timestamps[i] = timestamp
		w.live++
	default:
		panic(fmt.Sprintf("countminsketch: slot for bucket %d still holds live bucket %d", timestamp, held))
	}
	return i
}

// index maps a bucket timestamp onto its slot. Bucket timestamps are multiples
// of precision, so the division is exact; the modulo rounds toward negative
// infinity so negative timestamps keep consecutive buckets on consecutive slots.
func (w *window) index(timestamp int64) int {
	n := int64(len(w.timestamps))
	i := (timestamp / w.precision) % n
	if i < 0 {
		i += n
	}
	return int(i)
}
