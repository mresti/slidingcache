package countminsketch

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/mresti/slidingcache/internal/countmin"
)

// testWindow is a window of the given precision and size on a small ring,
// fresh as newShards builds it, with applied at math.MinInt64.
func testWindow(t *testing.T, precision, windowSize int64) *window {
	t.Helper()
	slots := int((windowSize + precision - 1) / precision)
	ring, err := countmin.NewRing(8, 2, slots)
	if err != nil {
		t.Fatalf("NewRing(8, 2, %d): %v", slots, err)
	}
	w := newWindow(ring, precision, math.MinInt64)
	return &w
}

// expireToByScanningTheRing is the expiry expireTo replaced: every slot of the
// ring checked whenever the cutoff advances. It is the reference the walk must
// match.
func expireToByScanningTheRing(w *window, cutoff int64) {
	if cutoff <= w.applied {
		return
	}
	for i, timestamp := range w.timestamps {
		if timestamp != deadSlot && timestamp <= cutoff {
			w.expire(i)
		}
	}
	w.applied = cutoff
}

// windowState is everything a window holds, raw cells included, so two
// windows can be compared exactly.
type windowState struct {
	timestamps []int64
	applied    int64
	live       int
	slotCells  []uint32
	aggregate  []uint64
}

func stateOf(w *window) windowState {
	state := windowState{
		timestamps: slices.Clone(w.timestamps),
		applied:    w.applied,
		live:       w.live,
		aggregate:  w.ring.AppendAggregateCells(nil),
	}
	for slot := range w.ring.Slots() {
		state.slotCells = w.ring.AppendSlotCells(state.slotCells, slot)
	}
	return state
}

func requireSameState(t *testing.T, step string, got, want *window) {
	t.Helper()
	g, r := stateOf(got), stateOf(want)
	switch {
	case g.applied != r.applied:
		t.Fatalf("%s: applied %d, reference %d", step, g.applied, r.applied)
	case !slices.Equal(g.timestamps, r.timestamps):
		t.Fatalf("%s: timestamps %v, reference %v", step, g.timestamps, r.timestamps)
	case g.live != r.live || g.live != countLive(got):
		t.Fatalf("%s: live %d over %d live slots, reference %d", step, g.live, countLive(got), r.live)
	case !slices.Equal(g.slotCells, r.slotCells) || !slices.Equal(g.aggregate, r.aggregate):
		t.Fatalf("%s: the cells differ from the reference", step)
	}
}

func countLive(w *window) int {
	live := 0
	for _, timestamp := range w.timestamps {
		if timestamp != deadSlot {
			live++
		}
	}
	return live
}

// windowDriver plays a shard's life on two windows at once: a mark that jumps
// by chosen numbers of buckets, operations that catch up to it or lag behind,
// stale cutoffs, and events recorded only where a Store may record them, in
// (applied, applied + WindowSize] once the window has caught up.
type windowDriver struct {
	t                     *testing.T
	rng                   *rand.Rand
	walk, reference       *window
	precision, windowSize int64
	highWater             int64
	jumps                 []int64
}

func newWindowDriver(t *testing.T, seed uint64, precision, windowSize int64) *windowDriver {
	t.Helper()
	walk := testWindow(t, precision, windowSize)
	slots := int64(len(walk.timestamps))
	return &windowDriver{
		t:          t,
		rng:        rand.New(rand.NewPCG(seed, uint64(precision))),
		walk:       walk,
		reference:  testWindow(t, precision, windowSize),
		precision:  precision,
		windowSize: windowSize,
		highWater:  -3 * slots * precision,
		jumps:      []int64{0, 1, 1, 2, slots - 1, slots, slots + 1, 3 * slots, 1_000_000},
	}
}

func (d *windowDriver) cutoff() int64 {
	return cutoffFor(d.highWater, d.windowSize)
}

func (d *windowDriver) step(i int) {
	d.t.Helper()
	switch op := d.rng.IntN(8); {
	case op < 4:
		d.highWater += d.jumps[d.rng.IntN(len(d.jumps))] * d.precision
		if d.rng.IntN(4) == 0 {
			return // another shard's operation: this window lags.
		}
		d.expireTo(i, "catch up", d.cutoff())
		d.recordSome(i)
	case op < 6:
		d.expireTo(i, "stale cutoff", d.walk.applied-d.rng.Int64N(3*d.precision))
	default:
		d.expireTo(i, "partial cutoff", d.walk.applied+d.rng.Int64N(max(d.cutoff()-d.walk.applied, 0)+1))
	}
}

func (d *windowDriver) expireTo(i int, what string, cutoff int64) {
	d.t.Helper()
	d.walk.expireTo(cutoff)
	expireToByScanningTheRing(d.reference, cutoff)
	requireSameState(d.t, fmt.Sprintf("step %d, %s to %d at HW %d", i, what, cutoff, d.highWater), d.walk, d.reference)
}

// recordSome counts a few events into random live buckets of a window that
// has just caught up with the mark.
func (d *windowDriver) recordSome(i int) {
	d.t.Helper()
	first := (floorDiv(d.cutoff(), d.precision) + 1) * d.precision
	buckets := (d.highWater-first)/d.precision + 1
	for range d.rng.IntN(4) {
		timestamp := first + d.rng.Int64N(buckets)*d.precision
		h := d.rng.Uint64()
		d.walk.record(timestamp, h)
		d.reference.record(timestamp, h)
		requireSameState(d.t, fmt.Sprintf("step %d, record %d", i, timestamp), d.walk, d.reference)
	}
}

// TestExpireToWalkMatchesTheWholeRingScan drives the walk and the scan it
// replaced through the same pseudo-random lives: windows that are and are not
// multiples of their precision, timestamps on both sides of zero, jumps of a
// bucket, of about a ring and far beyond, lagging and stale cutoffs.
func TestExpireToWalkMatchesTheWholeRingScan(t *testing.T) {
	windows := []struct{ precision, windowSize int64 }{
		{1, 60},
		{10, 60},
		{10, 65},
		{3, 10},
		{7, 13},
		{5, 5},
	}
	const steps = 3_000
	for _, win := range windows {
		for seed := range uint64(4) {
			name := fmt.Sprintf("precision=%d/window=%d/seed=%d", win.precision, win.windowSize, seed)
			t.Run(name, func(t *testing.T) {
				d := newWindowDriver(t, seed, win.precision, win.windowSize)
				// The first operation of a shard, from the fresh applied of
				// math.MinInt64, below which no stale cutoff exists.
				d.expireTo(-1, "first cutoff", d.cutoff())
				for i := range steps {
					d.step(i)
				}
			})
		}
	}
}

// recordBuckets records one event in each bucket from first to last.
func recordBuckets(w *window, first, last int64) {
	for timestamp := first; timestamp <= last; timestamp += w.precision {
		w.record(timestamp, splitmix64(uint64(timestamp-first)))
	}
}

func requireTimestamps(t *testing.T, w *window, want ...int64) {
	t.Helper()
	var live []int64
	for _, timestamp := range w.timestamps {
		if timestamp != deadSlot {
			live = append(live, timestamp)
		}
	}
	slices.Sort(live)
	if !slices.Equal(live, want) {
		t.Fatalf("live buckets %v, want %v", live, want)
	}
	if w.live != len(want) {
		t.Fatalf("live count %d, want %d", w.live, len(want))
	}
}

// requireFinishesQuickly fails rather than hangs if expireTo walked bucket by
// bucket from math.MinInt64, which would take about 2^63 steps.
func requireFinishesQuickly(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("expireTo did not finish: it walked the distance from math.MinInt64")
	}
}

func TestExpireToFromAFreshWindowDoesNoPerBucketWork(t *testing.T) {
	const huge = int64(1) << 62
	t.Run("no live slot", func(t *testing.T) {
		w := testWindow(t, 1, 60)
		requireFinishesQuickly(t, func() { w.expireTo(huge) })
		if w.applied != huge {
			t.Fatalf("applied %d, want %d", w.applied, huge)
		}
		requireTimestamps(t, w)
	})
	t.Run("live slots under the sentinel", func(t *testing.T) {
		w := testWindow(t, 1, 60)
		recordBuckets(w, 0, 59)
		requireFinishesQuickly(t, func() { w.expireTo(30) })
		if w.applied != 30 {
			t.Fatalf("applied %d, want 30", w.applied)
		}
		want := make([]int64, 0, 29)
		for timestamp := int64(31); timestamp <= 59; timestamp++ {
			want = append(want, timestamp)
		}
		requireTimestamps(t, w, want...)
	})
}

func TestExpireToExpiresExactlyTheBucketsAtOrBelowTheCutoff(t *testing.T) {
	tests := []struct {
		name                  string
		precision, windowSize int64
		applied               int64
		first, last           int64
		cutoff                int64
		want                  []int64
	}{
		{
			name:      "cutoff between two buckets of a window not a multiple of precision",
			precision: 10, windowSize: 65, applied: -5,
			first: 0, last: 60, cutoff: 25,
			want: []int64{30, 40, 50, 60},
		},
		{
			name:      "window straddling zero",
			precision: 10, windowSize: 60, applied: -35,
			first: -30, last: 20, cutoff: 5,
			want: []int64{10, 20},
		},
		{
			name:      "distance one short of the ring, the longest walk",
			precision: 10, windowSize: 70, applied: 100,
			first: 110, last: 170, cutoff: 169,
			want: []int64{170},
		},
		{
			name:      "distance exactly the ring",
			precision: 10, windowSize: 70, applied: 100,
			first: 110, last: 170, cutoff: 170,
		},
		{
			name:      "distance one past the ring",
			precision: 10, windowSize: 70, applied: 100,
			first: 110, last: 170, cutoff: 171,
		},
		{
			name:      "negative buckets, cutoff not on a bucket",
			precision: 3, windowSize: 10, applied: -31,
			first: -30, last: -21, cutoff: -25,
			want: []int64{-24, -21},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := testWindow(t, tt.precision, tt.windowSize)
			reference := testWindow(t, tt.precision, tt.windowSize)
			for _, win := range []*window{w, reference} {
				win.expireTo(tt.applied)
				recordBuckets(win, tt.first, tt.last)
			}
			w.expireTo(tt.cutoff)
			expireToByScanningTheRing(reference, tt.cutoff)

			requireTimestamps(t, w, tt.want...)
			if w.applied != tt.cutoff {
				t.Fatalf("applied %d, want %d", w.applied, tt.cutoff)
			}
			requireSameState(t, tt.name, w, reference)
		})
	}
}
