package countminsketch

import (
	"sync"
	"sync/atomic"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

// shard is an independently locked partition of the keys, with its own window
// over its share of the sketch's columns. Keys are assigned to shards by the
// same hash that picks their columns, so operations on different shards proceed
// concurrently. The high-water mark stays global: a shard that receives no
// traffic for a while only lags, keeping slots its next operation expires.
type shard struct {
	// rejects comes first, and is padded to a whole cache line, so the atomic
	// writes of the rejection paths never invalidate the line holding mu and
	// the window, which every accepted operation touches.
	rejects shardRejects
	// The fields an accepted operation writes are kept adjacent, so the lock, the
	// counters and the window's header fall on as few cache lines as the
	// allocator allows: the increments then dirty the line the lock has already
	// taken.
	mu       sync.Mutex
	counters shardCounters
	window   window
}

// shardRejects counts the rejections decided before the shard lock is taken, so
// they are atomic. They are written only on the rare paths: an accepted Store or
// Get never touches them.
//
// The trailing pad rounds the struct to a 64-byte cache line. The Go allocator
// does not promise 64-byte-aligned objects, so the pad does not put the block on
// a line of its own; what it does guarantee is that no field after it in shard
// can share a line with the atomics.
type shardRejects struct {
	outOfRange atomic.Uint64
	late       atomic.Uint64
	future     atomic.Uint64
	getLate    atomic.Uint64
	getFuture  atomic.Uint64
	_          [cacheLineSize - 5*8]byte
}

// shardCounters counts the outcomes decided under the shard lock, so plain adds
// suffice: the lock is already held and the read in Stats takes it too.
type shardCounters struct {
	accepted  uint64
	late      uint64
	saturated uint64
	getHit    uint64
	getMiss   uint64
}

// cacheLineSize is the 64-byte line of every architecture this library targets;
// on the 128-byte-line arm64 cores it merely under-pads, which costs nothing on
// a path that is never hot.
const cacheLineSize = 64

// newShards allocates every shard's window in full, after New has checked the
// layout against the memory cap.
func newShards(l windowLayout, precision, applied int64) ([]*shard, error) {
	shards := make([]*shard, l.shards)
	for i := range shards {
		ring, err := countmin.NewRing(l.shardWidth, l.depth, l.buckets)
		if err != nil {
			return nil, err
		}
		shards[i] = &shard{window: newWindow(ring, precision, applied)}
	}
	return shards, nil
}

// store records h in the bucket at timestamp and returns the shard's estimate
// for h afterwards, or LateEvent when timestamp has already expired under the
// cutoff in force at the moment the shard lock is acquired.
//
// The cutoff is derived here, under the lock, rather than passed in: one read
// before the lock can be stale, because a concurrent Store may advance the
// high-water mark in between and turn an apparently live timestamp into an
// expired one.
func (s *shard) store(timestamp int64, h uint64, highWater *atomic.Int64, windowSize int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := cutoffFor(highWater.Load(), windowSize)
	if timestamp <= cutoff {
		s.counters.late++
		return slidingcache.LateEvent
	}
	s.window.expireTo(cutoff)
	estimate, saturated := s.window.record(timestamp, h)
	s.counters.accepted++
	if saturated {
		s.counters.saturated++
	}
	return clampToInt(estimate)
}

// count expires the shard's window to the cutoff Get computed before the lock,
// as the root package counts against it, and returns the estimate for h; a
// newer state already applied by a concurrent Store is kept.
func (s *shard) count(cutoff int64, h uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.window.expireTo(cutoff)
	estimate := s.window.estimate(h)
	if estimate > 0 {
		s.counters.getHit++
	} else {
		s.counters.getMiss++
	}
	return clampToInt(estimate)
}

// addTo accumulates this shard's counters into stats, the atomics first and then
// the lock-guarded fields, so a shard is visited once.
func (s *shard) addTo(stats *Stats) {
	stats.OutOfRange += s.rejects.outOfRange.Load()
	stats.Late += s.rejects.late.Load()
	stats.Future += s.rejects.future.Load()
	stats.GetLate += s.rejects.getLate.Load()
	stats.GetFuture += s.rejects.getFuture.Load()

	s.mu.Lock()
	defer s.mu.Unlock()

	stats.Accepted += s.counters.accepted
	stats.Late += s.counters.late
	stats.Saturated += s.counters.saturated
	stats.GetHit += s.counters.getHit
	stats.GetMiss += s.counters.getMiss
}
