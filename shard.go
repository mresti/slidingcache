package slidingcache

import (
	"maps"
	"sync"
	"sync/atomic"
)

// shard is an independently locked partition of the key space. Keys are assigned
// to shards by a hash of the key, so operations on different shards proceed
// concurrently.
type shard struct {
	// rejects comes first, and is padded to a whole cache line, so the atomic
	// writes of the rejection paths never invalidate the line holding mu and
	// keys, which every accepted operation touches.
	rejects shardRejects
	// The fields an accepted operation writes are kept adjacent, so the lock, the
	// counters, and the map header fall on as few cache lines as the allocator
	// allows: the increments then dirty the line the lock has already taken.
	mu       sync.Mutex
	counters shardCounters
	keys     map[string]*entry
	peak     int // largest observed len(keys) since the last map compaction.
	// layout is a copy of the Cache's bucket layout, held here so the hot path
	// reads it from the shard it has already loaded. It is read-only, so it comes
	// last, after every field an operation writes.
	layout bucketLayout
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
	accepted uint64
	late     uint64
	getHit   uint64
	getMiss  uint64
}

// cacheLineSize is the 64-byte line of every architecture this library targets;
// on the 128-byte-line arm64 cores it merely under-pads, which costs nothing on
// a path that is never hot.
const cacheLineSize = 64

func newShards(count int, layout bucketLayout) []*shard {
	shards := make([]*shard, count)
	for i := range shards {
		shards[i] = &shard{layout: layout, keys: make(map[string]*entry)}
	}
	return shards
}

// store records timestamp for key and returns the resulting live count, or
// LateEvent when timestamp has already expired under the cutoff in force at the
// moment the shard lock is acquired. The key's expired prefix is pruned before
// the insert so the entry is grown at most once.
//
// The cutoff is derived here, under the lock, rather than passed in: one read
// before the lock can be stale, because a concurrent Store may advance the
// high-water mark in between and turn an apparently live timestamp into an
// expired one.
func (s *shard) store(key string, timestamp int64, highWater *atomic.Int64, windowSize int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := cutoffFor(highWater.Load(), windowSize)
	if timestamp <= cutoff {
		s.counters.late++
		return LateEvent
	}

	s.counters.accepted++

	e, ok := s.keys[key]
	if !ok {
		s.keys[key] = newEntry(s.layout, timestamp)
		s.trackPeak()
		return 1
	}

	e.prune(s.layout, cutoff)
	// Spelled out instead of e.insert so that the in-order path inlines here;
	// see entry.insert for why the combined function does not.
	if e.inOrder(s.layout, timestamp) {
		e.recordInOrder(s.layout, timestamp)
	} else {
		e.recordOutOfOrder(s.layout, timestamp)
	}
	return e.total
}

// count returns the key's live count without mutating the shard. Cleanup of
// expired buckets and empty keys happens on the next Store to the key and in
// the janitor sweep. It returns 0 for an absent key.
func (s *shard) count(key string, cutoff int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.keys[key]
	if !ok {
		s.counters.getMiss++
		return 0
	}
	s.counters.getHit++
	return e.liveCount(s.layout, cutoff)
}

// sweep prunes every key in the shard, deletes the keys left without a single
// retained event, and compacts the backing map when it has shrunk substantially
// since its peak.
func (s *shard) sweep(cutoff int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, e := range s.keys {
		e.prune(s.layout, cutoff)
		if e.total == 0 {
			delete(s.keys, key)
		}
	}
	s.compactIfSparse()
}

func (s *shard) trackPeak() {
	if len(s.keys) > s.peak {
		s.peak = len(s.keys)
	}
}

// compactIfSparse rebuilds the shard map into a right-sized one when the live
// key count has fallen well below the observed peak. Go maps never shrink their
// bucket arrays on their own, so this returns memory to the garbage collector
// after bursts of short-lived keys.
func (s *shard) compactIfSparse() {
	live := len(s.keys)
	if s.peak <= mapCompactMinPeak || live >= s.peak/2 {
		return
	}
	compacted := make(map[string]*entry, live)
	maps.Copy(compacted, s.keys)
	s.keys = compacted
	s.peak = live
}

func (s *shard) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// addTo accumulates this shard's counters into stats. The atomics and the
// lock-guarded fields are read in one pass so a shard is visited once, and
// len(keys) is read under the lock that already has to be taken.
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
	stats.GetHit += s.counters.getHit
	stats.GetMiss += s.counters.getMiss
	stats.Keys += len(s.keys)
}
