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
	mu   sync.Mutex
	keys map[string]*entry
	peak int // largest observed len(keys) since the last map compaction.
}

func newShards(count int) []*shard {
	shards := make([]*shard, count)
	for i := range shards {
		shards[i] = &shard{keys: make(map[string]*entry)}
	}
	return shards
}

// store records timestamp for key and returns the resulting live count, or
// lateEvent when timestamp has already expired under the cutoff in force at the
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
		return lateEvent
	}

	e, ok := s.keys[key]
	if !ok {
		s.keys[key] = &entry{timestamps: []int64{timestamp}}
		s.trackPeak()
		return 1
	}

	e.prune(cutoff)
	// Spelled out instead of e.insert so that every piece inlines here; see
	// entry.insert for why the combined function does not.
	if e.inOrder(timestamp) {
		e.appendInOrder(timestamp)
	} else {
		e.insertOutOfOrder(timestamp)
	}
	return len(e.timestamps)
}

// count returns the key's live count without mutating the shard. Cleanup of
// expired timestamps and empty keys happens on the next Store to the key and in
// the janitor sweep. It returns 0 for an absent key.
func (s *shard) count(key string, cutoff int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.keys[key]
	if !ok {
		return 0
	}
	return e.liveCount(cutoff)
}

// sweep prunes every key in the shard and compacts the backing map when it has
// shrunk substantially since its peak.
func (s *shard) sweep(cutoff int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, e := range s.keys {
		e.prune(cutoff)
		if len(e.timestamps) == 0 {
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
