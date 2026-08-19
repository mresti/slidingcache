package slidingcache

// entry holds the live event timestamps for a single key, kept sorted in
// ascending order. Timestamps are seconds truncated to the cache Precision, and
// duplicates are permitted: two events in the same bucket are stored as two
// timestamps and counted as two events.
type entry struct {
	timestamps []int64
}

// insert places timestamp into the sorted slice, preserving ascending order.
// Using an ordered insert keeps prune a cheap prefix operation even when events
// arrive out of order.
func (e *entry) insert(timestamp int64) {
	i := e.lowerBound(timestamp)
	e.timestamps = append(e.timestamps, 0)
	copy(e.timestamps[i+1:], e.timestamps[i:])
	e.timestamps[i] = timestamp
}

// firstAlive returns the index of the first timestamp that is still alive
// (timestamp > cutoff). Because the slice is sorted, expired timestamps form a
// prefix, so the index doubles as the length of that prefix. cutoff+1 cannot
// overflow: cutoff is derived from a high-water mark minus a WindowSize of at
// least one second.
func (e *entry) firstAlive(cutoff int64) int {
	return e.lowerBound(cutoff + 1)
}

// liveCount returns how many of the entry's timestamps are still alive without
// mutating it.
func (e *entry) liveCount(cutoff int64) int {
	return len(e.timestamps) - e.firstAlive(cutoff)
}

// lowerBound returns the index of the first timestamp >= target, or len when
// there is none. It is hand-rolled rather than delegating to slices.BinarySearch
// so that it stays within the inliner's budget: every read and write of an entry
// goes through it, and the call overhead dominates for the short slices that a
// bounded window produces.
func (e *entry) lowerBound(target int64) int {
	low, high := 0, len(e.timestamps)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if e.timestamps[mid] < target {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

// prune drops every timestamp that is no longer alive (timestamp <= cutoff).
// When the backing array is much larger than the surviving suffix, the suffix is
// copied into a right-sized slice so the large backing array can be collected
// instead of being pinned by a re-slice. A fully expired entry keeps its small
// backing array: it is about to be refilled by the Store that triggered the
// prune, and dropping it would make every re-store of an idle key allocate.
func (e *entry) prune(cutoff int64) {
	firstAlive := e.firstAlive(cutoff)
	if firstAlive == 0 {
		return
	}
	if firstAlive == len(e.timestamps) {
		if shouldRightSize(cap(e.timestamps), 0) {
			e.timestamps = nil
		} else {
			e.timestamps = e.timestamps[:0]
		}
		return
	}

	alive := e.timestamps[firstAlive:]
	if shouldRightSize(cap(e.timestamps), len(alive)) {
		e.timestamps = append([]int64(nil), alive...)
		return
	}
	e.timestamps = e.timestamps[:copy(e.timestamps, alive)]
}

func shouldRightSize(capacity, liveLen int) bool {
	return capacity > entryReallocMinCap && capacity > 2*liveLen
}
