package slidingcache

// bucket is one Precision bucket of a key together with the number of events
// recorded in it. Events that share a bucket are indistinguishable to the window
// semantics, so storing a count instead of one timestamp per event is lossless.
type bucket struct {
	timestamp int64
	count     int
}

// entry holds the buckets of a single key.
//
// Invariants, maintained by every method below:
//
//   - buckets is sorted strictly ascending by timestamp: no two buckets share a
//     timestamp, so a bucket is the unique home of its Precision window.
//   - every count is >= 1: a bucket exists only once an event landed in it.
//   - total equals the sum of the counts, expired buckets included. It is the
//     number of events physically retained, which liveCount and prune keep in
//     step with the slice.
//   - len(buckets) is bounded by WindowSize/Precision once the key has been
//     pruned, regardless of the event rate, because the live window spans that
//     many distinct timestamps. Between a prune and the next one the length may
//     exceed the bound by the expired prefix, which the next Store or sweep of
//     the key drops.
type entry struct {
	buckets []bucket
	total   int
}

// newEntry returns the entry of a key whose first event landed at timestamp.
func newEntry(timestamp int64) *entry {
	return &entry{buckets: []bucket{{timestamp: timestamp, count: 1}}, total: 1}
}

// insert records one event at timestamp.
//
// It is the composition of inOrder, recordInOrder and recordOutOfOrder. The hot
// path in shard.store calls those three directly instead of insert: inOrder and
// recordInOrder each fit the inliner's budget, so an in-order event is recorded
// without a call at all, whereas insert combines them with the out-of-order path
// and no longer fits, which would cost a call on every Store. Only the
// out-of-order path, which does not fit either, pays one.
func (e *entry) insert(timestamp int64) {
	if e.inOrder(timestamp) {
		e.recordInOrder(timestamp)
		return
	}
	e.recordOutOfOrder(timestamp)
}

// inOrder reports whether timestamp is at least as new as every stored bucket,
// which includes repeats of the newest one. Such arrivals are recorded without
// searching or shifting anything.
func (e *entry) inOrder(timestamp int64) bool {
	n := len(e.buckets)
	return n == 0 || timestamp >= e.buckets[n-1].timestamp
}

// recordInOrder counts an event for which inOrder returned true: it increments
// the newest bucket when the event repeats it, and appends a new bucket
// otherwise. A hot key therefore pays an increment per event rather than a slice
// growth, and its bucket count tracks elapsed time instead of event volume.
func (e *entry) recordInOrder(timestamp int64) {
	e.total++
	if n := len(e.buckets); n > 0 && e.buckets[n-1].timestamp == timestamp {
		e.buckets[n-1].count++
		return
	}
	e.buckets = append(e.buckets, bucket{timestamp: timestamp, count: 1})
}

// recordOutOfOrder counts an event older than the newest bucket. It increments
// the bucket at the event's timestamp, or creates that bucket in sorted position
// when the event is the first to land in it. Only a genuinely new bucket pays
// the shift; repeats of an existing one cost an increment, so a hot key that
// receives jittered timestamps cannot degrade into a memmove per event.
func (e *entry) recordOutOfOrder(timestamp int64) {
	e.total++
	i := e.lowerBound(timestamp)
	if i < len(e.buckets) && e.buckets[i].timestamp == timestamp {
		e.buckets[i].count++
		return
	}
	e.buckets = append(e.buckets, bucket{})
	copy(e.buckets[i+1:], e.buckets[i:])
	e.buckets[i] = bucket{timestamp: timestamp, count: 1}
}

// firstAlive returns the index of the first bucket that is still alive
// (timestamp > cutoff). Because the slice is sorted, expired buckets form a
// prefix, so the index doubles as the length of that prefix. cutoff+1 cannot
// overflow: cutoff is derived from a high-water mark minus a WindowSize of at
// least one second.
func (e *entry) firstAlive(cutoff int64) int {
	return e.lowerBound(cutoff + 1)
}

// liveCount returns how many of the entry's events are still alive without
// mutating it.
//
// It subtracts the expired prefix from the cached total, so the cost is the
// length of that prefix. The prefix is empty for any key stored or swept since
// the cutoff last moved past its oldest bucket, which is the common case; the
// worst case is a key that was written and then left untouched until all of its
// buckets expired, where the scan is bounded by WindowSize/Precision buckets.
func (e *entry) liveCount(cutoff int64) int {
	live := e.total
	for i := range e.firstAlive(cutoff) {
		live -= e.buckets[i].count
	}
	return live
}

// lowerBound returns the index of the first bucket with a timestamp >= target,
// or len when there is none. It is hand-rolled rather than delegating to
// slices.BinarySearchFunc so that it stays within the inliner's budget: every
// read and write of an entry goes through it, and the call overhead dominates
// for the short slices that a bounded window produces.
func (e *entry) lowerBound(target int64) int {
	low, high := 0, len(e.buckets)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if e.buckets[mid].timestamp < target {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

// prune drops every bucket that is no longer alive (timestamp <= cutoff) and
// discounts its events from total. A fully expired entry keeps its small backing
// array: it is about to be refilled by the Store that triggered the prune, and
// dropping it would make every re-store of an idle key allocate.
//
// Surviving buckets are kept in one of three ways, cheapest applicable first:
//
//   - Right-sizing, when the backing array is much larger than the survivors:
//     they are copied into an exact-fit slice so the large array is collected
//     instead of being pinned by a re-slice.
//   - A copy to the front, for a survivor run of at most pruneCopyMaxLen:
//     shifting those buckets is cheaper than the repeated reallocation that
//     re-slicing forward brings on an entry of that size.
//   - A forward re-slice, for a long survivor run: dropping the prefix costs
//     nothing per call, and the next append that outgrows the remaining capacity
//     reclaims the array while copying only the survivors.
func (e *entry) prune(cutoff int64) {
	firstAlive := e.firstAlive(cutoff)
	if firstAlive == 0 {
		return
	}
	for i := range firstAlive {
		e.total -= e.buckets[i].count
	}
	if firstAlive == len(e.buckets) {
		if shouldRightSize(cap(e.buckets), 0) {
			e.buckets = nil
		} else {
			e.buckets = e.buckets[:0]
		}
		return
	}

	alive := e.buckets[firstAlive:]
	if shouldRightSize(cap(e.buckets), len(alive)) {
		e.buckets = append([]bucket(nil), alive...)
		return
	}
	if len(alive) <= pruneCopyMaxLen {
		e.buckets = e.buckets[:copy(e.buckets, alive)]
		return
	}
	e.buckets = alive
}

// pruneCopyMaxLen is the survivor count up to which prune shifts the survivors
// to the front of the backing array instead of re-slicing forward.
//
// The value follows from how append grows a slice: below 256 elements it
// doubles the capacity, above it grows by roughly a quarter. An entry that
// re-slices forward gives up its prefix capacity, so its next appends regrow it;
// a doubled capacity then satisfies the right-sizing rule (capacity > 2x live)
// on the very next prune, which copies the survivors into an exact-fit slice,
// and the cycle repeats: a reallocation and copy on nearly every prune. Above
// 256 the quarter growth never trips the rule and the re-slice stays free,
// amortizing its reallocation over many appends. Sweeping the threshold over
// entries of 20 to 3600 buckets confirms it: 256 (a 4KB memmove) is the largest
// value that never loses, removing up to 5x of churn on a ~200-bucket key, while
// 1024 makes the memmove dominate. BenchmarkPruneCopyThreshold reproduces the
// sweep.
const pruneCopyMaxLen = 256

func shouldRightSize(capacity, liveLen int) bool {
	return capacity > entryReallocMinCap && capacity > 2*liveLen
}
