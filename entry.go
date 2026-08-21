package slidingcache

// bucket packs one Precision bucket of a key into a single 8-byte word: the
// bucket timestamp in the high bits and the number of events that landed in it
// in the low bits. How the word is split is not fixed by this type; a
// bucketLayout decides it per Cache, and every read and write of a word goes
// through one.
//
// Events that share a bucket are indistinguishable to the window semantics, so
// storing a count instead of one timestamp per event is lossless. Packing that
// count next to the timestamp instead of beside it in a struct halves the size
// of a bucket, and keeps ordering comparisons a plain integer comparison on the
// word: the timestamp occupies the most significant bits, so a larger timestamp
// always makes a larger word whatever the count.
//
// The count occupying the low bits also makes counting one more event into a
// bucket an increment of the word itself, which is what the record methods do
// once accepts has confirmed there is room; incrementing a full count would
// carry into the timestamp instead.
type bucket int64

const (
	// defaultCountBits is the count width of a Config that leaves CountBits
	// zero: a million events per bucket, with timestamps reaching the year
	// 280,707, which suits every ordinary workload.
	defaultCountBits = 20
	// minCountBits and maxCountBits bound Config.CountBits. Below the minimum a
	// bucket would spill so often that the memory saved on timestamp bits is
	// lost several times over; above the maximum the timestamp bits would start
	// excluding real Unix epochs. Config.CountBits documents both ends.
	minCountBits = 8
	maxCountBits = 24
)

// bucketLayout fixes, for one Cache, how a bucket word is split between the
// timestamp and the count: the low countBits hold the count, the remaining
// 63-countBits bits hold the bucket timestamp in seconds.
//
// It is built once by New and copied by value into the Cache and into every
// shard, and the entry methods take it by value as well. Holding it by value
// costs about 3% on Store against the compile-time constants it replaces, and
// is the cheapest of the variants measured: a pointer field adds a load and an
// aliasing barrier the compiler cannot see through, and deriving the bounds
// from countBits on the fly pushes the record methods past the inliner's
// budget.
type bucketLayout struct {
	countBits uint
	// maxCount is both the largest count a word holds and the mask that reads
	// it back. A bucket that reaches it spills into a further word with the same
	// timestamp, so no event is lost.
	maxCount int
	// minTimestamp and maxTimestamp bound the bucket timestamps, in seconds,
	// that fit in the high bits of a word. Store and Get reject anything beyond
	// them, because packing such a timestamp would overflow into the sign bit.
	minTimestamp int64
	maxTimestamp int64
}

func newBucketLayout(countBits int) bucketLayout {
	bits := uint(countBits)
	return bucketLayout{
		countBits:    bits,
		maxCount:     1<<bits - 1,
		minTimestamp: -1 << (63 - bits),
		maxTimestamp: 1<<(63-bits) - 1,
	}
}

// inRange reports whether a bucket timestamp is representable in this layout.
func (l bucketLayout) inRange(timestamp int64) bool {
	return timestamp >= l.minTimestamp && timestamp <= l.maxTimestamp
}

// newBucket packs a bucket timestamp and an event count into one word. The
// timestamp must be in range, which Store and Get guarantee by rejecting epochs
// outside it, and the count must not exceed maxCount.
func (l bucketLayout) newBucket(timestamp int64, count int) bucket {
	return bucket(timestamp<<l.countBits | int64(count))
}

func (l bucketLayout) timestamp(b bucket) int64 { return int64(b) >> l.countBits }

func (l bucketLayout) count(b bucket) int { return int(int64(b) & int64(l.maxCount)) }

func (l bucketLayout) full(b bucket) bool { return l.count(b) == l.maxCount }

// accepts reports whether one more event at timestamp can be counted into b,
// which requires b to be the bucket of that timestamp and to have room left.
func (l bucketLayout) accepts(b bucket, timestamp int64) bool {
	return l.timestamp(b) == timestamp && !l.full(b)
}

// floor is the smallest word whose timestamp is timestamp, used as a
// binary-search target so the search compares packed words without unpacking
// them. It is only meaningful for a timestamp within the representable range,
// which lowerBound checks before calling it.
func (l bucketLayout) floor(timestamp int64) bucket { return bucket(timestamp << l.countBits) }

// entry holds the buckets of a single key.
//
// Invariants, maintained by every method below:
//
//   - buckets is sorted by timestamp, non-decreasing. Adjacent buckets share a
//     timestamp only when the earlier one is full, which is how a bucket that
//     outgrows the count of one word spills into the next.
//   - every count is >= 1: a bucket exists only once an event landed in it.
//   - total equals the sum of the counts, expired buckets included. It is the
//     number of events physically retained, which liveCount and prune keep in
//     step with the slice.
//   - len(buckets) is bounded by WindowSize/Precision once the key has been
//     pruned, regardless of the event rate, because the live window spans that
//     many distinct timestamps, plus one extra word per full bucket count that a
//     single timestamp receives. Between a prune and the next one the
//     length may exceed the bound by the expired prefix, which the next Store or
//     sweep of the key drops.
type entry struct {
	buckets []bucket
	total   int
}

// newEntry returns the entry of a key whose first event landed at timestamp.
func newEntry(l bucketLayout, timestamp int64) *entry {
	return &entry{buckets: []bucket{l.newBucket(timestamp, 1)}, total: 1}
}

// insert records one event at timestamp.
//
// It is the composition of inOrder, recordInOrder and recordOutOfOrder. The hot
// path in shard.store calls those three directly instead of insert: inOrder and
// recordInOrder each fit the inliner's budget, so an in-order event is recorded
// without a call at all, whereas insert combines them with the out-of-order path
// and no longer fits, which would cost a call on every Store. Only the
// out-of-order path, which does not fit either, pays one.
func (e *entry) insert(l bucketLayout, timestamp int64) {
	if e.inOrder(l, timestamp) {
		e.recordInOrder(l, timestamp)
		return
	}
	e.recordOutOfOrder(l, timestamp)
}

// inOrder reports whether timestamp is at least as new as every stored bucket,
// which includes repeats of the newest one. Such arrivals are recorded without
// searching or shifting anything.
func (e *entry) inOrder(l bucketLayout, timestamp int64) bool {
	n := len(e.buckets)
	return n == 0 || timestamp >= l.timestamp(e.buckets[n-1])
}

// recordInOrder counts an event for which inOrder returned true: it increments
// the newest bucket when the event repeats it, and appends a new bucket
// otherwise, whether because the timestamp is new or because the newest bucket
// has no room left. A hot key therefore pays an increment per event rather than
// a slice growth, and its bucket count tracks elapsed time instead of event
// volume.
func (e *entry) recordInOrder(l bucketLayout, timestamp int64) {
	e.total++
	if n := len(e.buckets); n > 0 && l.accepts(e.buckets[n-1], timestamp) {
		e.buckets[n-1]++
		return
	}
	e.buckets = append(e.buckets, l.newBucket(timestamp, 1))
}

// recordOutOfOrder counts an event older than the newest bucket. It increments
// the first word of the event's timestamp that still has room, and creates a
// word in sorted position when the timestamp has none: either because no event
// had landed in it yet, or because every word it already owns is full. Only such
// a new word pays the shift; repeats of an existing bucket cost an increment, so
// a hot key that receives jittered timestamps cannot degrade into a memmove per
// event.
func (e *entry) recordOutOfOrder(l bucketLayout, timestamp int64) {
	e.total++
	i := e.lowerBound(l, timestamp)
	for ; i < len(e.buckets) && l.timestamp(e.buckets[i]) == timestamp; i++ {
		if !l.full(e.buckets[i]) {
			e.buckets[i]++
			return
		}
	}
	e.buckets = append(e.buckets, 0)
	copy(e.buckets[i+1:], e.buckets[i:])
	e.buckets[i] = l.newBucket(timestamp, 1)
}

// firstAlive returns the index of the first bucket that is still alive
// (timestamp > cutoff). Because the slice is sorted, expired buckets form a
// prefix, so the index doubles as the length of that prefix. cutoff+1 cannot
// overflow: cutoff is derived from a high-water mark minus a WindowSize of at
// least one second, and lowerBound saturates on a cutoff that lies outside the
// representable bucket range.
func (e *entry) firstAlive(l bucketLayout, cutoff int64) int {
	return e.lowerBound(l, cutoff+1)
}

// liveCount returns how many of the entry's events are still alive without
// mutating it.
//
// It subtracts the expired prefix from the cached total, so the cost is the
// length of that prefix. The prefix is empty for any key stored or swept since
// the cutoff last moved past its oldest bucket, which is the common case; the
// worst case is a key that was written and then left untouched until all of its
// buckets expired, where the scan is bounded by WindowSize/Precision buckets.
func (e *entry) liveCount(l bucketLayout, cutoff int64) int {
	live := e.total
	for i := range e.firstAlive(l, cutoff) {
		live -= l.count(e.buckets[i])
	}
	return live
}

// lowerBound returns the index of the first bucket with a timestamp >= target,
// or len when there is none. It compares packed words against the word target
// floors to, so the search never unpacks a bucket.
//
// A target outside the representable range saturates instead of being packed:
// shifting it into a word would overflow and wrap the comparison around. This is
// not hypothetical, because firstAlive derives its target from a cutoff, and the
// cutoff of a cache that has not yet observed an epoch sits at math.MinInt64.
//
// The search is hand-rolled rather than delegating to slices.BinarySearchFunc so
// that it stays within the inliner's budget: every read and write of an entry
// goes through it, and the call overhead dominates for the short slices that a
// bounded window produces.
func (e *entry) lowerBound(l bucketLayout, target int64) int {
	if target <= l.minTimestamp {
		return 0
	}
	if target > l.maxTimestamp {
		return len(e.buckets)
	}
	floor := l.floor(target)
	low, high := 0, len(e.buckets)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if e.buckets[mid] < floor {
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
func (e *entry) prune(l bucketLayout, cutoff int64) {
	firstAlive := e.firstAlive(l, cutoff)
	if firstAlive == 0 {
		return
	}
	for i := range firstAlive {
		e.total -= l.count(e.buckets[i])
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
// entries of 20 to 3600 buckets confirms it: 256 (a 2KB memmove) is the largest
// value that never loses, removing up to 5x of churn on a ~200-bucket key, while
// 1024 makes the memmove dominate. BenchmarkPruneCopyThreshold reproduces the
// sweep.
const pruneCopyMaxLen = 256

func shouldRightSize(capacity, liveLen int) bool {
	return capacity > entryReallocMinCap && capacity > 2*liveLen
}
