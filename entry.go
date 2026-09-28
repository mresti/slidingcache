package slidingcache

import "slices"

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

// floor is the smallest word whose timestamp is timestamp, used as a search
// target so the search compares packed words without unpacking them. It is only
// meaningful for a timestamp within the representable range, which is why
// the searches are only given targets between two stored timestamps.
func (l bucketLayout) floor(timestamp int64) bucket { return bucket(timestamp << l.countBits) }

// entry holds the buckets of a single key.
//
// The buckets the key retains are buckets[head:]. The words before head expired
// and were discounted from total by an earlier prune; they are kept, unread,
// only so that the entry can later compact its retained buckets into their room
// instead of reallocating (see makeRoom).
//
// Invariants, maintained by every method below:
//
//   - buckets[head:] is sorted by timestamp, non-decreasing. Adjacent buckets
//     share a timestamp only when the earlier one is full, which is how a bucket
//     that outgrows the count of one word spills into the next. Every word before
//     head is older than every timestamp the cache can still accept, so a search
//     or an insert over the whole slice never stops before head.
//   - every count in buckets[head:] is >= 1: a bucket exists only once an event
//     landed in it.
//   - total equals the sum of the counts in buckets[head:], expired buckets not
//     yet pruned included. It is the number of events physically retained, which
//     liveCount and prune keep in step with the slice.
//   - head < len(buckets), or head == len(buckets) == 0.
//   - len(buckets)-head is bounded by WindowSize/Precision once the key has been
//     pruned, regardless of the event rate, because the live window spans that
//     many distinct timestamps, plus one extra word per full bucket count that a
//     single timestamp receives. Between a prune and the next one the length
//     may exceed the bound by the expired prefix, which the next Store or sweep
//     of the key drops.
type entry struct {
	buckets []bucket
	total   int
	head    int
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
//
// The word is looked up the way firstAlive looks up the first live one. The
// search from a guess is a call, so it is made from recordFromGuess, where
// nothing is left to do after it: with the call in this function and the event
// still to record after it, the compiler spilled registers on every path, which
// cost about 4% on BenchmarkStoreOutOfOrder, whose key is too uneven to ever
// take the guess.
func (e *entry) recordOutOfOrder(l bucketLayout, timestamp int64) {
	e.total++
	switch {
	case l.timestamp(e.buckets[e.head]) >= timestamp:
		e.recordAt(l, timestamp, e.head)
	case e.evenlySpread(l):
		e.recordFromGuess(l, timestamp)
	default:
		e.recordAt(l, timestamp, e.bisect(e.head+1, len(e.buckets)-1, l.floor(timestamp)))
	}
}

// recordFromGuess is recordOutOfOrder on an evenly spread entry, for an event
// newer than its oldest bucket.
func (e *entry) recordFromGuess(l bucketLayout, timestamp int64) {
	e.recordAt(l, timestamp, e.searchFromGuess(l, timestamp))
}

// recordAt counts an event into the first word from index i on that holds its
// timestamp and has room, or into a new word at i when there is none; i is the
// first word at or after the timestamp. It is below len because the event is
// older than the newest bucket, so the new word can be inserted by a single
// append of the tail onto itself, which keeps recordAt within the inliner's
// budget and gives each of recordOutOfOrder's three lookups its own copy.
func (e *entry) recordAt(l bucketLayout, timestamp int64, i int) {
	for ; i < len(e.buckets) && l.timestamp(e.buckets[i]) == timestamp; i++ {
		if !l.full(e.buckets[i]) {
			e.buckets[i]++
			return
		}
	}
	e.buckets = append(e.buckets[:i+1], e.buckets[i:]...)
	e.buckets[i] = l.newBucket(timestamp, 1)
}

// firstAlive returns the index of the first bucket that is still alive
// (timestamp > cutoff). Because the slice is sorted, expired buckets form a
// prefix, so the index doubles as the length of that prefix.
//
// The index is never below head. Get and the janitor read the cutoff before
// they take the shard lock, so a concurrent Store can prune the entry with a
// newer cutoff in between; the buckets the older cutoff would still call alive
// have then already been discounted from total, exactly as if prune had dropped
// them.
//
// Nothing expired and everything expired are answered from the oldest and the
// newest retained word, so a key left idle for a whole window costs one word
// rather than a search. Both checks compare unpacked timestamps, which holds at
// any cutoff, including the math.MinInt64 of a cache that has not observed an
// epoch yet; any other cutoff lies between two stored timestamps, so cutoff+1
// is one too and the search can pack it.
func (e *entry) firstAlive(l bucketLayout, cutoff int64) int {
	newest := len(e.buckets) - 1
	switch {
	case e.aliveFrom(l, e.head, cutoff):
		return e.head
	case !e.aliveFrom(l, newest, cutoff):
		return newest + 1
	case e.evenlySpread(l):
		return e.searchFromGuess(l, cutoff+1)
	}
	return e.bisect(e.head+1, newest, l.floor(cutoff+1))
}

// aliveFrom reports whether every bucket from index i on is still alive under
// cutoff, which holds trivially when the entry has no bucket at i. Because
// buckets are sorted, one word answers it: the one at i.
//
// The cutoff moves forward one bucket at a time, so a key stored or read at
// least once per bucket finds at most its oldest bucket expired: nothing on
// every call but the first of a bucket, and the oldest bucket on that one.
// prune and liveCount therefore ask aliveFrom head and then head+1 before they
// search the entry, which settles almost every call from its first two
// retained words. The check compares the unpacked timestamp, so it cannot
// overflow whatever the cutoff, and it stays within the inliner's budget.
func (e *entry) aliveFrom(l bucketLayout, i int, cutoff int64) bool {
	return i >= len(e.buckets) || l.timestamp(e.buckets[i]) > cutoff
}

// liveCount returns how many of the entry's events are still alive without
// mutating it.
//
// Because total counts the expired prefix too, the live count is both total
// minus the prefix and the sum of the live suffix, and liveCount sums whichever
// of the two is shorter. The prefix is empty or a single bucket for any key
// stored or read since the cutoff last moved, which is the common case and
// aliveFrom settles without a search. A key that was written and then left
// untouched until all of its buckets expired has an empty suffix instead, which
// firstAlive finds from the newest word, so it costs neither a search nor a
// scan, where subtracting its prefix would walk WindowSize/Precision buckets.
// The worst case is half the entry.
func (e *entry) liveCount(l bucketLayout, cutoff int64) int {
	switch {
	case e.aliveFrom(l, e.head, cutoff):
		return e.total
	case e.aliveFrom(l, e.head+1, cutoff):
		return e.total - l.count(e.buckets[e.head])
	}
	firstAlive := e.firstAlive(l, cutoff)
	if expired, alive := firstAlive-e.head, len(e.buckets)-firstAlive; alive < expired {
		return sumCounts(l, e.buckets[firstAlive:])
	}
	return e.total - sumCounts(l, e.buckets[e.head:firstAlive])
}

// sumCounts returns the number of events held in buckets.
func sumCounts(l bucketLayout, buckets []bucket) int {
	sum := 0
	for _, b := range buckets {
		sum += l.count(b)
	}
	return sum
}

// searchFromGuess returns the index of the first retained bucket with a
// timestamp >= target, for a target newer than the oldest retained bucket and no
// newer than the newest, on an evenly spread entry. The answer lies in
// (head, len-1]. Its callers settle the targets outside that range from the end
// words, comparing unpacked timestamps; within it the target is a representable
// timestamp, so the search compares packed words against the word it floors to
// and never unpacks a bucket.
//
// It starts from a guess rather than from the middle. On an evenly spread entry
// — one word per bucket for a key written every bucket — where target would sit
// if the timestamps were exactly even (interpolate) is the answer or a few words
// off. The search probes that guess, gallops away from it in doubling steps of
// at most maxGallop words, which settles a key with a few gaps or spilled
// buckets within a line or two of the guess, and bisects whatever the gallop has
// not ruled out.
//
// Bisection from the middle probes the same indices on every key of the same
// length, and a key that fills a long window holds a power-of-two array (16 KiB
// for 1,800 one-second buckets, see grownCapacity), so over a population of such
// keys its first probes land in the same few cache sets and evict one another.
// The guess touches one or two lines next to the answer instead of about seven
// spread over the array.
func (e *entry) searchFromGuess(l bucketLayout, target int64) int {
	low, high := e.head+1, len(e.buckets)-1
	floor := l.floor(target)
	guess := e.interpolate(l, target)
	if e.buckets[guess] < floor {
		low = guess + 1
		for step := 1; step <= maxGallop && guess+step < high; step <<= 1 {
			if e.buckets[guess+step] >= floor {
				return e.bisect(low, guess+step, floor)
			}
			low = guess + step + 1
		}
		return e.bisect(low, high, floor)
	}
	high = guess
	for step := 1; step <= maxGallop && guess-step >= low; step <<= 1 {
		if e.buckets[guess-step] < floor {
			return e.bisect(guess-step+1, high, floor)
		}
		high = guess - step
	}
	return e.bisect(low, high, floor)
}

// evenlySpread reports whether the retained words cover at least 8/9 of their
// timestamp span, which is when searchFromGuess's guess is close enough to be
// worth a probe. A sparser key is bisected instead: its guess could be far off,
// and on an entry that fits the L1 cache the probes a wrong guess wastes cost
// more than the bisection steps it saves. A key with a Precision above one
// second spans Precision timestamps per bucket and is always bisected.
//
// The bound also keeps interpolate's product in range: with the span at most
// 9/8 of the words and the words below 2^31, it stays below 2^63.
func (e *entry) evenlySpread(l bucketLayout) bool {
	head, newest := e.head, len(e.buckets)-1
	span := l.timestamp(e.buckets[newest]) - l.timestamp(e.buckets[head])
	words := int64(newest - head)
	return span-words <= words>>3 && words>>31 == 0
}

// interpolate returns where target would sit among the retained buckets if
// their timestamps were evenly spread from the oldest to the newest: an index in
// [head, len-1] for a target in searchFromGuess's range.
func (e *entry) interpolate(l bucketLayout, target int64) int {
	head, newest := e.head, len(e.buckets)-1
	oldest := l.timestamp(e.buckets[head])
	span, words := l.timestamp(e.buckets[newest])-oldest, int64(newest-head)
	return head + int((target-oldest)*words/span)
}

// maxGallop is the longest step searchFromGuess gallops from its guess before it
// bisects the rest: eight words, so the gallop stays within two cache lines of
// the guess.
const maxGallop = 8

// bisect returns the first index in [low, high) whose word is >= floor, or high
// when there is none. The words in that range are sorted, as every entry's are.
//
// It is hand-rolled rather than delegating to slices.BinarySearchFunc so that it
// stays within the inliner's budget.
func (e *entry) bisect(low, high int, floor bucket) int {
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
//     instead of staying pinned.
//   - A copy to the front, for a survivor run of at most pruneCopyMaxLen:
//     shifting those buckets is cheaper than the repeated reallocation that
//     moving head forward brings on an entry of that size.
//   - Moving head forward, for a long survivor run: dropping the prefix costs
//     nothing per call, and the room it leaves at the front of the array is
//     reclaimed in one copy once the array fills up (see makeRoom).
//
// An entry with more than its oldest bucket expired is handed to
// pruneSearching, and the hand-off is the last thing prune does there: with the
// search's call on a path that had work left after it, the compiler spilled
// registers on every path, which cost up to 5% on stores that expire exactly
// one bucket, the path almost every Store of a steadily written key takes.
func (e *entry) prune(l bucketLayout, cutoff int64) {
	switch {
	case e.aliveFrom(l, e.head, cutoff):
		return
	case !e.aliveFrom(l, e.head+1, cutoff):
		e.pruneSearching(l, cutoff)
		return
	}
	firstAlive := e.head + 1
	e.total -= sumCounts(l, e.buckets[e.head:firstAlive])
	if firstAlive == len(e.buckets) {
		e.head = 0
		if shouldRightSize(cap(e.buckets), 0) {
			e.buckets = nil
		} else {
			e.buckets = e.buckets[:0]
		}
		return
	}

	alive := e.buckets[firstAlive:]
	if shouldRightSize(cap(e.buckets), len(alive)) {
		e.buckets, e.head = append([]bucket(nil), alive...), 0
		return
	}
	if len(alive) <= pruneCopyMaxLen {
		e.buckets, e.head = e.buckets[:copy(e.buckets, alive)], 0
		return
	}
	e.head = firstAlive
}

// pruneSearching is prune for an entry with at least two buckets expired. It
// moves head onto the newest expired bucket, discounting the ones before it,
// and prunes again, which then finds exactly one bucket expired.
func (e *entry) pruneSearching(l bucketLayout, cutoff int64) {
	lastExpired := e.firstAlive(l, cutoff) - 1
	e.total -= sumCounts(l, e.buckets[e.head:lastExpired])
	e.head = lastExpired
	e.prune(l, cutoff)
}

// needsRoom reports whether recording one more event at timestamp would append
// to a backing array that has no room left, which is when shard.store calls
// makeRoom first. Repeats of the newest bucket are increments and need none, so
// a hot key whose array happens to be full never reaches makeRoom.
func (e *entry) needsRoom(l bucketLayout, timestamp int64) bool {
	n := len(e.buckets)
	return n == cap(e.buckets) && (n == 0 || !l.accepts(e.buckets[n-1], timestamp))
}

// makeRoom gives a full backing array room for one more bucket before an append,
// so that append never grows it on its own terms. It is off the hot path: an
// entry reaches it once per stretch of appends that fills the room it left.
//
// When the pruned prefix at the front of the array is at least 1/compactionRatio
// of the retained buckets, those are copied to the front: an entry that has
// filled its window slides forward one bucket per Precision, and compacting it
// in place costs at most compactionRatio words of copy per bucket appended and
// no allocation at all. Otherwise the retained buckets move to a larger array,
// sized by grownCapacity.
func (e *entry) makeRoom(windowBuckets int) {
	retained := e.buckets[e.head:]
	if e.head > 0 && e.head >= len(retained)/compactionRatio {
		e.buckets, e.head = e.buckets[:copy(e.buckets, retained)], 0
		return
	}
	grown := slices.Grow([]bucket(nil), grownCapacity(len(retained), windowBuckets))
	e.buckets, e.head = append(grown, retained...), 0
}

// grownCapacity is the capacity, before rounding up to a size class, that a full
// entry retaining n buckets grows to: what append would grow it to, except that
// a long entry does not grow past its window.
//
// append adds about a quarter to a long slice, so an entry filling a window of
// 1,800 buckets would land in the 2,560-word size class and carry 760 words it
// can never use, since pruning holds it at the window: 42% on top of its data,
// for every key of a long window. A window plus 1/compactionRatio fits the
// 2,048-word class instead and still leaves the entry the room makeRoom
// compacts into. An entry that retains more words than its window has buckets,
// because its buckets spill, is given the same proportion of room above what it
// retains. Short entries keep append's doubling, which pruneCopyMaxLen is tuned
// against.
func grownCapacity(n, windowBuckets int) int {
	if n < appendGrowthThreshold {
		return max(2*n, 1)
	}
	appendGrowth := n + (n+3*appendGrowthThreshold)/4
	bound := max(n, windowBuckets)
	return min(appendGrowth, bound+bound/compactionRatio+1)
}

// appendGrowthThreshold is the length at which the runtime's append stops
// doubling a slice and starts growing it by about a quarter.
const appendGrowthThreshold = 256

// compactionRatio bounds the copying makeRoom does: it compacts only when the
// pruned prefix is at least 1/compactionRatio of the retained buckets, and
// grownCapacity leaves at least that much room, so each word copied buys room
// for at least 1/compactionRatio of a bucket.
const compactionRatio = 8

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
