package slidingcache

import "slices"

// word packs one Precision bucket of a key: the bucket timestamp in the high
// bits and the number of events that landed in it in the low bits. How the word
// is split is not fixed by this type; a bucketLayout decides it per Cache, and
// every read and write of a word goes through one.
//
// A word comes in two widths. An 8-byte word holds the whole timestamp. A
// 4-byte word holds only its low 32-CountBits bits, the timestamp modulo
// 2^(32-CountBits) seconds, and the entry's base supplies the rest: every
// timestamp an entry retains lies less than that span after its base, so
// subtracting the base's own low bits in the word's width, which wraps, gives
// the distance of a timestamp from the base whatever its high bits were. That
// holds for any window that spans at most 2^(32-CountBits) seconds, 4,096 at
// the default CountBits (see Config.wordBits), and halves the memory of every
// key. New picks one width per Cache, so no operation branches on it: entry and
// shard are generic over the word, and the compiler builds each of them once
// per width.
//
// Events that share a bucket are indistinguishable to the window semantics, so
// storing a count instead of one timestamp per event is lossless. Packing that
// count next to the timestamp instead of beside it in a struct halves the size
// of a bucket, and keeps ordering comparisons a plain integer comparison on the
// word taken relative to the base: the timestamp occupies the most significant
// bits, so a later timestamp always makes a larger word whatever the count.
//
// The count occupying the low bits also makes counting one more event into a
// bucket an increment of the word itself, which is what the record methods do
// once accepts has confirmed there is room; incrementing a full count would
// carry into the timestamp instead.
type word interface{ uint32 | uint64 }

// narrowWordBits and wideWordBits are the widths, in bits, of the two word
// types: uint32 and uint64.
const (
	narrowWordBits = 32
	wideWordBits   = 64
)

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
// timestamp and the count: the low countBits hold the count, the remaining bits
// of the word the bucket timestamp in seconds, or its low bits in a 4-byte word.
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
	// that Store and Get accept: the range an 8-byte word packs without
	// overflowing into the sign bit, whatever the width of the cache's words.
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

// newWord packs a bucket timestamp and an event count into the bits of an
// 8-byte word; a 4-byte word keeps the low half. The timestamp must be in
// range, which Store and Get guarantee by rejecting epochs outside it, and the
// count must not exceed maxCount.
//
// The layout's methods work on the bits of a word held in an int64 rather than
// on the word type, so that they are plain functions: the inliner charges a
// call from one generic method into another extra, which pushed the record
// methods past its budget, and the compiled code loads a dictionary for it.
//
// Every shift by countBits is masked to 63, which countBits never exceeds: an
// unmasked shift by a variable costs a compare and a select on each use, for
// the counts of 64 and more that Go defines and the hardware does not.
func (l bucketLayout) newWord(timestamp int64, count int) int64 {
	return timestamp<<(l.countBits&63) | int64(count)
}

// floor is the smallest word whose timestamp is timestamp: newWord with no
// events. The searches compare words against the floor of their target, and an
// entry's words are taken relative to the floor of its base.
func (l bucketLayout) floor(timestamp int64) int64 { return timestamp << (l.countBits & 63) }

// offset unpacks the timestamp of a word taken relative to another word, which
// wraps in the word's width: the distance between the two timestamps, in
// seconds, for a word no older than the other and less than a word's span
// after it, which the relative word holds as a non-negative number.
func (l bucketLayout) offset(relative int64) int64 { return relative >> (l.countBits & 63) }

func (l bucketLayout) count(w int64) int { return int(w & int64(l.maxCount)) }

func (l bucketLayout) full(w int64) bool { return l.count(w) == l.maxCount }

// sameBucket reports whether w holds the timestamp floor is the floor of, and
// accepts whether it also has room for one more event. Two words of one
// timestamp differ only in their count bits, so their XOR is then w's count,
// with no bit above them; of two different timestamps less than a word's reach
// apart, it has timestamp bits set, which make it exceed any count or, in the
// sign bit of an 8-byte word, negative. Both words are the bits of the same
// word type.
func (l bucketLayout) sameBucket(w, floor int64) bool { return (w^floor)>>(l.countBits&63) == 0 }

func (l bucketLayout) accepts(w, floor int64) bool {
	x := w ^ floor
	return x >= 0 && x < int64(l.maxCount)
}

// entry holds the buckets of a single key.
//
// The buckets the key retains are buckets[head:]. The words before head expired
// and were discounted from total by an earlier prune; they are kept, unread,
// only so that the entry can later compact its retained buckets into their room
// instead of reallocating (see makeRoom).
//
// Invariants, maintained by every method below:
//
//   - base is at most every timestamp in buckets[head:], and each of them lies
//     less than 2^(32-countBits) seconds after it in a 4-byte word, and less
//     than 2^(63-countBits), more than any window, in an 8-byte one. A Store
//     moves base to one past the cutoff it prunes with, which every timestamp
//     it keeps or records is after by less than the window, and New gives
//     4-byte words only to a window of at most 2^(32-countBits) seconds.
//   - buckets[head:] is sorted by timestamp, non-decreasing. Adjacent buckets
//     share a timestamp only when the earlier one is full, which is how a bucket
//     that outgrows the count of one word spills into the next. The words before
//     head are never read, so neither a search nor an insert stops before head.
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
type entry[W word] struct {
	buckets []W
	total   int
	head    int
	base    int64
}

// newEntry returns the entry of a key whose first event landed at timestamp,
// stored under cutoff.
func newEntry[W word](l bucketLayout, timestamp, cutoff int64) *entry[W] {
	return &entry[W]{buckets: []W{W(l.newWord(timestamp, 1))}, total: 1, base: cutoff + 1}
}

// rebase moves the base to one past cutoff, for a Store that has just pruned
// the entry under that cutoff: every timestamp the entry keeps is after it, and
// so is every one the Store can record. Moving the base touches no word, since
// a word keeps only the low bits of its timestamp, and keeping it this close
// lets a window of up to 2^(32-countBits) seconds decode without ambiguity in
// 4-byte words.
func (e *entry[W]) rebase(cutoff int64) { e.base = cutoff + 1 }

// timestamp unpacks the bucket timestamp of one of the entry's words: the base
// plus the offset of the word from the floor of the base, which the
// subtraction in the word's width recovers across the wrap of its low bits.
func (e *entry[W]) timestamp(l bucketLayout, w W) int64 {
	return e.base + l.offset(int64(w-W(l.floor(e.base))))
}

// insert records one event at timestamp.
//
// It is the composition of inOrder, recordInOrder and recordOutOfOrder. The hot
// path in shard.store calls those three directly instead of insert: inOrder and
// recordInOrder each fit the inliner's budget, so an in-order event is recorded
// without a call at all, whereas insert combines them with the out-of-order path
// and no longer fits, which would cost a call on every Store. Only the
// out-of-order path, which does not fit either, pays one.
func (e *entry[W]) insert(l bucketLayout, timestamp int64) {
	if e.inOrder(l, timestamp) {
		e.recordInOrder(l, timestamp)
		return
	}
	e.recordOutOfOrder(l, timestamp)
}

// inOrder reports whether timestamp is at least as new as every stored bucket,
// which includes repeats of the newest one. Such arrivals are recorded without
// searching or shifting anything.
//
// It unpacks the newest word in place, rather than through timestamp, for the
// reason newWord gives, as do the other methods inlined on the hot path.
func (e *entry[W]) inOrder(l bucketLayout, timestamp int64) bool {
	n := len(e.buckets)
	return n == 0 || timestamp-e.base >= l.offset(int64(e.buckets[n-1]-W(l.floor(e.base))))
}

// recordInOrder counts an event for which inOrder returned true: it increments
// the newest bucket when the event repeats it, and appends a new bucket
// otherwise, whether because the timestamp is new or because the newest bucket
// has no room left. A hot key therefore pays an increment per event rather than
// a slice growth, and its bucket count tracks elapsed time instead of event
// volume.
//
// The event lies within a word's span of the newest bucket, as every timestamp
// a Store records does of every one the entry keeps, so the newest word and the
// floor of the event compare as stored, base or not.
func (e *entry[W]) recordInOrder(l bucketLayout, timestamp int64) {
	e.total++
	floor := W(l.floor(timestamp))
	if n := len(e.buckets); n > 0 && l.accepts(int64(e.buckets[n-1]), int64(floor)) {
		e.buckets[n-1]++
		return
	}
	e.buckets = append(e.buckets, floor|1)
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
func (e *entry[W]) recordOutOfOrder(l bucketLayout, timestamp int64) {
	e.total++
	floor := W(l.floor(timestamp))
	switch {
	case e.timestamp(l, e.buckets[e.head]) >= timestamp:
		e.recordAt(l, floor, e.head)
	case e.evenlySpread(l):
		e.recordFromGuess(l, timestamp, floor)
	default:
		base := W(l.floor(e.base))
		e.recordAt(l, floor, e.bisect(e.head+1, len(e.buckets)-1, floor-base, base))
	}
}

// recordFromGuess is recordOutOfOrder on an evenly spread entry, for an event
// newer than its oldest bucket, whose floor is floor.
func (e *entry[W]) recordFromGuess(l bucketLayout, timestamp int64, floor W) {
	e.recordAt(l, floor, e.searchFromGuess(l, timestamp))
}

// recordAt counts an event into the first word from index i on that holds the
// timestamp floor is the floor of and has room, or into a new word at i when
// there is none; i is the first word at or after the timestamp. It is below len
// because the event is older than the newest bucket, so the new word can be
// inserted by a single append of the tail onto itself, which keeps recordAt
// within the inliner's budget and gives each of recordOutOfOrder's three
// lookups its own copy.
func (e *entry[W]) recordAt(l bucketLayout, floor W, i int) {
	for ; i < len(e.buckets) && l.sameBucket(int64(e.buckets[i]), int64(floor)); i++ {
		if !l.full(int64(e.buckets[i])) {
			e.buckets[i]++
			return
		}
	}
	e.buckets = append(e.buckets[:i+1], e.buckets[i:]...)
	e.buckets[i] = floor | 1
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
// is one too and the search can pack it relative to the base.
func (e *entry[W]) firstAlive(l bucketLayout, cutoff int64) int {
	newest := len(e.buckets) - 1
	switch {
	case e.aliveFrom(l, e.head, cutoff):
		return e.head
	case !e.aliveFrom(l, newest, cutoff):
		return newest + 1
	case e.evenlySpread(l):
		return e.searchFromGuess(l, cutoff+1)
	}
	base := W(l.floor(e.base))
	return e.bisect(e.head+1, newest, W(l.floor(cutoff+1))-base, base)
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
func (e *entry[W]) aliveFrom(l bucketLayout, i int, cutoff int64) bool {
	return i >= len(e.buckets) || e.base+l.offset(int64(e.buckets[i]-W(l.floor(e.base)))) > cutoff
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
func (e *entry[W]) liveCount(l bucketLayout, cutoff int64) int {
	switch {
	case e.aliveFrom(l, e.head, cutoff):
		return e.total
	case e.aliveFrom(l, e.head+1, cutoff):
		return e.total - l.count(int64(e.buckets[e.head]))
	}
	firstAlive := e.firstAlive(l, cutoff)
	if expired, alive := firstAlive-e.head, len(e.buckets)-firstAlive; alive < expired {
		return sumCounts(l, e.buckets[firstAlive:])
	}
	return e.total - sumCounts(l, e.buckets[e.head:firstAlive])
}

// sumCounts returns the number of events held in words.
func sumCounts[W word](l bucketLayout, words []W) int {
	sum := 0
	for _, w := range words {
		sum += l.count(int64(w))
	}
	return sum
}

// searchFromGuess returns the index of the first retained bucket with a
// timestamp >= target, for a target newer than the oldest retained bucket and no
// newer than the newest, on an evenly spread entry. The answer lies in
// (head, len-1]. Its callers settle the targets outside that range from the end
// words, comparing unpacked timestamps; within it the target lies less than a
// word's span after the base, so the search compares words relative to the
// base against the relative floor of the target and never unpacks a bucket.
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
// length, and a key that fills a long window holds a power-of-two array (8 KiB
// for 1,800 one-second buckets in 4-byte words, see grownCapacity), so over a
// population of such keys its first probes land in the same few cache sets and
// evict one another. The guess touches one or two lines next to the answer
// instead of about seven spread over the array.
func (e *entry[W]) searchFromGuess(l bucketLayout, target int64) int {
	low, high := e.head+1, len(e.buckets)-1
	base, targetFloor := W(l.floor(e.base)), W(l.floor(target))
	floor := targetFloor - base
	guess := e.interpolate(l, targetFloor)
	if e.buckets[guess]-base < floor {
		low = guess + 1
		for step := 1; step <= maxGallop && guess+step < high; step <<= 1 {
			if e.buckets[guess+step]-base >= floor {
				return e.bisect(low, guess+step, floor, base)
			}
			low = guess + step + 1
		}
		return e.bisect(low, high, floor, base)
	}
	high = guess
	for step := 1; step <= maxGallop && guess-step >= low; step <<= 1 {
		if e.buckets[guess-step]-base < floor {
			return e.bisect(guess-step+1, high, floor, base)
		}
		high = guess - step
	}
	return e.bisect(low, high, floor, base)
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
//
// It and interpolate measure the span from the oldest word's floor rather than
// from the base: the difference of two floors wraps in the word's width to the
// distance between their timestamps.
func (e *entry[W]) evenlySpread(l bucketLayout) bool {
	head, newest, counts := e.head, len(e.buckets)-1, W(l.maxCount)
	span := l.offset(int64(e.buckets[newest]&^counts - e.buckets[head]&^counts))
	words := int64(newest - head)
	return span-words <= words>>3 && words>>31 == 0
}

// interpolate returns where the target whose floor is targetFloor would sit
// among the retained buckets if their timestamps were evenly spread from the
// oldest to the newest: an index in [head, len-1] for a target in
// searchFromGuess's range.
func (e *entry[W]) interpolate(l bucketLayout, targetFloor W) int {
	head, newest := e.head, len(e.buckets)-1
	oldest := e.buckets[head] &^ W(l.maxCount)
	span := l.offset(int64(e.buckets[newest] - oldest))
	return head + int(l.offset(int64(targetFloor-oldest))*int64(newest-head)/span)
}

// maxGallop is the longest step searchFromGuess gallops from its guess before it
// bisects the rest: eight words, so the gallop stays within two cache lines of
// the guess.
const maxGallop = 8

// bisect returns the first index in [low, high) whose word, taken relative to
// base (the floor of the entry's base), is >= floor, or high when there is none.
// The words in that range are sorted, as every entry's are, and relative to the
// base they compare in that order across the wrap of their low bits.
//
// It is hand-rolled rather than delegating to slices.BinarySearchFunc so that it
// stays within the inliner's budget.
func (e *entry[W]) bisect(low, high int, floor, base W) int {
	for low < high {
		mid := int(uint(low+high) >> 1)
		if e.buckets[mid]-base < floor {
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
func (e *entry[W]) prune(l bucketLayout, cutoff int64) {
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
		e.buckets, e.head = append([]W(nil), alive...), 0
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
func (e *entry[W]) pruneSearching(l bucketLayout, cutoff int64) {
	lastExpired := e.firstAlive(l, cutoff) - 1
	e.total -= sumCounts(l, e.buckets[e.head:lastExpired])
	e.head = lastExpired
	e.prune(l, cutoff)
}

// needsRoom reports whether recording one more event at timestamp would append
// to a backing array that has no room left, which is when shard.store calls
// makeRoom first. Repeats of the newest bucket are increments and need none, so
// a hot key whose array happens to be full never reaches makeRoom.
func (e *entry[W]) needsRoom(l bucketLayout, timestamp int64) bool {
	n := len(e.buckets)
	return n == cap(e.buckets) && (n == 0 || !l.accepts(int64(e.buckets[n-1]), int64(W(l.floor(timestamp)))))
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
func (e *entry[W]) makeRoom(windowBuckets int) {
	retained := e.buckets[e.head:]
	if e.head > 0 && e.head >= len(retained)/compactionRatio {
		e.buckets, e.head = e.buckets[:copy(e.buckets, retained)], 0
		return
	}
	grown := slices.Grow([]W(nil), grownCapacity(len(retained), windowBuckets))
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
