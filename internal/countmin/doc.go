// Package countmin implements a count-min sketch and a Ring of slot sketches
// that keeps their cell-wise sum, which is what a sliding-window counter needs:
// one sketch per time slot, and an aggregate that answers for the whole window
// without visiting every slot. A Ring stores all its slots in one flat array
// allocated up front.
//
// A sketch is depth rows of width counters. Every item raises one counter per row
// and its estimate is the smallest of those counters, so collisions can only
// inflate it: an estimate is never below the true count. With
// width = ceil(e/epsilon) and depth = ceil(ln(1/delta)), the estimate exceeds the
// true count by more than epsilon times the total with probability at most delta.
//
// Items are hashed once to 64 bits and every row derives its column from that
// hash by a multiply-shift with its own odd salt, so a caller that already holds
// a hash pays no further hashing. This scheme, one hash with a random odd salt
// per row, is adapted from the countmin package of github.com/nalgeon/unsure
// (MIT licensed); the code here is an independent implementation.
//
// None of the types in this package is safe for concurrent use.
package countmin
