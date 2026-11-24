// Package countminsketch provides an approximate sliding-window event counter
// that implements slidingcache.SlidingCache with fixed memory.
//
// It keeps the window semantics of the exact cache in the root package: the same
// mapping of epochs onto Precision buckets, the same single high-water mark, the
// same strict liveness rule (a bucket timestamp t is alive if and only if
// t > HW - WindowSize), the same future guard, and the same LateEvent and
// FutureEvent sentinels returned in the same cases. What changes is how events
// are counted. Instead of a list of buckets per key, the window is a ring of
// count-min sketches, one per Precision bucket, plus an aggregate that holds
// their cell-wise sum, so a Store or a Get touches a fixed number of counters
// whatever the number of keys.
//
// # Guarantee
//
// An estimate is never below the exact count the root cache would return for
// the same sequence of calls. It exceeds that count by more than Epsilon times
// the number of events in the window with probability at most Delta.
//
// # Differences from the exact cache
//
// The estimate is an upper bound, and a few consequences follow that callers
// must design for:
//
//   - A Store for a key never seen before may return more than 1, and a Get for
//     an absent key may return more than 0.
//   - Events of other keys can inflate the count of yours: collisions only ever
//     add.
//   - Memory is fixed by Epsilon, Delta, WindowSize and Precision instead of
//     growing with the number of keys. With few keys it is larger than the exact
//     cache's.
//   - A window holds at most 2^20 Precision buckets; New rejects more.
//   - A counter caps at 2^32-1 events per bucket. A Store that meets a capped
//     counter is still accepted and is counted in Stats.Saturated; the estimate
//     of a key that hot is then low rather than wrapped.
//   - Stats has no key count, and GetHit and GetMiss are decided by the estimate,
//     not by the presence of the key.
//
// Callers must therefore compare counts against thresholds with >=, never with
// == 0 or == 1: a rate limiter that trips at count >= limit behaves as with the
// exact cache, only slightly earlier under collisions.
//
// A Cache is safe for concurrent use. It guards its window with a single mutex.
package countminsketch
