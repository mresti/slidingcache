# slidingcache

A sharded, concurrency-safe **sliding-window event counter** for Go.

`slidingcache` records events per key and reports, at any moment, how many of a
key's events still fall inside a fixed-length sliding window. It is built for
high write throughput, out-of-order tolerance, and a small, bounded memory
footprint even under heavy churn of short-lived keys.

- Zero external dependencies (standard library only).
- Lock-sharded for concurrent access; safe under `-race`.
- Lazy pruning on `Store` plus a background janitor that returns memory to the
  garbage collector; `Get` is read-only and never mutates the cache.

## Install

```sh
go get github.com/mresti/slidingcache
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/mresti/slidingcache"
)

func main() {
	cache, err := slidingcache.New(slidingcache.Config{
		Precision:  1 * time.Second,  // 1-second buckets
		WindowSize: 60 * time.Second, // retain events for 60 seconds
		EpochUnit:  slidingcache.EpochInMillis,
	})
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	now := time.Now().UnixMilli()

	fmt.Println(cache.Store(now, "user-42")) // 1
	fmt.Println(cache.Store(now, "user-42")) // 2
	fmt.Println(cache.Get(now, "user-42"))   // 2

	// 61 seconds later the earlier events have fallen out of the window.
	later := now + 61_000
	cache.Store(later, "user-42")          // advances the high-water mark
	fmt.Println(cache.Get(later, "user-42")) // 1
}
```

## Public interface

```go
type SlidingCache interface {
	// Store records an event for keyInHash at the window containing epoch and
	// returns the resulting live count. Returns -1 if the event is late (its
	// timestamp is outside the live window) and was not stored.
	Store(epoch int64, keyInHash string) int
	// Get returns the live count for keyInHash within the sliding window
	// covering epoch, or 0 if the key does not exist. Returns -1 if epoch itself
	// is outside the live window.
	Get(epoch int64, keyInHash string) int
}
```

Both methods return the sentinel `-1` when the (converted, truncated) timestamp
falls outside the live window; see [Late events and the `-1` sentinel](#late-events-and-the--1-sentinel).

`New` returns a `*Cache`, which implements `SlidingCache` and additionally
exposes `Close() error` to stop the background janitor.

> **Naming note.** A more idiomatic Go API would be an interface named
> `SlidingWindowCounter` with methods `Add(epoch, key) int` and
> `Count(epoch, key) int`. The `SlidingCache` interface with `Store`/`Get` is
> kept here for compatibility and its signatures are stable.

## Semantics

### High-water mark

The cache keeps a single **global high-water mark** `HW`, defined as the maximum
epoch (converted to seconds) ever observed by `Store`. All timestamps are
converted to seconds and truncated to `Precision` before storage.

An event with truncated timestamp `t` is **alive** if and only if:

```
t > HW - WindowSize
```

The boundary is strict (`>`, not `>=`): an event exactly `WindowSize` seconds
behind the high-water mark has expired. Because `Store` advances `HW` to at
least the epoch `E` of the call before evaluating liveness, this rule is always
at least as inclusive as the intuitive window `t >= E - WindowSize` anchored at
the call itself.

The live count for a key is the number of its events with an alive timestamp.
Events that share a truncated timestamp are counted individually but stored
once, as a bucket with a count, so a key's memory depends on the window length
and not on its event rate.

`HW` only ever moves **forward**, and it is never clamped. A single `Store` with
an epoch in the wrong unit — nanoseconds passed to a cache configured for
milliseconds, say — pushes `HW` far into the future and makes every subsequent
real event look late, permanently. There is no reset: the only recovery is to
build a new cache with `New`. Feed epochs of a single unit matching
`Config.EpochUnit`.

A cache on which `Store` has never been called starts with its high-water mark
below every usable epoch, so its first event is accepted whatever its value;
`Get` before any `Store` returns `0` rather than the `-1` sentinel.

Negative epochs are supported. Truncation to a `Precision` bucket rounds toward
negative infinity, so buckets have a uniform width on both sides of zero.

### Out-of-order tolerance

Events may arrive in any order. An event is counted into the bucket of its
truncated timestamp, located by binary search in the key's sorted bucket list,
so an older-but-still-live event stored after a newer one is counted normally.

### Late events and the `-1` sentinel

An event that is already expired relative to the current high-water mark
(`t <= HW - WindowSize`) is **rejected**: it is not stored, existing live events
for the key are left untouched, and `Store` returns `-1` to signal "late event,
not stored". This keeps already-closed windows immutable and avoids resurrecting
stale keys.

`Get` applies the symmetric rule to the caller's own epoch: if the queried
timestamp is outside the live window (`t <= HW - WindowSize`), `Get` returns
`-1` ("queried epoch is outside the live window"). Otherwise it returns the
live count, or `0` if the key does not exist.

Because the boundary is strict, a timestamp exactly `WindowSize` seconds behind
the high-water mark yields `-1`. Callers should treat any negative return value
as the late/out-of-window sentinel rather than a count.

### `Get` and the high-water mark

`Get` validates the caller's epoch against the current high-water mark but does
**not** advance it. Only `Store` moves the window forward. The window is always
anchored to the global high-water mark; the epoch passed to `Get` is used solely
to detect an out-of-window query, not to widen or shift the window. As a
consequence, `Get` with a very small epoch (for example `0`) against a cache
whose high-water mark has advanced past `WindowSize` returns `-1`.

## Configuration

| Field           | Type            | Required | Default            | Description |
|-----------------|-----------------|----------|--------------------|-------------|
| `Precision`     | `time.Duration` | yes      | —                  | Bucket granularity; timestamps are truncated to this. Must be `>= 1s` and a whole multiple of `time.Second`. |
| `WindowSize`    | `time.Duration` | yes      | —                  | Total sliding-window length. Must be `>= Precision` and a whole multiple of `time.Second`. |
| `EpochUnit`     | `EpochUnit`     | no       | `EpochInMillis` (0) | Unit of the `epoch` arguments: `EpochInMillis`, `EpochInNanos`, or `EpochInSeconds`. Defaults to milliseconds (`time.Now().UnixMilli()`). |
| `Shards`        | `int`           | no       | `16`               | Number of internal shards; rounded up to a power of two. Must be between `0` and `1048576` (`1<<20`). See [Choosing `Shards`](#choosing-shards). |
| `SweepInterval` | `time.Duration` | no       | `WindowSize`       | Period of the background janitor. May be sub-second (useful in tests). Must be `>= 0`. |

`Precision` and `WindowSize` must be positive, whole-second durations: sub-second
values (e.g. `500ms`) or non-whole-second values (e.g. `1500ms`) are rejected
rather than silently rounded. Internally they are converted once to integer
seconds, so there is no per-operation cost. `New` validates the configuration
and returns an error for any invalid value.

### Choosing `Shards`

Every operation locks the shard that owns its key, so under concurrent write
load the shard count — not the global high-water mark — is the main contention
point. Measured with `BenchmarkParallelStoreShards` (Apple M2, 4 cores, 10k
keys, `benchstat` over 10 runs, ±1-4%):

| Shards | ns/op | Speedup vs 16 |
|--------|-------|---------------|
| 16 (default) | 26.7 | 1.00x |
| 64           | 17.0 | 1.57x |
| 256          | 14.8 | 1.80x |

**Recommendation:** with multi-core writers, set `Shards: 64` or higher — the
default of 16 leaves nearly half of the write throughput on the table. The
per-shard overhead is tiny (a map and a mutex), so oversizing is cheap. The
default is fine for single-writer or low-concurrency use.

### Custom hash

Keys are assigned to shards by a 64-bit hash. The default is **FNV-1a** —
allocation-free, deterministic, and inlined on the hot path. Override it with the
`WithHashFunc` option, for example to use `hash/maphash` with a per-process seed
so the shard mapping is not predictable from outside the process (useful when
keys are attacker-influenced and you want to avoid deliberate shard skew):

```go
import "hash/maphash"

seed := maphash.MakeSeed() // fixed for the cache's lifetime

c, err := slidingcache.New(cfg, slidingcache.WithHashFunc(
    func(key string) uint64 {
        return maphash.String(seed, key)
    },
))
```

The hash **must be deterministic and fixed for the lifetime of the cache**:
changing the key-to-shard mapping after events are stored would misroute later
`Store`/`Get` calls to the wrong shard. Passing a `nil` function makes `New`
return an error. Omitting the option keeps the default FNV-1a.

## Memory management

Go maps and slices never shrink their backing storage on their own, so a naive
counter accumulates memory under churn. `slidingcache` keeps its footprint
bounded through four mechanisms:

1. **Counted buckets.** A key stores one entry per distinct truncated timestamp,
   holding the number of events that landed in it, so a key never holds more
   than `WindowSize / Precision` buckets no matter how many events per second it
   receives. Repeated events cost an increment rather than memory, and the
   entry's cached total keeps `Store` returning the live count without a scan.
   A bucket is twice the size of a bare timestamp, so a key that never receives
   more than one event per bucket pays about twice the memory it used to; every
   key above that rate pays less, and the worst case stops growing with traffic.
2. **Lazy pruning.** Every accepted `Store` prunes the touched key's expired
   prefix before inserting, so a hot key never accumulates dead buckets. Pruning
   never removes the key itself: an entry emptied by pruning is immediately
   refilled by the incoming event, and keys that stop receiving events are
   deleted only by the background sweep. `Get` is read-only: it discounts the
   expired prefix from the cached total, located by binary search, and never
   mutates the cache.
3. **Background sweep.** A janitor goroutine (a `time.Ticker`) periodically scans
   all shards, prunes expired buckets, and deletes keys left without events.
4. **Right-sizing and map compaction.** When a key's bucket slice has a backing
   array much larger than its live contents, the survivors are copied into a
   right-sized slice so the large array can be collected instead of being pinned
   by a re-slice. During a sweep, if a shard's live key count has fallen well
   below its observed peak, the shard's map is rebuilt into a fresh, right-sized
   map to release hash-bucket memory to the garbage collector.

Call `Close` to stop the janitor when the cache is no longer needed. `Close` is
idempotent and safe to call concurrently.

## Testing

```sh
go test -race ./...
go test -bench=. -benchtime=1x ./...
```

The benchmark suite includes a memory-footprint benchmark that reports retained
heap as `bytes/key`.

## License

See [LICENSE](./LICENSE).
