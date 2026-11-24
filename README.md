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
- An approximate, fixed-memory implementation of the same interface lives in
  the `countminsketch` subpackage; see
  [Approximate counting with `countminsketch`](#approximate-counting-with-countminsketch).

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
	// returns the key's live count, that event included: 1 for a key with no
	// other live event on an exact implementation, possibly more on an
	// approximate one. Returns -1 if the event is late (its timestamp is outside
	// the live window) and was not stored, or if its bucket timestamp is
	// outside the range the implementation accepts (±2^(63−CountBits) seconds
	// for Cache); -2 if it lies further than MaxFutureSkew ahead of Clock().
	Store(epoch int64, keyInHash string) int
	// Get returns the live count for keyInHash within the sliding window
	// covering epoch: 0 for a key with no live event on an exact
	// implementation, possibly more on an approximate one. Returns -1 if epoch
	// itself is outside the live window, or its bucket timestamp is outside the
	// range the implementation accepts; -2 if it lies further than
	// MaxFutureSkew ahead of Clock().
	Get(epoch int64, keyInHash string) int
}
```

Two implementations exist: `slidingcache.Cache`, in this package, counts
exactly; `countminsketch.Cache` returns estimates in fixed memory, never below
the exact count and sometimes above it. Code that must work with both compares
a non-negative result with `>=` against a threshold, never with `== 0` or
`== 1`; see
[Approximate counting with `countminsketch`](#approximate-counting-with-countminsketch).

Both methods return the sentinel `-1` (`slidingcache.LateEvent`) when the
(converted, truncated) timestamp falls outside the live window, or outside the
representable epoch range; see
[Late events and the `-1` sentinel](#late-events-and-the--1-sentinel). On a cache
configured with `MaxFutureSkew`, both return `-2`
(`slidingcache.FutureEvent`) for a timestamp too far ahead of the clock; see
[Future events and the `-2` sentinel](#future-events-and-the--2-sentinel).

`New` returns a `*Cache`, which implements `SlidingCache` and additionally
exposes `Close() error` to stop the background janitor,
`HighWater() (int64, bool)` to read the window's anchor, and `Stats() Stats` to
read the per-outcome counters; see [Diagnostics](#diagnostics).

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

`HW` only ever moves **forward**, and it is never clamped, so an epoch in the
wrong unit drags the window forward with it. `Store` and `Get` reject outright,
with `-1` and **without touching `HW`**, any epoch whose bucket timestamp falls
outside `±2^(63−CountBits)` seconds (`±2^43` seconds, about ±278,000 years,
with the default `CountBits` of 20 — the range a bucket word represents). That
absorbs the grossest unit mistake: nanoseconds passed to a cache configured for
seconds land near `1.7e18` seconds and are rejected instead of bricking the
cache.

A wrong unit that still lands inside the range is not detectable and remains
destructive: milliseconds passed to a cache configured for seconds push `HW` to
the year 55,000 and make every subsequent real event look late, permanently.
There is no reset: the only recovery is to build a new cache with `New`. Feed
epochs of a single unit matching `Config.EpochUnit`.

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

The same `-1` covers the second rejection: an epoch whose bucket timestamp,
after conversion to seconds and truncation, falls outside `±2^(63−CountBits)`
seconds (`±2^43` seconds with the default `CountBits` of 20) does not fit the
packed bucket word, so `Store` refuses it before consulting the high-water mark
and `Get` refuses to query with it. The check applies to the converted
timestamp, not to the raw argument, so the usable range of `epoch` depends on
`EpochUnit`: with the default `CountBits`, `±2^43` seconds, `±2^43 * 1e3`
milliseconds, and the whole `int64` in nanoseconds (`±2^43` seconds already
exceeds it). See [`CountBits` and the epoch range](#countbits-and-the-epoch-range).

### Future events and the `-2` sentinel

`HW` only moves forward and is never clamped, so before v1.3.0 a single event
dated ahead of real time expired everything behind it. One event three hours in
the future, on a 30-minute window, pushed `HW` three hours ahead: every real
event that followed was `-1` until the wall clock caught up, two and a half
hours later, and the only recovery was to rebuild the cache.

`MaxFutureSkew` closes that hole. With a non-zero skew, an event whose bucket
timestamp lies further than the skew ahead of `Clock()` is **rejected**: it is
not stored, `HW` is left where it was, and `Store` returns `-2`
(`slidingcache.FutureEvent`). `Get` applies the same rule to the epoch it is
queried with, and never advances `HW` in any case.

```
reject iff  t > bucket(Clock()) + MaxFutureSkew
```

The clock reading is truncated to a `Precision` bucket before the comparison, so
an epoch inside the boundary bucket is still accepted whatever the precision.
The boundary itself is accepted; the first bucket past it is not.

**The guard is free in steady state.** Only an event beyond the current `HW` can
advance it, so only such an event is checked, and only such an event calls
`Clock()`. A continuous in-order stream reads the clock about once per
`Precision` bucket, not once per `Store`: an event landing in the current bucket,
an out-of-order arrival, a late event, and every `Get` below the mark pay
nothing at all. `Store` and `Get` remain allocation-free.

An event ahead of the clock but **inside** the skew is accepted and *does*
advance `HW`, so the skew also bounds how far a single misdated event can drag
the window: with `MaxFutureSkew: 5*time.Minute`, five minutes, not three hours.
Keep it small.

Other properties worth knowing:

- **A clock that steps backwards** (NTP correction) only tightens the guard
  while it is behind. `HW` never moves back, and events at or below `HW` are
  never re-checked, so a backward step cannot expire anything.
- **A clock ahead of the producers** makes the guard inert: everything is
  accepted, exactly as before v1.3.0.
- **Non-Unix epochs** need an injected `Clock` on the same time base. A nil
  `Clock` reads the Unix wall clock in `EpochUnit`; comparing that against
  epochs on another base would either reject everything or nothing.
- `MaxFutureSkew: 0` (the default) keeps the pre-v1.3.0 behavior: the misdated
  event is accepted and drags the window. `Clock` without a skew is rejected by
  `New` as dead configuration.

#### Caller-side defence

The cache guard is the last line, not the only one. Reject obviously misdated
events before they reach `Store`, so they are counted as what they are, and
treat both sentinels explicitly:

```go
const maxSkew = 5 * time.Minute

if ts > time.Now().Add(maxSkew).UnixNano() {
    metrics.FutureEvents.Inc()
    return
}

switch n := cache.Store(ts, key); n {
case slidingcache.FutureEvent: // -2: ahead of the clock, not stored.
    metrics.FutureEvents.Inc()
case slidingcache.LateEvent: // -1: expired or out of range, not stored.
    metrics.LateEvents.Inc()
default:
    handle(n) // n >= 1: the live count for the key.
}
```

Never treat a negative return as a count. The recommended configuration for a
high-throughput producer:

```go
cfg := slidingcache.Config{
    Precision:     time.Second,
    WindowSize:    30 * time.Minute,
    EpochUnit:     slidingcache.EpochInNanos,
    MaxFutureSkew: 5 * time.Minute,
    Shards:        128,
    SweepInterval: 2 * time.Minute,
}
```

### `Get` and the high-water mark

`Get` validates the caller's epoch against the current high-water mark but does
**not** advance it. Only `Store` moves the window forward. The window is always
anchored to the global high-water mark; the epoch passed to `Get` is used solely
to detect an out-of-window query, not to widen or shift the window. As a
consequence, `Get` with a very small epoch (for example `0`) against a cache
whose high-water mark has advanced past `WindowSize` returns `-1`.

## Diagnostics

### `HighWater()`

```go
func (c *Cache) HighWater() (hw int64, ok bool)
```

Reports the current high-water mark and whether a `Store` has ever advanced it.
A `Store` advances the mark before it is checked against the window, so `ok` can
be `true` even when that `Store` was then rejected as late by a concurrent, newer
one. On a cache that has never stored anything `ok` is `false` and the `hw`
returned with it is an internal sentinel below every usable epoch: it carries no
meaning and must not be used.

`hw` is **not** expressed in `Config.EpochUnit`. It is always a number of
**seconds** of bucket timestamp, truncated to `Precision`, whatever unit `Store`
and `Get` take. Multiply it by the unit to compare it against the epochs you
feed in:

| `EpochUnit` | epoch equivalent of `hw` |
|---|---|
| `EpochInSeconds` | `hw` |
| `EpochInMillis` | `hw * 1e3` |
| `EpochInNanos` | `hw * 1e9` |

The call is a single atomic load (~0.3 ns, no allocation) and is safe to make
concurrently with `Store` and `Get`. It reflects the mark at the instant of that
load, which a concurrent `Store` may already have moved forward.

**Typical use.** Export it as a gauge and watch its distance from the current
second:

```go
if hw, ok := cache.HighWater(); ok {
    metrics.HighWaterLagSeconds.Set(float64(time.Now().Unix() - hw))
}
```

- `now - hw` **growing** means nothing is advancing the window: the producer has
  stopped, or every event arrives already expired and is rejected with `-1`.
- `hw - now` **large** means the window has been dragged into the future by a bad
  timestamp — a wrong unit that still lands inside the representable range, or a
  badly skewed producer clock. The mark never moves backwards, so from that
  point every real event looks late and the only recovery is a new cache; see
  [High-water mark](#high-water-mark).

Alert on both directions: the first catches a stalled pipeline, the second
catches the poisoning that no amount of correct traffic undoes.

### `Stats()`

```go
func (c *Cache) Stats() Stats
```

Returns a snapshot of the counters the cache keeps for every outcome `Store` and
`Get` can have:

| Field | Type | Counts |
|---|---|---|
| `Accepted` | `uint64` | `Store` calls that recorded an event (returned a count `>= 1`). |
| `Late` | `uint64` | `Store` calls rejected as late (`-1`, timestamp at or below `HW − WindowSize`). |
| `Future` | `uint64` | `Store` calls rejected as too far ahead of the clock. Returned `-2`; stays `0` while `MaxFutureSkew` is disabled. |
| `OutOfRange` | `uint64` | `Store` calls rejected because the bucket timestamp is not representable (`-1`, beyond `±2^(63−CountBits)` seconds). |
| `GetHit` | `uint64` | `Get` calls that found the key, whatever count they returned — a key whose events have all expired is a hit that returns `0`. |
| `GetMiss` | `uint64` | `Get` calls for a key the cache does not hold (returned `0`). |
| `GetLate` | `uint64` | `Get` calls that returned `-1`, whether the epoch is outside the live window or not representable. Neither a hit nor a miss: the key is never looked up. |
| `GetFuture` | `uint64` | `Get` calls rejected as too far ahead of the clock (`-2`). Stays `0` while `MaxFutureSkew` is disabled. |
| `Keys` | `int` | Keys the shards currently hold, expired-but-not-yet-removed ones included. |

Every call increments exactly one counter, so the counters partition the traffic:

```
Accepted + Late + Future + OutOfRange == Store calls
GetHit + GetMiss + GetLate + GetFuture == Get calls
```

**Not an atomic snapshot.** `Stats` walks the shards one by one, so a burst of
concurrent traffic can be counted in the shards it visits late and missed in
those it visits early. Each counter is individually exact and the skew is bounded
by the duration of the call, which makes the result good for metrics and unfit as
a synchronization point. The counters are monotonic for the life of the cache and
there is no reset: take deltas between two calls to get rates.

**Cost.** The accepted paths pay a plain increment under the shard lock they
already hold; the rejections pay one atomic add on the shard that owns the key,
which means hashing the key to find that shard.
`Stats` itself takes each shard's lock in turn, briefly serializing against that
shard's operations — about 2.5 µs over 256 shards, allocation-free. Call it on a
metrics interval (10s or so), not per request.

**Typical export.** The eight counters are monotonic and map onto counter
metrics; `Keys` is a gauge:

```go
for range time.Tick(10 * time.Second) {
    s := cache.Stats()
    metrics.StoreAccepted.Set(float64(s.Accepted)) // a counter metric, exported as-is
    metrics.StoreLate.Set(float64(s.Late))
    metrics.StoreFuture.Set(float64(s.Future))
    metrics.StoreOutOfRange.Set(float64(s.OutOfRange))
    metrics.GetHit.Set(float64(s.GetHit))
    metrics.GetMiss.Set(float64(s.GetMiss))
    metrics.GetLate.Set(float64(s.GetLate))
    metrics.GetFuture.Set(float64(s.GetFuture))
    metrics.Keys.Set(float64(s.Keys)) // a gauge
}
```

What to alert on:

- **`Late` rate** climbing means events are arriving after their window closed:
  a lagging producer, a backed-up queue, or a high-water mark dragged forward by
  a bad timestamp. Cross-check with `HighWater()`, which distinguishes the two.
- **`OutOfRange` rate** above zero is almost always a unit mistake — nanoseconds
  fed to a cache configured for seconds. It should be flat at zero in a healthy
  deployment.
- **`Future` rate** is the mirror of `Late`: producers whose clocks run ahead.
- **`GetLate` rate** rising while `Late` is flat means readers are querying with
  stale epochs rather than writers producing them.
- **`Keys`** is the memory gauge. A steady climb means keys are being created
  faster than the janitor removes them; see [Memory management](#memory-management).

`Future` and `GetFuture` stay at `0` while `MaxFutureSkew` is disabled, so the
metrics can be wired up before it is set; once it is, they start counting
without any change to the export above.

## Configuration

| Field           | Type            | Required | Default            | Description |
|-----------------|-----------------|----------|--------------------|-------------|
| `Precision`     | `time.Duration` | yes      | —                  | Bucket granularity; timestamps are truncated to this. Must be `>= 1s` and a whole multiple of `time.Second`. |
| `WindowSize`    | `time.Duration` | yes      | —                  | Total sliding-window length. Must be `>= Precision` and a whole multiple of `time.Second`. |
| `EpochUnit`     | `EpochUnit`     | no       | `EpochInMillis` (0) | Unit of the `epoch` arguments: `EpochInMillis`, `EpochInNanos`, or `EpochInSeconds`. Defaults to milliseconds (`time.Now().UnixMilli()`). |
| `Shards`        | `int`           | no       | `16`               | Number of internal shards; rounded up to a power of two. Must be between `0` and `1048576` (`1<<20`). See [Choosing `Shards`](#choosing-shards). |
| `SweepInterval` | `time.Duration` | no       | `WindowSize`       | Period of the background janitor. May be sub-second (useful in tests). Must be `>= 0`. |
| `MaxFutureSkew` | `time.Duration` | no       | `0` (disabled)     | Furthest a bucket timestamp may lie ahead of `Clock()` before `Store` and `Get` reject it with `-2`. Must be `>= 0` and a whole multiple of `time.Second`. See [Future events and the `-2` sentinel](#future-events-and-the--2-sentinel). |
| `Clock`         | `func() int64`  | no       | Unix wall clock    | Current epoch in `EpochUnit`. Consulted only when `MaxFutureSkew > 0`, and only by an event that would advance the high-water mark. `nil` means `time.Now().UnixNano()`/`UnixMilli()`/`Unix()` per `EpochUnit`; setting it without `MaxFutureSkew` is an error. |
| `CountBits`     | `int`           | no       | `20`               | Width of the per-bucket event count inside a bucket word; the other `63−CountBits` bits hold the bucket timestamp in seconds. Must be `0` (the default) or between `8` and `24`. See [`CountBits` and the epoch range](#countbits-and-the-epoch-range). |

`Precision` and `WindowSize` must be positive, whole-second durations: sub-second
values (e.g. `500ms`) or non-whole-second values (e.g. `1500ms`) are rejected
rather than silently rounded. Internally they are converted once to integer
seconds, so there is no per-operation cost. `New` validates the configuration
and returns an error for any invalid value.

Epochs are validated per call rather than at configuration time: whatever the
`EpochUnit`, an `epoch` whose bucket timestamp lands outside
`±2^(63−CountBits)` seconds is rejected with `-1` and leaves the high-water mark
untouched. See [Late events and the `-1` sentinel](#late-events-and-the--1-sentinel).

### `CountBits` and the epoch range

A bucket word is one `int64`: the low `CountBits` bits hold how many events
landed in the bucket, the remaining `63−CountBits` bits hold the bucket
timestamp in seconds. `CountBits` moves that split, trading events per bucket
against the range of epochs the cache accepts:

| CountBits | events per bucket before spill | timestamp bits | Unix-seconds epochs usable until |
|-----------|-------------------------------|----------------|----------------------------------|
| 8 | 255 | 55 | year ~1.1e9 |
| 12 | 4,095 | 51 | year ~7.1e7 |
| 16 | 65,535 | 47 | year ~4.5e6 |
| 20 (default) | 1,048,575 | 43 | year 280,707 |
| 24 | 16,777,215 | 39 | year 19,391 |

The range is expressed in **seconds of bucket timestamp whatever the
`EpochUnit`**, and it is symmetric around the epoch: the cache accepts bucket
timestamps within `±2^(63−CountBits)` seconds. An epoch outside it makes `Store`
and `Get` return `-1` **without advancing the high-water mark**.

The upper bound of `24` keeps at least 39 timestamp bits, so every epoch an
`int64` of nanoseconds can express (up to 2262-04-11) still fits with room to
spare. Past it the timestamp range erodes toward dates real deployments reach —
32 count bits would stop at 2038-01-19 — which is why wider counts are
rejected.

Below the default, the timestamp range gained is one nobody needs, while the
spills bought are real: a bucket that outgrows its count continues into further
words with the same timestamp. That stays correct, but it costs memory. A key
taking 20,000 events per bucket needs 79 words per bucket at `CountBits: 8` —
about 185 KB over a 300-bucket window — against 2.3 KB at 16 or 20.

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
   A bucket is one 8-byte word holding both the timestamp and the count, so a
   key that never receives more than one event per bucket costs the same memory
   as a bare timestamp per event plus the entry's cached total, and every key
   above that rate pays less.
2. **Lazy pruning.** Every accepted `Store` prunes the touched key's expired
   prefix before inserting, so a hot key never accumulates dead buckets. Pruning
   never removes the key itself: an entry emptied by pruning is immediately
   refilled by the incoming event, and keys that stop receiving events are
   deleted only by the background sweep. `Get` is read-only: it discounts the
   expired prefix from the cached total, or sums the live buckets when they are
   fewer, and never mutates the cache. A key stored or read at least once per
   bucket has at most its oldest bucket expired, which both paths settle from
   the first two buckets without a search; a key left idle for a whole window
   costs a search and no scan.
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

## Approximate counting with `countminsketch`

The cache above keeps a list of buckets per key, so its memory grows with the
number of keys. The `countminsketch` subpackage implements the same
`SlidingCache` interface in **memory fixed at construction**: the window of each
shard is a ring of [count-min sketches](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch),
one per `Precision` bucket, so a `Store` or a `Get` touches a fixed number of
counters whatever the number of keys. The price is that counts are estimates:
never below the exact count, sometimes above it.

It is an opt-in alternative, not a replacement. Use it when keys are too many to
hold one by one **and** the caller compares counts against a threshold well
above the overestimate (see [Sizing](#sizing)). Stay on the exact cache when the
caller tests `== 0` or `== 1`, when thresholds are small against the traffic of
the whole window, or when the keys fit in memory: with few keys the sketch takes
more memory than the exact cache, not less.

```go
import (
	"fmt"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/countminsketch"
)

c, err := countminsketch.New(countminsketch.Config{
	Precision:  time.Second,
	WindowSize: time.Minute,
	EpochUnit:  slidingcache.EpochInSeconds,
})
if err != nil {
	panic(err)
}
defer c.Close()

const limit = 100
if c.Store(time.Now().Unix(), "client-42") >= limit {
	// Trips at the same event as the exact cache, or earlier.
	fmt.Println("over the limit")
}
```

`*countminsketch.Cache` implements `slidingcache.SlidingCache` and also has
`HighWater()`, `Stats()` and `Close()`. `Close` is a no-op kept for parity: no
goroutine runs, because nothing needs sweeping.

### What stays the same

The window semantics are those of the exact cache, checked against it call by
call in the tests: the mapping of epochs onto `Precision` buckets, the single
high-water mark that only `Store` advances, the strict liveness rule
(`t > HW − WindowSize`), out-of-order tolerance, the future guard
(`MaxFutureSkew`, `Clock`) and the `-1` and `-2` sentinels in the same cases.

### What changes

| Exact cache | `countminsketch` |
|---|---|
| Exact count. | Never below the exact count. With one shard, above it by more than `Epsilon × events in the window` with probability at most `Delta`. |
| First `Store` of a key returns `1`. | May return more than `1`. |
| Two `Store`s of a key at the same epoch return consecutive counts. | They can return the same count: only the key's smallest counters in its bucket are raised, and the window count may sit on another one. Only collisions cause it. |
| `Get` of an absent key returns `0`. | May return more than `0`. |
| Keys do not affect each other. | Events of other keys can raise the count of yours. They never lower it. |
| Memory grows with the number of keys; `Store` allocates for a new key. | Memory is fixed by `Epsilon`, `Delta` and `WindowSize / Precision`. `New` reserves all of it and nothing allocates afterwards. |
| Any `WindowSize / Precision`. | At most 2^20 buckets per window; `New` returns an error above that. |
| A bucket's count spills into further words. | A counter caps at 2^32−1 events per bucket. A `Store` whose smallest counter in the bucket is capped is still accepted, raises nothing and is counted in `Stats.Saturated`. |
| `Stats.Keys`; `GetHit` / `GetMiss` by presence of the key. | No `Keys`. `GetHit` is an estimate above `0`, `GetMiss` an estimate of `0`. |
| The hash only spreads keys over shards. | The hash decides correctness: two keys with the same 64-bit hash count as one. The default is `hash/maphash` with a seed drawn per cache. |
| `Store` stays within tens of ns at any traffic density. | Each new bucket costs an expiry per shard; see [Cost](#cost). |

Compare counts with `>=` against a threshold. A rate limiter that trips at
`count >= limit` trips at the same event as with the exact cache or earlier,
never later.

### Sizing

`Epsilon` and `Delta` size the sketch: `width = ceil(e / Epsilon)` columns by
`depth = ceil(ln(1 / Delta))` rows, at most 16 rows. The defaults, `0.001` and
`0.01`, give 2719 × 5.

**Accuracy.** The overestimate scales with the events of the **whole window**,
all keys together, not with the key's own count. With the default `Epsilon` and
`Delta`, one shard and one million live events, an estimate is within 1,000 of
the exact count with 99% probability. That is harmless against a limit of
100,000 and useless against a limit of 100. Choose `Epsilon` so that
`Epsilon × events in the window` is small against the threshold you compare
with. With more than one shard the bound holds on average over the keys; see
[Sketch configuration](#sketch-configuration).

A `Store` raises only the key's smallest counters in its bucket (the
conservative update), which never makes an estimate larger than raising all of
them would. The gain depends on how full the buckets are: a quarter to a half
of the overestimate goes once a bucket receives a few events per column of the
sketch, 10,000 per bucket with the default `Epsilon`, and little below one event
per column.

**Memory.** `New` reserves about `4 × width × depth` bytes per bucket:

| `Epsilon` | Sketch | 1 min at 1 s | 1 h at 1 min | 5 min at 1 s | 24 h at 1 min | 1 h at 1 s |
|---|---|---|---|---|---|---|
| `0.01` | 272 × 5 | 0.3 MB | 0.3 MB | 1.7 MB | 8.0 MB | 20.1 MB |
| `0.001` (default) | 2719 × 5 | 3.4 MB | 3.4 MB | 16.5 MB | 78.6 MB | 196.4 MB |
| `0.0001` | 27183 × 5 | 33.7 MB | 33.7 MB | 164.2 MB | 784.2 MB | 1,958.8 MB |

`Delta` at `0.01`, default `Shards`, 1 MB = 10^6 bytes. Memory is proportional to
the depth: `Delta` `0.1` is 3 rows, `0.001` is 7.

`New` estimates this before allocating anything and returns an error when the
estimate exceeds `Config.MaxMemoryBytes`, **1 GiB by default** (1,073.7 MB). The
last cell of the table needs the cap raised explicitly.

### Sketch configuration

`Precision`, `WindowSize`, `EpochUnit`, `MaxFutureSkew` and `Clock` follow the
rules of the exact cache. There is no `CountBits` and no `SweepInterval`.

| Field | Type | Default | Description |
|---|---|---|---|
| `Epsilon` | `float64` | `0.001` | Bound on the overestimate, as a fraction of the events in the window. Strictly between `0` and `1`. |
| `Delta` | `float64` | `0.01` | Probability that an estimate exceeds that bound. Strictly between `0` and `1`; at most 16 rows. |
| `Shards` | `int` | up to `16` | Rounded up to a power of two. The columns are divided among the shards, so a shard needs at least 8: the default drops below `16` for a large `Epsilon`, and an explicit value that leaves fewer is an error. |
| `MaxMemoryBytes` | `int64` | `1 << 30` | Most memory `New` may reserve for the window. |
| `TimestampBits` | `int` | `43` | Accepted range of bucket timestamps, `±2^TimestampBits` seconds; `39` to `62`. The default matches the exact cache's default `CountBits`. |
| `DisableRangeGuard` | `bool` | `false` | Same as `TimestampBits: 62`, the arithmetic ceiling. Cannot be combined with `TimestampBits`. |

`Shards` only buys concurrency. With more than one, `Epsilon` and `Delta` hold
on average rather than per key: a key that shares its shard with a very hot key
sees a fatter tail, noticeably so below about 40 columns per shard
(`e / (Epsilon × Shards)`). Memory barely depends on it: 256 shards take about
7% more than 16 with the default `Epsilon`.

`WithHashFunc` replaces the default hash. It must be deterministic and fixed for
the lifetime of the cache, and it is called on rejected events too.

### Cost

Measured on an Apple M2, default `Epsilon` and `Shards`, 1 h window at 1 s
(`benchmarks/countminsketch-depth5-*.txt`):

| | Exact | `countminsketch` |
|---|---|---|
| `Store`, 1,000 events per bucket (`StoreDense`) | 59.7 ns | 34.2 ns |
| `Store`, one hot key (`StoreHotKeyNanos`, 5 min window) | 17.4 ns | 20.1 ns |
| `Get`, many keys (`GetHitManyKeys`) | 28.0 ns | 16.6 ns |
| `Store`, a new bucket on every call (`StoreManyKeys`) | 33.4 ns | 742 ns |

The last row is the worst case. The first event a shard sees in a new bucket
expires that shard's oldest slot, which adds up to about 9 µs per bucket once
all 16 shards are active. Spread over the events of the bucket, that cost decides
the comparison. `BenchmarkStoreDensity` sweeps it with 100,000 keys, counting the
events of all keys together:

| Events per bucket | Exact | `countminsketch` |
|---|---|---|
| 1 | 34 ns | 747 ns |
| 10 | 34 ns | 462 ns |
| 30 | 36 ns | 271 ns |
| 100 | 36 ns | 116 ns |
| 300 | 44 ns | 56 ns |
| 1,000 | 61 ns | 34 ns |
| 10,000 | 70 ns | 26 ns |

The 1,000 row is the workload of `StoreDense` above; the exact cache reads 59
to 62 ns on it from run to run. The sketch is within 3× of the exact cache from
about 110 events per bucket and faster from about 450. An expiry clears `width × depth` counters, so those
thresholds move with `1 / Epsilon`.

A jump of the high-water mark expires one slot per bucket skipped, at most the
whole window: about 0.6 ms per shard for 1,800 buckets.

These figures are for the default `Delta`, 5 rows, which has a dedicated form of
the update. Other values of `Delta` use a generic form that measured 4 to 6 ns
more per `Store` at 5 rows.

## Testing

```sh
go test -race ./...
go test -bench=. -benchtime=1x ./...
```

The benchmark suite includes a memory-footprint benchmark that reports retained
heap as `bytes/key`.

The Makefile's bench and fuzz targets run the root package; `PKG` points them at
another one, a single package for the fuzz targets:

```sh
make test-bench PKG=./countminsketch
make test-fuzz-all PKG=./countminsketch FUZZTIME=30s
```

## License

See [LICENSE](./LICENSE).
