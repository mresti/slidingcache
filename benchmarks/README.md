# Benchmarks

Results saved per PR so they can be compared with benchstat. The extension is
`.txt` (`*.out` is in .gitignore).

| File | What it is |
|---|---|
| `baseline-v1.2.0-serial.txt` | `^Benchmark(Store|Get|Sweep|Memory)` -count=10 -cpu=1 on main (v1.2.0) |
| `baseline-v1.2.0-parallel.txt` | `^BenchmarkParallel` -count=10 -cpu=4 on main (v1.2.0) |
| `baseline-v1.3.0-serial.txt` | `^Benchmark(Store|Get|Sweep|Memory)` -count=10 -cpu=1 on main (v1.3.0) |
| `baseline-v1.3.0-parallel.txt` | `^BenchmarkParallel` -count=10 -cpu=4 on main (v1.3.0) |
| `*.benchstat.txt` | benchstat summary of a single file |
| `<pr>-serial.txt`, `<pr>-parallel.txt` | post-change results of that PR |
| `<pr>-vs-baseline-{serial,parallel}.txt` | `benchstat baseline <pr>` |
| `pr-b-highwater-serial.txt` | PR-B (HighWater) post, serial |
| `pr-b-highwater-parallel.txt` | PR-B post, parallel |
| `pr-b-highwater-vs-baseline-{serial,parallel}.txt` | PR-B vs baseline v1.2.0 |
| `pr-c-stats-serial.txt` | PR-C (Stats) post, serial |
| `pr-c-stats-parallel.txt` | PR-C post, parallel |
| `pr-c-stats-vs-baseline-{serial,parallel}.txt` | PR-C vs baseline v1.2.0 |
| `pr-fast-path-live-count-{serial,parallel}.txt` | fast-path live count (O(1) cutoff check, scan-free idle `Get`) post |
| `pr-fast-path-live-count-vs-baseline-{serial,parallel}.txt` | fast-path live count vs baseline v1.3.0 |

Generate the post results plus the comparison:
```
make test-bench-save PR=pr-c-stats
```
The target writes the header (go version, CPU, cores, commit, date) and then the
two `go test -bench` runs; afterwards it runs benchstat against the baseline.
Changes made on top of v1.3.0 compare against its baseline instead:
```
make test-bench-save PR=<name> BASELINE=benchmarks/baseline-v1.3.0
```
Gate: delta <= 3% on StoreManyKeys, StoreHotKeyNanos, GetHitManyKeys and
ParallelStore; 0 allocs in Store/Get.

PR-B does not touch the hot path: its post results must match the baseline within
the noise.

PR-C adds counters to both paths — a plain increment under the shard lock that is
already held on the accepted path, and an atomic on the rejection paths only — so
it is measured against the same gate, with `ParallelStore` as the sensitive case
since it would expose false sharing. Two results are worth reading before the
table:

- `StoreLateReject` goes from 2.0 ns to 6.0 ns (+198%). It is not in the gate.
  The rejection paths now hash the key to find the shard that owns the counter,
  which is the cost the design deliberately moves off the accepted path.
- The parallel benchmarks come out **faster** than the baseline (`ParallelStore`
  -28%, at 16 shards -27%). The counters made the shard struct outgrow its
  64-byte size class, so fewer shards share a cache line and the default 16-shard
  configuration false-shares less than it used to. The effect fades as the shard
  count rises (`shards=256`: -0.9%), which is consistent with that reading.
  A first attempt that placed the lock-guarded counters after `keys` and `peak`
  put them on a second cache line and cost `ParallelGet` +25%; keeping every
  written field adjacent to `mu` removed it.

The fast-path live count change is measured against `baseline-v1.3.0`. It changes
how `prune` and `liveCount` find the first live bucket, so it shows on the rows
where a key holds many buckets: `StoreSingleKey` -25.8%,
`StoreAdvancingHighWater` -25.4%/-24.2%, `StoreHotKeyNanos` -16.7%,
`StoreOutOfOrder` -14.3%, `Sweep` -10.4%/-6.9%. The gate holds (`StoreManyKeys`
-1.3%, `GetHitManyKeys` -3.4%, `ParallelStore` within noise), allocations are
unchanged and `MemoryFootprint` stays at 195 bytes/key; `StoreFutureReject`
+0.3% is on a path the change does not touch. Most rows keep a few buckets per
key; on keys holding a 1,800-bucket window an in-order `Store` is about 40%
faster and a `Get` on an idle key 95%, which the PR description measures.

The diagnostics benchmarks, `BenchmarkHighWater` and `BenchmarkStats`, are
deliberately named outside the `Store|Get|Sweep|Memory` families (like
`BenchmarkPruneCopyThreshold`), so they do not appear in these files; they are
measured separately:
```
go test -run '^$' -bench '^Benchmark(HighWater|Stats)$' -benchmem -cpu=1 .
```

Baseline machine: Apple M2 (8 cores), go1.27.0, darwin/arm64. Re-run the baseline
on the production host before comparing absolute figures.

## countminsketch

| File | What it is |
|---|---|
| `countminsketch-phase3-serial.txt` | serial benchmarks of `./countminsketch` (-count=10 -cpu=1) after shards, the flat ring and the memory cap |
| `countminsketch-phase3-parallel.txt` | parallel benchmarks of `./countminsketch` (-count=10 -cpu=4), same commit |

The package's benchmarks mirror the root ones and run both implementations
through the `SlidingCache` interface: every mirrored row has an `impl=exact` and
an `impl=sketch` arm, so one file compares the two and needs no baseline. The
files come from the same two commands as the root archive, pointed at the
package, under the same two header lines:
```
go test -run '^$' -bench '^Benchmark(Store|Get|Sweep|Memory)' -benchmem -count=10 -cpu=1 ./countminsketch
go test -run '^$' -bench '^BenchmarkParallel' -benchmem -count=10 -cpu=4 ./countminsketch
```
Gates: 0 allocs/op on every `impl=sketch` row of Store and Get, and in
`MemoryFootprint` a `heap-bytes` within 5% of `estimate-bytes` that does not move
with the number of keys.

Medians of the 10 runs, ns/op (default Epsilon and Delta, 2719x5 over 16 shards;
1 h window at 1 s, except `StoreHotKeyNanos`, which uses a 5 min window):

| Benchmark | exact | sketch |
|---|---|---|
| `StoreDense` (1,000 events per bucket) | 62.1 | 35.6 |
| `StoreHotKeyNanos` | 17.6 | 20.8 |
| `StoreHotKeySameBucket` | 17.6 | 20.5 |
| `GetHit` | 16.6 | 16.4 |
| `GetHitManyKeys` | 27.7 | 16.7 |
| `StoreLateReject` | 7.4 | 8.4 |
| `StoreManyKeys` (a new bucket per op) | 33.6 | 765 |
| `StoreSingleKey` (a new bucket per op) | 24.4 | 342 |
| `StoreOutOfOrder` | 59.1 | 235 |
| `ParallelStoreDense` | 21.7 | 21.8 |
| `ParallelGet` | 16.0 | 15.5 |
| `ParallelMixed` | 12.7 | 43.6 |
| `ParallelStore` (a new bucket per op) | 18.0 | 159 |

- The sketch wins or ties where many events share a bucket and loses where each
  op opens a new one: the first event a shard sees in a new bucket expires that
  shard's oldest slot, 850 cells with these settings. `StoreHighWaterJump` prices
  it at 338 ns per slot and 559 µs for a whole 1,800-slot window.
- The cost does not depend on the number of buckets: `StoreWindowBuckets` dense
  reads 34.0 ns with 60 buckets and 35.0 with 1,800. The sparse arms, 451 and
  711 ns, differ by memory locality (a 3.3 MB ring against a 98 MB one), not by
  the walk.
- `ParallelStoreShards` on the sketch reads 159 / 145 / 241 ns at 16 / 64 / 256
  shards: on this benchmark, which opens a new bucket per op, more shards stop
  helping past 64. The cause was not profiled.
- `MemoryFootprint`: 196,591,872 `heap-bytes` against 196,409,600
  `estimate-bytes` (+0.09%), identical at 100K and 1M keys. Its 83 allocs/op are
  `New`'s; no other sketch row allocates.
- `ParallelGet/impl=exact` is bimodal in this file (13 to 18 ns).

Three earlier runs were discarded, one serial and two parallel: the machine was
in interactive use and the `-cpu=4` rows of the exact arm read up to twice these
figures. The archived runs were taken after that load was gone. Same machine as the baseline, go1.27.3.

The next archive of the package comes from the Makefile, pointed at it with
`PKG` and compared against this one with `BASELINE`:
```
make test-bench-save PR=<name> PKG=./countminsketch BASELINE=benchmarks/countminsketch-phase3
```
It runs the same two commands under the same two header lines and writes
`<name>-{serial,parallel}.txt` plus `<name>-vs-baseline-{serial,parallel}.txt`.
Here the baseline is the previous archive of the package, not the exact arm:
`BASELINE` must name an archive of the same `PKG`, and the default,
`baseline-v1.2.0`, is one of the root package.

`countminsketch-phase4-density.txt` holds `BenchmarkStoreDensity` alone
(-count=10 -cpu=1), the workload of `StoreDense` swept over the events per
one-second bucket, all keys together. It is the reproducer of the performance
target: the first event a shard sees in a new bucket expires a slot, and that
cost is spread over the events of the bucket.
```
go test -run '^$' -bench '^BenchmarkStoreDensity' -benchmem -count=10 -cpu=1 ./countminsketch
```
Medians, ns/op:

| Events per bucket | exact | sketch | sketch / exact |
|---|---|---|---|
| 1 | 33.8 | 757 | 22.4 |
| 10 | 33.8 | 466 | 13.8 |
| 30 | 36.4 | 277 | 7.6 |
| 100 | 36.3 | 121 | 3.3 |
| 300 | 43.2 | 56.2 | 1.3 |
| 1,000 | 58.3 | 34.6 | 0.59 |
| 10,000 | 70.0 | 26.5 | 0.38 |

The sketch is within 3x of the exact cache from about 110 events per bucket and
faster from about 450; 0 allocs/op on both arms. The benchmark is also part of
the serial archive from now on, since its name starts with `BenchmarkStore`.

`countminsketch-phase5-{serial,parallel}.txt` are the archive after the
conservative update (`Ring.Add` raises only the key's counters at the minimum of
its slot), and `countminsketch-phase5-vs-baseline-{serial,parallel}.txt` their
benchstat comparison with the Phase 3 archive. They are the first files produced
by the Makefile target pointed at the package:
```
make test-bench-save PR=countminsketch-phase5 PKG=./countminsketch BASELINE=benchmarks/countminsketch-phase3
```
Medians, ns/op, sketch arm (the exact arm did not change):

| Benchmark | phase 3 | phase 5 | exact |
|---|---|---|---|
| `StoreDense` | 35.6 | 40.2 | 59.2 |
| `StoreMediumCardinalitySameBucket/keys=10000` | 25.5 | 31.2 | 27.2 |
| `StoreHotKeyNanos` | 20.8 | 24.9 | 17.5 |
| `StoreHotKeySameBucket` | 20.5 | 24.8 | 17.6 |
| `StoreWindowBuckets/buckets=1800/density=dense` | 35.0 | 39.7 | n/a |
| `StoreManyKeys` | 765 | 760 | 33.6 |
| `GetHitManyKeys` | 16.7 | 16.6 | 27.9 |
| `ParallelStoreDense` | 21.8 | 23.5 | 21.5 |
| `ParallelStore` | 159 | 161 | 18.0 |

- A `Store` pays 4 to 6 ns where the counter update dominates (+13% to +23%) and
  nothing measurable where expiry does. `Get` does not change, nothing
  allocates, and `MemoryFootprint` reads the same 196,591,872 `heap-bytes`; its
  ns/op rises 16% because it times the Stores that fill the window.
- `StoreDensity` is in the serial file from this archive on. Medians, exact /
  sketch: 1 event per bucket 33.9 / 752; 10: 33.5 / 469; 30: 36.0 / 280; 100:
  36.0 / 123; 300: 42.1 / 61.9; 1,000: 61.8 / 40.4; 10,000: 69.7 / 31.8. The
  sketch is within 3x of the exact cache from about 120 events per bucket and
  faster from about 550 (about 110 and 450 before the conservative update).
- `StoreDense/impl=exact` and `StoreDensity/events=1000/impl=exact` spread 16%
  in this file; their medians, 59.2 and 61.8, are the same workload.

`countminsketch-depth5-{serial,parallel}.txt` are the archive after `Ring.Add`
gained a straight-line path for depth 5, the default, and
`countminsketch-depth5-vs-baseline-{serial,parallel}.txt` their benchstat
comparison with the Phase 5 archive:
```
make test-bench-save PR=countminsketch-depth5 PKG=./countminsketch BASELINE=benchmarks/countminsketch-phase5
```
Medians, ns/op, sketch arm:

| Benchmark | phase 3 (classic) | phase 5 | depth-5 path | exact |
|---|---|---|---|---|
| `StoreDense` | 35.6 | 40.2 | 34.2 | 59.7 |
| `StoreMediumCardinalitySameBucket/keys=10000` | 25.5 | 31.2 | 25.1 | 27.4 |
| `StoreHotKeyNanos` | 20.8 | 24.9 | 20.1 | 17.4 |
| `StoreHotKeySameBucket` | 20.5 | 24.8 | 19.9 | 17.5 |
| `StoreWindowBuckets/buckets=1800/density=dense` | 35.0 | 39.7 | 34.6 | n/a |
| `StoreManyKeys` | 765 | 760 | 742 | 33.4 |
| `GetHitManyKeys` | 16.7 | 16.6 | 16.6 | 28.0 |
| `ParallelStoreDense` | 21.8 | 23.5 | 22.1 | 21.5 |

- The conservative update no longer costs anything measurable at the default
  depth: every serial row is back at, or slightly under, its Phase 3 figure, and
  `ParallelStoreDense` is within its 11% spread of it.
- `StoreDensity`, exact / sketch: 1 event per bucket 34.0 / 747; 10: 33.6 / 462;
  30: 36.0 / 271; 100: 36.4 / 116; 300: 43.7 / 55.8; 1,000: 60.8 / 34.4; 10,000:
  69.9 / 26.2. The sketch is within 3x of the exact cache from about 110 events
  per bucket and faster from about 450, as before the conservative update.
- Every benchmark of this package runs at depth 5. Other depths take the generic
  path, whose only rows are `RingAdd` at 2719x7 in `internal/countmin` (not
  archived): 16.8 ns before this change and 17.1 after, the cost of one call.
