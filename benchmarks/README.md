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
| `pr-entry-compaction-{serial,parallel}.txt` | entry compaction (bounded growth, in-place compaction) post |
| `pr-entry-compaction-vs-baseline-{serial,parallel}.txt` | entry compaction vs baseline v1.3.0 |
| `pr-entry-compaction-vs-pr-fast-path-live-count-{serial,parallel}.txt` | entry compaction vs fast-path live count, the PR it is stacked on |
| `pr-search-aliasing-{serial,parallel}.txt` | bucket search from a guess on evenly spread keys post |
| `pr-search-aliasing-vs-baseline-{serial,parallel}.txt` | search from a guess vs baseline v1.3.0 |
| `pr-search-aliasing-vs-pr-entry-compaction-{serial,parallel}.txt` | search from a guess vs entry compaction, the PR it is stacked on |

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

The entry compaction change is stacked on the fast-path one, so besides
`baseline-v1.3.0` it is compared with the fast-path files, which isolates its own
effect. It is a memory change: a key that fills a long window settles in a
smaller array and slides it without allocating, which shows as `B/op` 26 -> 0 on
`StoreSingleKey` and `StoreAdvancingHighWater` and 18 -> 0 on `StoreOutOfOrder`,
and as `StoreSingleKey` -16.1% and `StoreAdvancingHighWater` -15.4%/-14.4%
against the fast-path files. What it costs: `entry` moves from the 32-byte to the
48-byte size class, so `MemoryFootprint` goes from 195 to 211 bytes/key (+8.2%),
and the rows that spread their stores over many small keys are 3-7% slower than
the fast-path files (`StoreSteadyState`, `StoreMediumCardinalitySameBucket`,
`StoreSteadyStateWithSkew`, `Sweep`). Against v1.3.0 the gate holds
(`StoreManyKeys` +1.6%, `StoreHotKeyNanos` -17.4%, `GetHitManyKeys` -2.1%,
`ParallelStore` within noise); the largest regressions left are
`StoreMediumCardinalitySameBucket/keys=1000` +3.6% and
`StoreSteadyStateWithSkew` +1.8% to +3.0%. `ParallelStore-4`'s runs split
between about 13 and 18 ns with the share of its stores refused as late; at an
equal share this change costs about 2% there.

The search change is stacked on entry compaction and compared with its files as
well. It adds `BenchmarkGetFullWindowKeys` and `BenchmarkStoreFullWindowKeys`:
10,000 keys each holding a full 30-minute window of one-second buckets, the
16 KiB array class, read three seconds after their last write or after a whole
idle window, and written in three-second batches or out of order. None of the
earlier files has them, so the comparisons show them on one side only; measured
interleaved in one session against entry compaction's code with the same
benchmarks compiled in, they are `lagging` -50%, `idle` -57%, `out-of-order`
-62% and `batched` -40%. The rows that never search stay within noise or get
faster (`StoreSteadyState` -2.4% to -6.1%, `StoreSteadyStateWithSkew` -3.1% to
-5.0%, `StoreAdvancingHighWater` -2.5%, `StoreManyKeys` -2.1%), since `prune`'s
common path lost the inlined search; `StoreHotKeySameBucket` +0.6%,
`StoreMediumCardinalitySameBucket/keys=100` +1.4% and `StoreSingleKey` +0.4%
are the largest moves up. Against v1.3.0 the gate holds (`StoreManyKeys` -0.6%,
`StoreHotKeyNanos` -17.2%, `GetHitManyKeys` -2.2%, `ParallelStore` within
noise), allocations are unchanged and `MemoryFootprint` stays at 211
bytes/key.

The diagnostics benchmarks, `BenchmarkHighWater` and `BenchmarkStats`, are
deliberately named outside the `Store|Get|Sweep|Memory` families (like
`BenchmarkPruneCopyThreshold`), so they do not appear in these files; they are
measured separately:
```
go test -run '^$' -bench '^Benchmark(HighWater|Stats)$' -benchmem -cpu=1 .
```

Baseline machine: Apple M2 (8 cores), go1.27.0, darwin/arm64. Re-run the baseline
on the production host before comparing absolute figures.
