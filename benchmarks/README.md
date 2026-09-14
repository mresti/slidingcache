# Benchmarks

Results saved per PR so they can be compared with benchstat. The extension is
`.txt` (`*.out` is in .gitignore).

| File | What it is |
|---|---|
| `baseline-v1.2.0-serial.txt` | `^Benchmark(Store|Get|Sweep|Memory)` -count=10 -cpu=1 on main (v1.2.0) |
| `baseline-v1.2.0-parallel.txt` | `^BenchmarkParallel` -count=10 -cpu=4 on main (v1.2.0) |
| `*.benchstat.txt` | benchstat summary of a single file |
| `<pr>-serial.txt`, `<pr>-parallel.txt` | post-change results of that PR |
| `<pr>-vs-baseline-{serial,parallel}.txt` | `benchstat baseline <pr>` |
| `pr-b-highwater-serial.txt` | PR-B (HighWater) post, serial |
| `pr-b-highwater-parallel.txt` | PR-B post, parallel |
| `pr-b-highwater-vs-baseline-{serial,parallel}.txt` | PR-B vs baseline v1.2.0 |
| `pr-c-stats-serial.txt` | PR-C (Stats) post, serial |
| `pr-c-stats-parallel.txt` | PR-C post, parallel |
| `pr-c-stats-vs-baseline-{serial,parallel}.txt` | PR-C vs baseline v1.2.0 |

Generate the post results plus the comparison:
```
make test-bench-save PR=pr-c-stats
```
The target writes the header (go version, CPU, cores, commit, date) and then the
two `go test -bench` runs; afterwards it runs benchstat against the baseline.
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

The diagnostics benchmarks, `BenchmarkHighWater` and `BenchmarkStats`, are
deliberately named outside the `Store|Get|Sweep|Memory` families (like
`BenchmarkPruneCopyThreshold`), so they do not appear in these files; they are
measured separately:
```
go test -run '^$' -bench '^Benchmark(HighWater|Stats)$' -benchmem -cpu=1 .
```

Baseline machine: Apple M2 (8 cores), go1.27.0, darwin/arm64. Re-run the baseline
on the production host before comparing absolute figures.
