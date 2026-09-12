# Workload benchmarks

Reference numbers for the production workload the library is sized against, and
the before/after record for every change that touches the hot paths. Compare
any two records with:

```sh
go run golang.org/x/perf/cmd/benchstat@latest bench/<before>.txt bench/<after>.txt
```

Regenerate a record with `make bench-save` (raw output lands in `bench/<date>-<rev>.txt`).

## Workload model

- `Config{Precision: 1s, WindowSize: 1800s, EpochUnit: EpochInNanos}`, default 16 shards.
- 100k unique hashes (10-char decimal keys, like `strconv.FormatInt` over the
  production tag hash). Hottest key: 50k events/s. Rest: medium rates, ~1
  event/s each. Worst case = every key touched every second of the window.
- Defined in `workload_bench_test.go`. Full-window benchmarks run 180M stores
  per iteration (~2GB live buckets) and are designed for `-benchtime=1x` with a
  fresh cache per iteration; `make bench-save` runs everything with `-count=10`.

## Baseline v1.2.0 (rev 4471427, 2026-09-12)

Apple M2 (4P+4E), darwin/arm64, go1.27. Raw: `bench/2026-09-12-4471427.txt`.

| Benchmark | Result | Meaning |
|---|---|---|
| `WorkloadHotKey50KPerSec` | 25.2 ns/op, 0 B/op | 39.7M ev/s single core; the 50k ev/s hot key uses ~0.13% of one core |
| `Workload100KKeysParallel` (GOMAXPROCS=8) | 76.6 ns/op | 13.1M ev/s aggregate ingest over the full key set |
| `Workload100KKeysFullWindow` | 29.4 s per 180M stores | 6.1M ev/s serial while filling the worst-case 30-min window; fills it 29x faster than real time (worst-case required rate is 100k ev/s) |
| `WorkloadFutureJumpRejectStorm` | 3.1 ns/op | the +3h black-out: 100% of real-time ingest returns -1 from the pre-shard late check |
| `WorkloadFootprint` | 20,547 bytes/key, 1.96 GB live, 5.6 GB churn/fill | worst-case retained heap for 100k keys x 1800 buckets; plan ~2GB live, ~4GB RSS at GOGC=100, or set GOMEMLIMIT |

## The +3h future-jump behavior (verified)

One `Store` 3h ahead is a perfectly valid timestamp, so the library accepts it
and drags the **global** high-water mark forward for every key:

1. `HW` jumps `T0 -> T0+3h`; cutoff becomes `T0+9000s`.
2. Every real-time `Store`/`Get` (`t <= T0+9000s`) returns `-1`: nothing is
   stored, and existing entries are not even pruned (the late check precedes
   the prune), so the ~2GB stay resident.
3. First janitor sweep (default every 30 min) expires all buckets of all keys
   and deletes them: memory drops to ~0, counts are gone permanently.
4. First accepted real timestamp is `T0+9001s`: **2h30m of ingest discarded**
   (at the worst-case rate, ~22.5G events), plus up to 30 min of dead memory.

`StoreLateReject`'s 2-3 ns/op is the sound of that outage: cheap, silent, and
total. Mitigation tracked as the ingest-side skew guard (`guard` package) and
v1.3.0 observability (`HighWater()`, `Stats()`).

## PR-2: HighWater + Stats counters (rev db1952f, 2026-09-12)

Adds `Cache.HighWater()` and four global atomic counters (`Cache.Stats()`), one
atomic add per call. Raw: `bench/2026-09-12-db1952f.txt`, paired with the
PR-1 record `bench/2026-09-12-d360176.txt` (same library hot paths before the
counters).

| Benchmark | PR-1 | PR-2 | Delta |
|---|---|---|---|
| `WorkloadHotKey50KPerSec` | 24.71 ns/op | 24.55 ns/op | ~ |
| `Workload100KKeysParallel` (cpu=1) | 75.34 ns/op | 76.89 ns/op | +2.1% |
| `Workload100KKeysParallel` (cpu=8) | 67.51 ns/op | 80.55 ns/op | **+19.3%** |
| `WorkloadFutureJumpRejectStorm` | 3.03 ns/op | 3.95 ns/op | +30% (+0.9 ns absolute) |
| `Workload100KKeysFullWindow` | 29.54 s | 26.01 s | ~ |
| `WorkloadFootprint` | 27.34 s | 26.11 s | ~ |

Serial hot paths are unchanged; the counters add one shared cache line
written on every call, and with 8 concurrent writers that line ping-pongs:
+19.3% on the parallel workload, the only case past the 2-3% budget. The
absolute cost at the sizing workload's required rate (100k ev/s worst case)
is negligible.

**Proposed follow-up (not implemented):** move the four counters onto the
shards (each shard owns its counters, `Stats()` sums 16 shards' worth) so
writers contend only with the shard lock they already share; or accept the
global counters given the absolute cost. Decide when a read-heavy, many-writer
deployment actually feels it.
