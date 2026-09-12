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

## PR-1: ingest skew guard (rev d360176, 2026-09-12)

The `guard` package rejects out-of-skew epochs before the cache sees them:
`epoch > now+MaxFutureSkew` → `-2`, `epoch < now-MaxPastSkew` → `-1`, both
opt-in via `Config.Enabled` (disabled = the cache itself, zero overhead).
Raw: `bench/2026-09-12-d360176.txt` (workload rows statistically unchanged vs
the v1.2.0 baseline; the guard is a separate package and never touches the
library hot path).

| Benchmark | Result | Meaning |
|---|---|---|
| `RawCacheStoreRealClock` | 48.7 ns/op | raw cache + one `time.Now()` per call, no guard |
| `GuardStoreRealClock` | 81.8 ns/op | guard adds its own clock read + 2 integer comparisons (~33 ns) |
| `GuardStoreFakeClock` | 21.1 ns/op | pinned clock: guard overhead ~0 vs the raw 19.7-25 ns same-bucket path |
| `GuardGetFakeClock` | 17.7 ns/op | read path, same shape |

The ~33 ns per call is the guard's own `time.Now()`; the two skew comparisons
are noise-level. Against the 2.5h full-ingest blackout of an unguarded +3h
jump, that is the trade the guard exists to buy.
