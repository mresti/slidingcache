# Plan v1.3: guard de futuro, HighWater/Reset, Stats

Base: `main` == v1.2.0 (4471427). 3 PRs independientes (cada uno parte de `main`), merge secuencial A -> B -> C.
Sin commits sin confirmación. Cada PR guarda benchmarks pre/post en `benchmarks/` (extensión `.txt`; `*.out` está en .gitignore).

## Datos de entrada (respuestas)
- 99% keys medias: 10K ev/key en 30 min -> 5.5 ev/s -> saturan los 1800 buckets/key.
  **RAM = peor caso: ~18 KB/key -> 100K keys ≈ 1.8-2.0 GB live, 3.6-4 GB RSS (GOGC=100).** Poner `GOMEMLIMIT`.
- 4 goroutines / 4 cores -> cache ≈ 18-22 M Store/s con Shards>=64. `hashstructure.Hash` (1-5 µs) limita a ~1-4 M ev/s. Cache no es cuello.
- No aceptable perder 30 min -> el guard de futuro NO debe tocar HW ni requerir recrear cache. `Reset()` queda solo como herramienta ops.
- Fix en lib + guard en caller. `Get/Store == -1` = pasado, `-2` = futuro.

## Invariantes de rendimiento (gate de cada PR)
- Store/Get hot path: 0 allocs, delta <= 3% en `StoreManyKeys`, `StoreHotKeyNanos`, `GetHitManyKeys`, `ParallelStore` (benchstat count=10, cpu=1 serial / cpu=4 parallel).
- Ningún `time.Now()` por Store en estado estable.

## Flujo de benchmarks por PR
```
# pre (ya generado en main): benchmarks/baseline-v1.2.0-{serial,parallel}.txt + .benchstat.txt
# post (en la rama del PR):
go test -run '^$' -bench '^Benchmark(Store|Get|Sweep|Memory)' -benchmem -count=10 -cpu=1 . > benchmarks/<pr>-serial.txt
go test -run '^$' -bench '^BenchmarkParallel' -benchmem -count=10 -cpu=4 . > benchmarks/<pr>-parallel.txt
go run golang.org/x/perf/cmd/benchstat@latest benchmarks/baseline-v1.2.0-serial.txt benchmarks/<pr>-serial.txt > benchmarks/<pr>-vs-baseline-serial.txt
go run golang.org/x/perf/cmd/benchstat@latest benchmarks/baseline-v1.2.0-parallel.txt benchmarks/<pr>-parallel.txt > benchmarks/<pr>-vs-baseline-parallel.txt
```
Añadir target `make test-bench-save PR=<pr>` que haga lo anterior (PR-A lo introduce; B y C lo reutilizan; si B/C van en paralelo, duplican el target y se resuelve en merge).

---

## PR-A: `feat: reject future events without advancing the high-water mark`

### API
```go
type Config struct {
    // ...
    // MaxFutureSkew: distancia máxima por delante de Clock() que puede tener un
    // bucket. 0 = desactivado (comportamiento v1.2.0). Debe ser múltiplo de 1s.
    MaxFutureSkew time.Duration
    // Clock devuelve el epoch actual en EpochUnit. Solo se usa si MaxFutureSkew > 0.
    // nil = reloj de pared Unix en EpochUnit (time.Now().UnixNano()/UnixMilli()/Unix()).
    Clock func() int64
}

const (
    LateEvent   = -1 // ya existía como lateEvent; se exporta
    FutureEvent = -2 // nuevo: bucket > Clock()+MaxFutureSkew
)
```
- `Store`: si `ts > HW` (único caso que puede avanzar HW) y `MaxFutureSkew>0` y `ts > bucket(Clock())+skew` -> return `FutureEvent` **sin CAS**. Coste 0 en estado estable: `Clock()` solo se llama cuando el evento adelantaría HW (≈1 vez/segundo/precision con tráfico continuo).
- `Get`: misma regla -> `FutureEvent`. No toca HW (ya no lo hacía).
- Orden de chequeos: `inRange` -> futuro -> avanzar HW -> tardío. El chequeo de futuro va antes de `advanceHighWater`.
- Validación: `MaxFutureSkew < 0` o no múltiplo de 1s -> error. `Clock != nil` con `MaxFutureSkew == 0` -> error (config muerta).
- Semántica documentada: un evento futuro dentro del skew SÍ avanza HW (skew pequeño, 1-5 min, expira como mucho ese tramo).

### Archivos
- `slidingcache.go`: Config, validate, New (defaultClock por unit), Store/Get, constantes exportadas, doc de paquete (sección "Future events").
- `slidingcache_test.go`: nuevos tests.
- `slidingcache_bench_test.go`: nuevos benches.
- `README.md`: sección "Future events and the -2 sentinel", tabla Config, sección "Defensa en el caller" (guard + wrapper de ejemplo, ver abajo).
- `Makefile`: `test-bench-save`.
- `benchmarks/pr-a-*.txt`.

### Tests (deterministas, reloj inyectado, sin sleep)
1. `TestStoreFutureRejectedDoesNotAdvanceHW`: cache llena 30 min, `Store(now+3h)` = -2; `Get(now,k)` sigue = count; `Store(now+1s)` = count+1. (Reproduce el incidente.)
2. `TestStoreWithinSkewAccepted`: `now+skew` aceptado y avanza HW; `now+skew+1s` = -2.
3. `TestGetFutureReturnsFutureEvent`.
4. `TestFutureCheckOnlyWhenAdvancing`: Clock contador de llamadas; N stores con ts <= HW -> 0 llamadas; 1 store con ts > HW -> 1 llamada.
5. `TestMaxFutureSkewZeroKeepsV120Behaviour`: sin skew, `Store(now+3h)` avanza HW (regresión del comportamiento antiguo, documentado).
6. `TestConfigValidateFutureSkew`: negativo, sub-segundo, Clock sin skew.
7. `TestFutureRejectDoesNotPrune`: memoria/keys intactos tras rechazo (usa `shard.size()`).
8. Concurrencia: `TestConcurrentFutureAndNormalStores` con `-race`: goroutines de futuros + normales; HW nunca supera `Clock()+skew`.
9. Fuzz existente: añadir skew al fuzz si hay `Fuzz*` de Store (comprobar invariante HW <= clock+skew).

### Benches nuevos
- `BenchmarkStoreFutureReject` (esperado ~2-5 ns, sin Clock cacheado).
- `BenchmarkStoreAdvancingHWWithSkew`: cada op avanza HW (peor caso: paga Clock por op) vs sin skew.
- `BenchmarkStoreSteadyStateWithSkew`: réplica de `StoreSteadyState` con skew -> debe ser == baseline.

### Caller (fuera de este repo, documentado en README)
```go
const maxSkew = 5 * time.Minute
if ts > time.Now().Add(maxSkew).UnixNano() { metrics.Future.Inc(); return }
n := cache.Store(ts, key)
switch { case n == slidingcache.FutureEvent: ...; case n == slidingcache.LateEvent: ...; default: ... }
```
Config recomendada: `MaxFutureSkew: 5*time.Minute`, `Shards: 128`, `SweepInterval: 2*time.Minute`.

### Edge cases
- Clock que retrocede (NTP): solo hace el guard más estricto temporalmente; HW no retrocede nunca. Documentar.
- Clock muy adelantado respecto a productores: todo se acepta como antes (guard inerte), no rompe nada.
- Epoch negativos / base arbitraria: si el caller no usa Unix debe inyectar Clock. Documentar.
- `ts > HW` pero `ts <= cutoff`: imposible (cutoff < HW). No hay orden ambiguo.

---

## PR-B: `feat: expose HighWater` (Reset descartado: PR-A ya evita el envenenamiento)

### API
```go
// HighWater devuelve el HW en segundos de bucket y false si Store nunca aceptó un evento.
func (c *Cache) HighWater() (int64, bool)
```
- Un `Load` atómico. Sin cambios en Store/Get -> bench post debe ser == baseline (se guarda igualmente como evidencia).

### Tests
- HighWater tras New = (_, false); tras Store = bucket esperado; no cambia con Get ni con Store -1/-2; sube con Store dentro del skew.

### Archivos
`slidingcache.go`, tests, README ("Diagnostics"), `benchmarks/pr-b-*.txt`.

---

## PR-C: `feat: per-cache Stats (accepted / late / future / out-of-range)`

### API
```go
type Stats struct {
    Accepted   uint64 // Store que almacenó (count >= 1)
    Late       uint64 // Store == -1 (t <= HW-WindowSize)
    Future     uint64 // Store == -2 (requiere PR-A; si C va antes que A, campo existe y vale 0)
    OutOfRange uint64 // Store == -1 por bucket no representable
    GetHit     uint64 // Get con key presente (count >= 0 devuelto desde la shard)
    GetMiss    uint64 // Get con key ausente (0)
    GetLate    uint64 // Get == -1
    GetFuture  uint64 // Get == -2
    Keys       int    // suma de len(keys) por shard (toma locks; barato: 16..256 shards)
}
func (c *Cache) Stats() Stats
```
- Implementación sin contención: contadores **por shard**, `Accepted`/`Late`(bajo lock) como `int` plano incrementado dentro de `shard.store` (lock ya tomado, coste ≈ 1 add). Rechazos pre-lock (`inRange`, futuro, tardío rápido): `atomic.Uint64` en la shard elegida por `shardFor(key)` (paga FNV ~15 ns solo en la vía rara). Struct de contadores alineada a 64 B para evitar false sharing con `mu`.
- `Stats()` suma shards con `Load`/lock. No es snapshot atómico global (documentar).
- Sin dependencia de PR-A: si A no está mergeada, `Future`/`GetFuture` se añaden con TODO y `0`; al mergear A, se enlazan (conflicto trivial en Store).
- Opcional (no en este PR): `ResetStats()`; los consumidores usan deltas.

### Tests
- Cada camino incrementa exactamente su contador (mesa de casos: aceptado, tardío pre-lock, tardío bajo lock vía `SweepInterval`/HW concurrente si es reproducible, fuera de rango, Get tardío, Get miss no cuenta como Late).
- Sumas: `Accepted+Late+OutOfRange(+Future) == nº llamadas a Store` bajo carga concurrente con `-race`.
- `Keys` coherente con `shard.size()` y decrece tras sweep.

### Benches
- `BenchmarkStats` (256 shards).
- Todos los existentes: delta <= 3% (gate). Si `ParallelStore` empeora >3%: mover `Accepted` a contador bajo lock ya está; revisar padding.

---

## Orden y merge
1. PR-A (bloquea el incidente). 2. PR-B. 3. PR-C. Cada uno: rama desde `main`, `make lint test test-race`, bench pre/post en `benchmarks/`, PR con Yago.
Tras merge de los 3: tag v1.3.0, actualizar caller (guard + switch -1/-2 + exportar Stats a métricas cada 10 s).

## Decisiones cerradas (2026-09-12)
- Nombres `MaxFutureSkew` / `Clock`. `Clock func() int64` devuelve epoch en `EpochUnit` (sin `time.Time`: evita conversión por llamada).
- Stats incluye `GetHit`/`GetMiss` (1 add bajo lock ya tomado).
- `Reset()` descartado. PR-B = solo `HighWater()`.
- Gate 3%.
- Repetir baseline en host Linux de prod antes de comparar cifras absolutas:
  `make test-bench-save PR=baseline-prod` (tras PR-A) o los 2 `go test -bench` del bloque "Flujo de benchmarks".
