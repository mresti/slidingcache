# Benchmarks

Resultados guardados por PR para comparar con benchstat. Extensión `.txt` (`*.out` está en .gitignore).

| Archivo | Qué es |
|---|---|
| `baseline-v1.2.0-serial.txt` | `^Benchmark(Store|Get|Sweep|Memory)` -count=10 -cpu=1 en main (v1.2.0) |
| `baseline-v1.2.0-parallel.txt` | `^BenchmarkParallel` -count=10 -cpu=4 en main (v1.2.0) |
| `*.benchstat.txt` | resumen benchstat de un fichero |
| `<pr>-serial.txt`, `<pr>-parallel.txt` | post del PR |
| `<pr>-vs-baseline-{serial,parallel}.txt` | `benchstat baseline <pr>` |
| `pr-a-future-skew-serial.txt` | post de PR-A (guard de futuro), serial |
| `pr-a-future-skew-parallel.txt` | post de PR-A, parallel |
| `pr-a-future-skew-vs-baseline-{serial,parallel}.txt` | PR-A vs baseline v1.2.0 |

Generar post + comparación:
```
make test-bench-save PR=pr-a-future-skew
```
El target escribe la cabecera (go version, CPU, cores, commit, fecha) y luego los
dos `go test -bench`; después lanza benchstat contra el baseline.
Gate: delta <= 3% en StoreManyKeys, StoreHotKeyNanos, GetHitManyKeys, ParallelStore; 0 allocs en Store/Get.
Benches nuevos en PR-A (solo aparecen en el fichero post, sin fila en el
baseline): `StoreFutureReject`, `StoreAdvancingHighWater/skew={off,on}`,
`StoreSteadyStateWithSkew/keys=*`.

Máquina baseline: Apple M2 (8 cores), go1.27.0, darwin/arm64. Repetir baseline en el host de prod antes de comparar cifras absolutas.
