# Plan: SlidingCache probabilística sobre Count-Min Sketch (paquete nuevo)

Base: `main` == 26e668c (tags v1.4.0 y v1.5.0, mismo commit). Tests verdes en go1.27.3; `go.mod` se queda en `go 1.26`.
Paquete raíz **intacto** en Fases 1-3 (su gate 3% se cumple por construcción). Sin commits sin confirmación.
Código vía Pedro, PRs vía Yago. Cifras medidas en Apple M2, sin locks salvo que se diga.

## 1. Decisiones cerradas (2026-11-23)

| Tema | Decisión |
|---|---|
| Paquete cache | `github.com/mresti/slidingcache/countminsketch` (público, implementa `SlidingCache`) |
| Paquete sketch | `github.com/mresti/slidingcache/internal/countmin` |
| Dependencia `nalgeon/unsure/countmin` | descartada; sketch propio (§2) |
| ε con shards | global, columnas repartidas entre shards (§3). RAM no depende de `Shards` |
| Contadores | uint32 por slot + uint64 en el agregado, con saturación |
| Actualización | clásica (+1 en todas las filas) en el MVP; conservative update después (Fase 5) |
| Rango de epochs | todo opcional por config: cambiar el rango y desactivar el guard (forma abajo) |
| Adopción | consumidores con `== 0`/`== 1` siguen en el exacto; gate evaluado con los datos de la librería (§4) |
| Stats | sin `Keys`; `GetHit` = estimación > 0, `GetMiss` = estimación == 0 |
| `go.mod` | no se sube a 1.27 |
| Raíz | solo doc de la interface y Makefile, en Fase 4 |
| Constructor sketch | `New(ε, δ) (*Sketch, error)` |
| Hot path | `AddHash(h)`: 1 hash por op; `Add([]byte)` sigue exportado |

Forma del rango (Fase 2):
- `Config.TimestampBits int`: 0 -> 43 (±2^43 s, igual que raíz por defecto); válido 39..62. De 39 a 55 son
  los rangos de `CountBits` 24..8 y se comparan contra raíz; de 56 a 62 contra un modelo ingenuo en los tests.
- `Config.DisableRangeGuard bool`: equivale a 62 bits (±2^62 s). Es el techo aritmético: por encima,
  `bucket()` y `HW − window` desbordan int64, así que esos epochs se siguen rechazando con −1. En ms y ns
  cabe todo int64. Combinado con `TimestampBits != 0` -> error de `New`.

Propuestas mías sin objeción:
- Sin `SweepInterval` ni janitor: la RAM está acotada por construcción. `Close` queda como no-op por paridad.
- `Stats.Saturated`: contador nuevo de incrementos perdidos por celda saturada.

## 2. Evaluación: `github.com/nalgeon/unsure/countmin` vs sketch propio

Leído el código (v0.2.0, 169 líneas + `internal/wyhash`). Es el origen del snippet.

**Qué es:** MIT, sin dependencias, `go 1.25` (no obliga a subir `go.mod`). uint32, *conservative update*
(~2× menos error que el count-min clásico), wyhash con seed fija 0, `New` hace panic con parámetros inválidos.
API completa: `New, Add, AddN, Count, Total, Size`. Primer commit 2026-11-01, v0.2.0 del 2026-11-21.

**Medido (ventana = anillo de sketches de la dependencia, conteo vivo = suma de `Count`):**

| `Store` | 60 buckets | 1800 buckets |
|---|---|---|
| dependencia, sketch (0.001, 0.01) de 55 KB | 561 ns | 109 µs |
| dependencia, mejor caso (sketch pequeño en caché, ε·128) | 542 ns | 16 µs |
| prototipo con acceso a celdas (slot + agregado), **sin lock ni chequeos de ventana** | 13 ns | 14 ns |
| exacto actual (`StoreManyKeys` archivado, con lock y todo; ventana 3600 buckets) | 34 ns | 34 ns |

Las tres primeras filas son el núcleo sin mutex, sin chequeos de HW y sin Stats: el prototipo es un suelo, no
el coste final de `Store`. El veredicto no depende de eso: 16-109 µs frente a decenas de ns.

Caducar un bucket: dependencia = `New` = 3,1 µs y 57 KB (2 allocs); prototipo = restar y limpiar, 7,4 µs, 0 allocs.

| | Dependencia | Propio (`internal/countmin`) |
|---|---|---|
| Coste `Store`/`Get` | O(buckets·d): 16-109 µs con ventana prod (470-3.200× el exacto) | O(d): núcleo ~14 ns, no depende de la ventana |
| Expiración | sin `Reset` ni resta: 57 KB nuevos por bucket y shard | resta al agregado y reutiliza, 0 allocs |
| Decisiones §1 | choca con 4: sin `error` en `New`, sin `AddHash`, sin agregado uint64, sin tamaño por (ancho, filas) | las cumple |
| Hash | seed pública fija: colisiones fabricables (lo avisa su README) | `maphash` con seed por Cache |
| Precisión a igual RAM | mejor (~2×, conservative update) | clásico en MVP; CU opcional después (Q3) |
| Código a mantener | ~0 | ~200 líneas + tests |
| Madurez | bien testeada, pero 3 semanas de vida y v0.x | la que le demos; riesgo en el hashing |
| Dependencias del módulo | rompe "zero external dependencies" | ninguna |

**Veredicto: propio.** La dependencia es un buen sketch pero no expone lo que una ventana deslizante necesita
(restar, reiniciar, celdas), y eso no se arregla envolviéndola. Forkearla (MIT lo permite) equivale a propio
con otro origen: nuestro hot path no usa su hash ni su `Add`.
**Se aprovecha de ella, citándola:** un hash de 64 bit + salt impar por fila con multiply-shift (su
`history.md` documenta que el doble hashing `lo + i·hi mod w` deja keys en la misma columna en todas las
filas: era mi propuesta inicial, descartada), y las ideas de sus tests de cota de error, independencia de
filas y saturación, reescritas; si se copia código, va con su aviso MIT.

## 3. ε con shards: comparación y elección

ε=1e-4, δ=0.01, incrementos clásicos. Error = estimación − real por key.

| Opción | RAM/bucket | uniforme (100K keys): media / p99 | Zipf 1.1 (562K keys): media / p99 / máx |
|---|---|---|---|
| un sketch global, sin shards | 544 KB | 163 / 400 | 44 / 97 / 560 |
| **repartido**, S=16 | 544 KB | 163 / 400 | 44 / 97 / 417 |
| **repartido**, S=128 | 545 KB | 163 / 400 | 44 / 97 / 662 |
| **repartido**, S=1024 | 553 KB | 162 / 400 | 44 / 101 / 487 |
| literal por shard, S=16 | 8,7 MB | 0 / 0 | 0,3 / 3 / 66 |
| literal por shard, S=128 | 69,6 MB | 0 / 0 | 0 / 0 / 2 |

**Elegido: repartido.** Misma precisión que el sketch global a la misma RAM, con cualquier nº de shards, así
que `Shards` sigue siendo solo concurrencia, como hoy. "Literal" no es más preciso: es la misma estructura
con ε/S y S× RAM (69,6 MB × 1800 buckets = 125 GB con 128 shards). Quien quiera esa precisión baja ε.
- `ancho_total = ceil(e/ε)`, `ancho_shard = ceil(ancho_total/Shards)`, `filas = ceil(ln 1/δ)` (máx. 16).
- `New` devuelve error si `ancho_shard` queda por debajo de un mínimo (valor a fijar en Fase 2 con simulación).
- RAM ≈ `ceil(W/P) · ancho_total · filas · 4 B`, más la sobrecarga por slot (cabecera, redondeo del
  allocator): con 128 shards y ε por defecto un slot son 440 B de celdas y ~15% extra. Si el gate de RAM de
  Fase 3 no pasa, un array plano por shard la elimina.

## 4. Gate de adopción — cerrado con los datos de la librería

**Resultado: el sketch no sustituye al exacto en el workload actual; se entrega como alternativa opt-in.**
Datos usados: workload de `PLAN-v1.3` y configuración de `BenchmarkMemoryFootprint`. La librería no recoge el
umbral que compara el consumidor; sin él solo se puede afirmar lo de abajo.

Workload de `PLAN-v1.3` (100K keys × 10K ev/key en 30 min, 1800 buckets, ~1,8 GB exacto), simulado:

| ε, δ | RAM | sobreconteo mediano | keys exactas |
|---|---|---|---|
| 1e-3, 0.01 | 98 MB | +300.000 (30× el valor real) | 0% |
| 1e-4, 0.01 | 979 MB | +20.000 (2×) | 12% |
| 3e-5, 0.01 | 3,3 GB | 0 | 87% |
| 1e-5, 0.01 | 9,8 GB | 0 | 99,7% |
| exacto actual | ~1,8 GB | 0 | 100% |

- Con ese workload no hay configuración con menos RAM y precisión utilizable. Gana con cardinalidad muy alta
  de keys poco densas **y** consumidores que comparan contra un umbral `T >> N/ancho`.
- Consumidores con `== 0` o `== 1` no pueden usar el sketch (una key ausente puede dar > 0 y una nueva > 1):
  siguen en el exacto.
- `BenchmarkMemoryFootprint` (100K keys × 10 buckets, 19,5 MB exacto): sketch (0.001, 0.01) con slots
  perezosos ≈ 0,6 MB, pero sobrecuenta ~+300 sobre un valor real de 10. Misma conclusión.
- RAM exacto ≈ `K · (115 B + 8 B · buckets vivos/key)` (aprox., de un punto de `MemoryFootprint`).
- Los benches y tests del paquete nuevo usan los mismos datos que los de la librería (`benchConfig`,
  `makeKeys`, 100K keys × 10 ventanas, workload de `PLAN-v1.3`) para que las cifras sean comparables.

## 5. Diseño

### `internal/countmin`
```go
func Dimensions(epsilon, delta float64) (width, depth int, err error) // ancho = ceil(e/ε), filas = ceil(ln(1/δ)) <= 16
func New(epsilon, delta float64) (*Sketch, error)
func NewSized(width, depth int) (*Sketch, error)
func (s *Sketch) Add(item []byte); AddN(item []byte, n uint64)   // hash propio del sketch (maphash)
func (s *Sketch) Count(item []byte) uint64; Total() uint64
func (s *Sketch) AddHash(h uint64); CountHash(h uint64) uint64   // hash ya calculado
func (s *Sketch) Reset()

func NewAggregate(width, depth int) (*Aggregate, error)          // celdas uint64, mismos índices
func (a *Aggregate) NewSlot() *Sketch                            // slot con sus dimensiones
func (a *Aggregate) Add(slot *Sketch, h uint64) (estimate uint64, saturated bool) // slot+agregado, una pasada
func (a *Aggregate) Count(h uint64) uint64
func (a *Aggregate) Sub(slot *Sketch) error                      // caducar un slot
func (a *Aggregate) AddSketch(slot *Sketch) error                // reconstruir desde supervivientes
func (a *Aggregate) Reset()

func ShardOf(h uint64, bits uint) uint64                         // shard de h entre 1<<bits, con salt propio
```
- `Sketch`: celdas uint32. `Aggregate`: suma celda a celda de los slots vivos de una ventana.
- Índice de la fila i: `((h·salt_i) >> 32) · ancho >> 32`. La shard se elige igual, con su propio salt.
- Saturación: una celda de slot en 2^32−1 no sube más, ni la del agregado, para que I1 se mantenga.

### `countminsketch`
`Config`: Precision, WindowSize, EpochUnit (`slidingcache.EpochUnit`), Shards, MaxFutureSkew, Clock, mismas
reglas que raíz; más `Epsilon` (0 -> 0.001), `Delta` (0 -> 0.01), `TimestampBits`, `DisableRangeGuard`. Sin
CountBits ni SweepInterval.
`New`, `Store`, `Get`, `Close` (no-op, por paridad), `HighWater`, `Stats`, `WithHashFunc`.
Reutiliza de raíz: interface, `LateEvent`, `FutureEvent`, `EpochUnit`.

Por shard: `ring []slot` (len B = ceil(W/P); slot = {timestamp, *Sketch}, alloc perezoso y reutilizado) +
agregado + `applied` (cutoff ya aplicado).
Invariantes: **I1** agregado == Σ slots vivos, celda a celda; **I2** slot vivo ⇔ `applied < ts <= HW`; **I3** `applied` monótono.

```
ts := bucket(epoch)                                  // idéntico a raíz
!inRange(ts)                    -> OutOfRange++, -1
ts > HW: futuro?                -> Future++, -2 (sin CAS) ; si no, advanceHighWater
ts <= HW-window                 -> Late++, -1 (rechazo rápido pre-lock)
h := hash(key)                                       // 1 hash por op, sobre el string
lock shard; cutoff := HW.Load()-window               // re-derivado bajo lock, como raíz
ts <= cutoff                    -> Late++, -1
expireTo(cutoff)                                     // agregado -= slots caducados; Reset
slot := ring[floorMod(ts/precision, B)]; alloc/retag si hace falta
slot + agregado += 1 en las celdas de h; Accepted++
return min de las celdas del agregado                // O(filas)
```
- `Get`: mismos chequeos, `expireTo`, lectura del agregado. No escribe celdas.
- `expireTo`: k slots caducados -> O(k·celdas); si k > B/2 reconstruye desde los supervivientes; salto >=
  ventana -> todo a cero. La expiración es exacta respecto al HW visto bajo lock, también si lo avanzó otra shard.
- Hash por defecto `maphash.String(seed, key)` con seed por Cache: 4-5 ns, 0 allocs (`fnv1a` del repo: 9-39 ns).
- Mutex por shard como hoy. Lock-free con atómicos = optimización posterior.

## 6. Comportamiento: qué se preserva y qué cambia

**Idéntico (verificado contra la implementación actual como oráculo):** mapeo epoch->bucket (floor, negativos,
EpochUnit, Precision), validación de config, orden de chequeos, HW (solo avanza, Get no lo mueve, cache
virgen acepta el primer evento y Get da 0), centinelas −1/−2 en los mismos casos, guard de futuro y nº de
lecturas de Clock, regla de vida estricta, fuera de orden, rechazo tardío sin efectos, partición de Stats
(`Accepted+Late+Future+OutOfRange == Stores`), 0 allocs en estado estable, `Close` idempotente.

**Cambia, inherente al sketch:**
| Hoy | Sketch |
|---|---|
| conteo exacto | `>=` real; `<=` real + ε·N con prob. 1−δ; nunca por debajo |
| key nueva -> `Store` = 1 | `>= 1` |
| key ausente -> `Get` = 0 | `>= 0` (falso positivo) |
| keys aisladas | eventos de otras keys pueden subir tu conteo |
| RAM ∝ keys | RAM fija; **más** que el exacto con pocas keys |
| `Stats.Keys`, `GetHit/GetMiss` por presencia | sin `Keys`; hit/miss por estimación |
| `HashFunc` solo reparte shards | su calidad afecta a la corrección (colisión 64 bit = keys fusionadas) |
| count sin tope (spill) | tope 2^32−1 por celda y bucket |
| `Get` no muta | rota slots internamente (no observable) |
| prune O(1) amortizado | pico de rotación en cambio de bucket / salto de HW |

Se mantiene en single-thread: `Store(k)` == `Get` previo + 1, si ese `Store` no avanza el HW ni satura una celda.

## 7. Fases

**Fase 1 — `internal/countmin`. HECHA (2026-11-23).** Sketch + agregado + tests + benches. Nada
más. Gate: tests §8, 0 allocs, y a 2719×5 con hash precalculado no más de 1,5× el prototipo (M2):
`Aggregate.Add` <= 12 ns (prototipo 8,1), `Aggregate.Count` <= 8,5 ns (5,7), caducar un slot (`Sub` +
`Reset`) <= 11 µs (7,3).
- Resultado: `Aggregate.Add` 8,7 ns, `Aggregate.Count` 4,4 ns, caducar 5,7 µs (47 ns a 22×5), 0 allocs.
- `make ci` verde en go1.26.4 y go1.27.3 (lint 0 issues, `-race`, cobertura 98,2%); vet linux/386 ok.
- Fuzz real 30 s por target (9,4M y 0,8M ejecuciones), sin fallos. 18 mutantes elegidos a mano (no es un
  mutation score): 17 cazados; el que sobrevive quita `max(…, 1)` de las filas, que es código inalcanzable.
- Notas para fases siguientes: cada slot tiene su propia seed, así que la cache cuenta solo con
  `Aggregate.Add` (nunca `slot.Add([]byte)`); conservative update tocará los dos bucles de filas, no solo los
  helpers; `TestErrorBound` comprueba la cota con mucho margen, la Fase 5 necesita una medida de error más fina.
**Fase 2 — `countminsketch` correcto, 1 shard lógica.** Helpers de ventana duplicados (~80 líneas: bucket,
floorDiv, cutoffFor, advanceHighWater, isFuture, validadores, unixClock). Anillo + agregado + `expireTo`.
Gate: fuzz diferencial, 0 divergencias.
**Fase 3 — shards, concurrencia, rendimiento, memoria.** Benches espejo de los actuales (`StoreManyKeys`,
`StoreHotKeyNanos`, `GetHitManyKeys`, `ParallelStore`, `StoreSteadyState`, `StoreAdvancingHighWater`,
`MemoryFootprint`) archivados en `benchmarks/`. Gate de corrección: fuzz diferencial con Shards > 1, test de
HW avanzado por otra shard y `-race`, 0 divergencias (la Fase 2 no ejercita la shard atrasada). Gates de
rendimiento: 0 allocs tras calentar; coste independiente de B (B=60 vs B=1800 dentro de ±10%); RAM == fórmula
(con la sobrecarga por slot de §3) ±5% e independiente del nº de keys. Objetivo provisional `Store` <= 3× el
exacto; se fija con datos.
**Fase 4 — docs y deuda.** README (sección sketch, sizing, tabla §6), contrato de la interface, Makefile
(`PKG`), y opcional extraer `internal/window` (toca raíz -> gate 3%).
**Fase 5 — conservative update (después del MVP).** Solo sube las celdas por debajo del nuevo mínimo del
slot y refleja el mismo delta en el agregado (I1 se mantiene). Gate: nunca subestima, error medio <= 0,6× el
clásico en uniforme y Zipf, modo A sigue exacto. Coste asumido: `Store` deja de subir siempre +1.

Verificación local en todas las fases: `go vet`, tests con `-race` y lint (golangci-lint v2.15.0), todo en
go1.26.4 y go1.27.3.

## 8. Tests

`internal/countmin`:
- Dimensiones: (0.001, 0.01) -> 2719×5; (0.0001, 0.01) -> 27183×5 = 543.660 B. ε/δ fuera de (0,1), NaN, Inf,
  δ que pida más de 16 filas, tamaño desbordado -> error.
- Nunca subestima: fuzz Add/AddN vs `map` exacto, `Count >= real`, `Total` exacto.
- Cota (ε, δ) con semilla fija, uniforme y Zipf: fracción de keys con error > ε·N <= δ, presentes y ausentes.
- Independencia entre filas y entre shard y filas: pares que colisionan en ambas ≈ esperado.
- `Reset`; resta `(A+B)−B == A` celda a celda; dimensiones distintas -> rechazo. Saturación sin romper I1.
- `AllocsPerRun == 0` en Add, AddHash, Count, CountHash.

`countminsketch` (oráculo = `slidingcache.Cache` real, sin mocks):
- **Modo A, hash inyectado sin colisiones:** `got == want` en cada Store/Get -> la lógica de ventana es la misma.
- **Modo B, hash por defecto y sketch estrecho:** centinelas iguales, `got >= want`.
- Fuzz diferencial con el esquema de `FuzzStoreGetInvariants` y `FuzzFutureSkewInvariants` (4 keys, offsets
  que cruzan la ventana, extremos del rango en cada `TimestampBits`, reloj falso). I1 tras cada op (white-box).
- Rango: 39..55 contra raíz; 56..62 y guard desactivado contra modelo ingenuo; epochs en los extremos de
  int64 en las tres unidades (el desbordamiento de `bucket()` debe acabar en −1, nunca en un timestamp falso).
- Rotación: frontera estricta, salto < / == / >> ventana, fuera de orden a bucket antiguo vivo, tardío no
  toca nada, `Window` no múltiplo de `Precision`, timestamps negativos, reutilización de slot.
- HW avanzado por otra shard -> Get/Store de la key reflejan la expiración.
- Cache virgen: el primer `expireTo` parte de `applied` en el centinela (~MinInt64); `cutoff − applied` no debe
  desbordar ni recorrer bucket a bucket. Test con timeout, equivalente a `TestUnobservedHighWaterDoesNotUnderflow`.
- Stats: contadores de rechazo iguales al oráculo; partición bajo concurrencia con `-race`.
- Config: ε/δ, `TimestampBits`, `ancho_shard` mínimo, más los casos actuales. `AllocsPerRun == 0` tras calentar.

## 9. Edge cases y modos de fallo

- Workload uniforme: todas las keys sobrecontadas ~N/ancho (§4). Tráfico total sube -> error sube.
- Consumidores con `== 0` / `== 1` / `== umbral` se rompen; con `>= umbral`, no.
- Muchas shards con ε grande: `ancho_shard` diminuto -> error de `New`.
- Key en la shard de un heavy hitter (1/Shards de las keys): cota formal más débil que la media; en la
  simulación de §3 no se nota en p99, sí puede en el máximo.
- Salto de HW: pico de latencia bajo el lock de la shard, O(min(k, B−k)·celdas).
- Primer `expireTo` de una cache virgen: la distancia al centinela desborda si se resta sin saturar.
- Saturación de celda (Precision grande + key muy caliente): el conteo se queda corto; contarlo en Stats.
- Cambiar ε/δ exige cache nueva. Primer evento de cada slot aloca (calentamiento = 1 ventana).
- `HashFunc` débil del usuario fusiona keys. Plataformas de 32 bit: `int` de retorno, igual que hoy.

## 10. Deuda técnica detectada

1. `go.mod` en `go 1.26` mientras los benchmarks archivados dicen go1.27.0 (se queda así por decisión).
2. Tags v1.4.0 y v1.5.0 en el mismo commit; sin baseline v1.4/v1.5; `BASELINE` del Makefile sigue en v1.2.0.
3. Makefile: `test-bench*` y `test-fuzz*` solo corren el paquete raíz (`.`). Se arregla en Fase 4.
4. Doc de `SlidingCache` promete exactitud ("stores 1 and returns 1", "Returns 0 if the key does not exist").
   Se relaja en Fase 4.
5. Lógica de ventana no exportada en raíz -> duplicada hasta extraer `internal/window`.
6. Guard de rango (±2^(63−CountBits)) es regla de dominio colgada de un detalle de almacenamiento.
7. README obsoleto: "Future/GetFuture stay at 0 until MaxFutureSkew... land" (ya está); el export de ejemplo
   omite `GetFuture`; `HighWater` dice "ever accepted" y el código "ever advanced".
8. `slidingcache.go:350`: el comentario de `layout` quedó encima de `maxFutureSkew`.
9. `SlidingCache` no incluye `Close/HighWater/Stats`: tras la interface se pierde ciclo de vida y diagnóstico.
10. FNV-1a por defecto: predecible y ~7× más lento que `maphash` en keys de 58 B. En raíz solo sesga shards.
11. **Resuelta (2026-11-23).** `make lint` fallaba en local con go1.27.3: golangci-lint 2.12.2 no
    leía el export data v5 de la stdlib 1.27. Subido a v2.15.0 en `tools/go.mod` (`go 1.26.0` mínimo, CI no
    cambia): 0 issues en raíz con go1.27.3 y con go1.26.4. En `.golangci.yml`, `gofumpt.extra-rules` (obsoleto
    en 2.15) pasa a `extra` con las tres reglas que ya activaba.

## 11. Preguntas abiertas

Ninguna bloqueante. Pendiente de dato externo: el umbral real del consumidor, que la librería no recoge (§4).
