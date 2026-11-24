# Plan: SlidingCache probabilística sobre Count-Min Sketch (paquete nuevo)

Base: `main` == 26e668c (tags v1.4.0 y v1.5.0, mismo commit). Tests verdes en go1.27.3; `go.mod` se queda en `go 1.26`.
Paquete raíz **intacto** en Fases 1-3 (su gate 3% se cumple por construcción); en la Fase 4, solo comentarios. Sin commits sin confirmación.
Código vía Pedro, PRs vía Yago. Cifras medidas en Apple M2, sin locks salvo que se diga.

## 1. Decisiones cerradas (2026-11-23)

| Tema | Decisión |
|---|---|
| Paquete cache | `github.com/mresti/slidingcache/countminsketch` (público, implementa `SlidingCache`) |
| Paquete sketch | `github.com/mresti/slidingcache/internal/countmin` |
| Dependencia `nalgeon/unsure/countmin` | descartada; sketch propio (§2) |
| ε con shards | global, columnas repartidas entre shards (§3). La RAM casi no depende de `Shards`: +7% con 256 frente a 16, ε por defecto |
| Contadores | uint32 por slot + uint64 en el agregado, con saturación |
| Almacenamiento | un array plano por shard, reservado entero en `New` (§3, medido el 2026-11-24) |
| Actualización | clásica (+1 en todas las filas) en el MVP; conservative update por slot desde la Fase 5, siempre activo (decisión del 2026-11-24; no se hace opcional) |
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

Aceptado el 2026-11-24:
- Tope de 2^20 buckets por ventana (`New` da error por encima).
- Fase 3: estimación de RAM y tope configurable, para que un ε diminuto falle en `New` y no al reservar.
- Ancho mínimo por shard: 8 columnas. Tope de RAM: `Config.MaxMemoryBytes`, 1 GiB por defecto. La
  estimación no se exporta: solo sale en el mensaje de error.
- Objetivo de rendimiento re-alcanzado: `Store` <= 3× el exacto desde ~110 eventos por bucket en toda la
  cache; más lento por debajo (§7, Fase 3).
- `BenchmarkStoreDensity` se queda; `internal/window` no se extrae por ahora; Fase 5 aprobada.
- Fase 5: conservative update siempre activo; gate de precisión re-alcanzado por densidad; `Ring.Add` a 12,5 ns.

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
- `New` devuelve error si `ancho_shard` < 8 (constante `minShardWidth`). Es un guard contra el precipicio, no
  una garantía por shard: simulado con Zipf 1.1/1.5 y 100 keys calientes, con ancho <= 4 decenas de shards
  tienen del 10% al 50% de sus keys por encima de ε·N; de 6 a 12 la degradación es gradual. Las colas igualan
  a las del sketch global desde ~40 columnas por shard (p99.9 a ε=1e-3, Zipf 1.1: 2038 global, 2151 con 43,
  2302 con 22, 2735 con 11, 6827 con 3). ε por defecto con 256 shards da ancho 11: se acepta, con cola más gorda
  para las keys que comparten shard con una key muy caliente.
- RAM = `Shards · (B · ancho_shard · filas · 4 B + B · 8 B + ancho_shard · filas · 8 B)`, con `B = ceil(W/P)`.

Estructura, medida en estado estable (3600 buckets, 5 filas, ancho total 2719, M2):

| Shards | celdas | un sketch por slot: heap / ciclo de GC | array plano por shard (16 B de metadatos por slot): heap / GC |
|---|---|---|---|
| 16 | 196 MB | +4,3% / 1,2 ms | +0,6% / 0,10 ms |
| 128 | 203 MB | +22% / 6,5 ms | +4,5% / 0,11 ms |
| 256 | 203 MB | +42% / 9,2 ms | +8,6% / 0,15 ms |
| 1024 | 221 MB | +155% / 25 ms | +33% / 0,32 ms |

**Elegido: array plano**, con 8 B de metadatos por slot (la mitad de lo medido). Con un sketch por slot la RAM
dependía de `Shards`, contra la decisión de §1. Consecuencia: `New` reserva toda la ventana de una vez y
después no se aloca nada.

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
- `BenchmarkMemoryFootprint` (100K keys × 10 buckets, 19,5 MB exacto): el sketch (0.001, 0.01) reserva la
  ventana entera de `benchConfig` (3600 buckets, 196 MB) aunque solo se usen 10, y sobrecuenta ~+300 sobre
  un valor real de 10. Misma conclusión.
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

func NewRing(width, depth, slots int) (*Ring, error)            // slots uint32 en un array + agregado uint64
func (r *Ring) Slots() int
func (r *Ring) Add(slot int, h uint64) (estimate uint64, saturated bool) // slot+agregado, una pasada
func (r *Ring) Count(h uint64) uint64                           // estimación del agregado
func (r *Ring) Expire(slot int)                                 // agregado -= slot; slot a cero
func (r *Ring) Reset()
func (r *Ring) AppendSlotCells(dst []uint32, slot int) []uint32 // copias, para comprobar I1 desde fuera
func (r *Ring) AppendAggregateCells(dst []uint64) []uint64

func ShardOf(h uint64, bits uint) uint64                         // shard de h entre 1<<bits, con salt propio
```
- `Sketch`: celdas uint32, independiente. `Ring`: los slots de una ventana en un array plano más su
  agregado (suma celda a celda). Sustituye a `Aggregate` de la Fase 1 (medición de §3).
- Índice de la fila i: `((h·salt_i) >> 32) · ancho >> 32`. La shard se elige igual, con su propio salt.
- Saturación: una celda de slot en 2^32−1 no sube más, ni la del agregado, para que I1 se mantenga.

### `countminsketch`
`Config`: Precision, WindowSize, EpochUnit (`slidingcache.EpochUnit`), Shards, MaxFutureSkew, Clock, mismas
reglas que raíz; más `Epsilon` (0 -> 0.001), `Delta` (0 -> 0.01), `TimestampBits`, `DisableRangeGuard`. Sin
CountBits ni SweepInterval.
`New`, `Store`, `Get`, `Close` (no-op, por paridad), `HighWater`, `Stats`, `WithHashFunc`.
Reutiliza de raíz: interface, `LateEvent`, `FutureEvent`, `EpochUnit`, `HashFunc`.
- `Shards` (Fase 3), mismas reglas que raíz (0..2^20, redondeo a potencia de dos). Por defecto: la mayor
  potencia de dos <= 16 que deja `ancho_shard` >= 8, para que un valor por defecto nunca haga fallar `New`.
- `MaxMemoryBytes` (Fase 3): tope de la RAM que `New` puede reservar; 0 -> 1 GiB. Se comprueba antes de
  reservar nada; por encima, error con la estimación.
- El anillo se reserva entero: `New` devuelve error si `ceil(W/P)` > 1.048.576 buckets (2^20).

Por shard: `countmin.Ring` de B = ceil(W/P) slots con `ancho_shard` columnas + `timestamps []int64` (8 B
por slot; `math.MinInt64` = slot muerto) + `applied` (cutoff ya aplicado). Todo reservado en `New`.
Invariantes: **I1** agregado == Σ slots vivos, celda a celda; **I2** slot vivo ⇔ `applied < ts <= HW`; **I3** `applied` monótono.

```
ts := bucket(epoch)                                  // idéntico a raíz
!inRange(ts)                    -> OutOfRange++, -1
ts > HW: futuro?                -> Future++, -2 (sin CAS) ; si no, advanceHighWater
ts <= HW-window                 -> Late++, -1 (rechazo rápido pre-lock)
h := hash(key)                                       // 1 hash por op, sobre el string
lock shard; cutoff := HW.Load()-window               // re-derivado bajo lock, como raíz
ts <= cutoff                    -> Late++, -1
expireTo(cutoff)                                     // agregado -= slots caducados; slots a cero
slot := floorMod(ts/precision, B); se marca vivo si estaba muerto
slot + agregado += 1 en las celdas de h; Accepted++
return min de las celdas del agregado                // O(filas)
```
- `Get`: mismos chequeos, `expireTo`, lectura del agregado. No escribe celdas.
- `expireTo` (Fase 3): un cutoff viejo no cambia nada; sin slots vivos solo anota el cutoff (así una cache
  virgen no recorre desde el centinela); si la distancia `cutoff − applied` cubre el anillo, lo recorre
  entero; si no, visita solo los buckets de `(applied, cutoff]`, cada uno en su índice. El recorrido es
  completo porque todo timestamp vivo está en `(applied, applied + W]`: como mucho B buckets seguidos, sin
  dos en el mismo índice. La expiración es exacta respecto al HW visto bajo lock, también si lo avanzó otra
  shard: una shard sin tráfico se queda atrás y se pone al día en su siguiente operación.
- Un slot se reinicia al caducar. Un slot vivo con otro timestamp en el índice de `ts` es un bug: los tests
  lo detectan, el código nunca lo reetiqueta.
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
| RAM ∝ keys, crece con el uso | RAM fija, reservada entera en `New`; **más** que el exacto con pocas keys; tope `MaxMemoryBytes` (1 GiB) |
| `Shards` por defecto 16 | hasta 16: menos si ε deja < 8 columnas por shard; `Shards` explícito así -> error de `New` |
| conteo exacto con cualquier `Shards` | con `Shards` > 1 la cota ε/δ vale de media, no por key: cola más gorda junto a una key muy caliente |
| `Stats.Keys`, `GetHit/GetMiss` por presencia | sin `Keys`; hit/miss por estimación |
| `HashFunc` solo reparte shards | su calidad afecta a la corrección (colisión 64 bit = keys fusionadas) |
| count sin tope (spill) | tope 2^32−1 por celda y bucket; `Saturated` cuenta los `Store` cuyo contador mínimo del bucket ya está en el tope |
| dos `Store` seguidos de una key devuelven conteos consecutivos | pueden devolver el mismo conteo (Fase 5; solo con colisiones) |
| `Get` no muta | rota slots internamente (no observable) |
| prune O(1) amortizado | cada cambio de bucket caduca un slot por shard (~340 ns); un salto de HW, uno por bucket saltado |
| `Store` ~34 ns con cualquier densidad | más rápido que el exacto desde ~450 eventos por bucket; 3× a ~110; 22× con 1 |
| cualquier `WindowSize/Precision` | máximo 2^20 buckets por ventana; por encima, error de `New` |

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
**Fase 2 — `countminsketch` correcto, 1 shard lógica. HECHA (2026-11-24).** Resultado:
- `make ci` verde en go1.26.4 y go1.27.3 (lint 0 issues, `-race`, cobertura 98,5%); vet linux/386 ok.
- 4 targets de fuzz diferencial (exacto contra raíz, colisiones acotadas, rango ancho contra modelo, guard
  de futuro), 30 s cada uno, 2,7M ejecuciones, 0 divergencias. Tras cada op se comparan retorno, `HighWater`,
  contadores e invariantes I1-I3.
- 47 mutantes elegidos a mano, incluida la conversión de unidades: 46 cazados; el superviviente es
  equivalente por argumento, no por test (solo cambia cuál de los dos contadores internos de `Late` sube).
  Tres huecos de tests detectados así y cerrados: límites exactos del rango a precisión 1 s y borde del
  guard de futuro.
- Sin cubrir: `Stats.Saturated++`, y dos ramas inalcanzables en 64 bit (error de `NewAggregate` tras
  `Dimensions`, panic por slot de otra dimensión).
- Pendiente para la Fase 3: un ε diminuto que `Dimensions` acepta reserva decenas de GB (el agregado al
  construir, los slots durante la primera ventana) sin error previo; estimación de RAM y tope configurable,
  aceptados. Los tests no se han ejecutado en 32 bit, solo vet.

Alcance original: Helpers de ventana duplicados (~80 líneas: bucket,
floorDiv, cutoffFor, advanceHighWater, isFuture, validadores, unixClock). Anillo + agregado + `expireTo`
simple, un mutex. Sin `Config.Shards` y sin benchmarks (Fase 3). Gate: fuzz diferencial con tiempo real de
fuzzing, 0 divergencias; `make ci` verde en los dos toolchains. `Stats.Saturated` queda con una rama sin
cubrir: saturar una celda exige 2^32 eventos en un mismo bucket.
**Fase 3 — shards, concurrencia, rendimiento, memoria. HECHA (2026-11-24, rama `feat/countmin-phase-3-shards`).**
Pasos, cada uno con su gate:
0. Array plano (`countmin.Ring`) con una sola shard, sin cambio de comportamiento. **Hecho:** `make ci` verde
   en go1.26.4 y go1.27.3 (cobertura 99,0% / 98,2%); 6 targets de fuzz, 25 s cada uno, 12,6M ejecuciones, 0
   fallos; `Ring` a 2719×5: Add 9,5 ns, Count 4,4 ns, caducar 5,7 µs (límites 12 / 8,5 / 11), 0 allocs.
   37 mutantes elegidos a mano sobre `Ring` y la ventana: 32 cazados, 1 no compila, 2 equivalentes en 64 bit
   (tamaño de celda en la validación; valor inicial del mínimo) y 3 huecos de tests que se cierran en el paso 1
   (borde exacto de saturación; `applied` avanza; `applied` no retrocede con un cutoff viejo).
1. `Config.Shards`: ring, mutex y contadores por shard; HW global. Gate: fuzz diferencial con Shards > 1, test
   de la shard atrasada (HW avanzado por otra shard) y `-race`, 0 divergencias.
2. Estimación de RAM y `Config.MaxMemoryBytes`, comprobados antes de reservar.
   **Hechos 1 y 2:** `make ci` verde en los dos toolchains (cobertura 98,5% / 98,2%). Los 4 targets de fuzz
   eligen 1, 2 o 4 shards: 25 s cada uno, 169K ejecuciones, 0 divergencias (más lentos que antes: tras cada op
   se comprueban las celdas crudas de todas las shards). Test determinista de la shard atrasada y test
   concurrente con `-race`. 66 mutantes a mano (shards, tope de RAM, lógica heredada): 62 cazados; 1
   equivalente (cuál de los dos contadores de `Late` sube) y 3 huecos de tests que se cierran en el paso 3
   (`Shards: 2` demasiado estrecho; valor exacto del tope por defecto; un `Store` en el propio HW no consulta
   el reloj). El `HashFunc` del usuario ahora también se llama en los rechazos, como en raíz: hace falta para
   contar el rechazo en la shard de la key.
3. Benches espejo de los actuales en `countminsketch/cache_bench_test.go`, con el exacto como brazo de
   referencia (`impl=exact` / `impl=sketch`, los dos a través de la interface), más los propios del sketch
   (`StoreDense`, `ParallelStoreDense`, `StoreWindowBuckets`, `StoreHighWaterJump`). Gates: 0 allocs; RAM ==
   estimación ±5% e independiente del nº de keys. **Archivado:**
   `benchmarks/countminsketch-phase3-{serial,parallel}.txt`, medido sobre el commit 726e1c0 (lo nombra la
   cabecera).
4. `expireTo` recorre solo los buckets de `(applied, cutoff]`; sin slots vivos solo anota el cutoff; si la
   distancia cubre el anillo, lo recorre entero. Gate: coste independiente de B (B=60 vs B=1800 dentro de ±10%).
   **Hechos 3 y 4:** `make ci` verde en los dos toolchains (cobertura 98,6% / 98,2%); 6 targets de fuzz, 0
   fallos (761K ejecuciones en los 4 del cache, 30 s cada uno; el modo exacto solo 19K, es el más lento);
   test de equivalencia del recorrido nuevo contra el recorrido completo (6 configuraciones, 4 semillas, 3000
   pasos); 21 mutantes a mano, 20 cazados, el otro solo cambia velocidad. Los 3 huecos del paso 1 cerrados.

Resultados (M2, go1.27.3, mediana de 10, del archivo de `benchmarks/`; ε y shards por defecto: 2719×5 entre
16 shards, 1 h a 1 s, salvo `StoreHotKeyNanos`, con ventana de 5 min; la última columna es de la ejecución
anterior al paso 4, mediana de 5):

| Benchmark | exacto | sketch | sketch antes del paso 4 |
|---|---|---|---|
| `StoreDense` (1000 eventos por bucket) | 62,1 ns | 35,6 ns | 60,0 ns |
| `StoreHotKeyNanos` | 17,6 ns | 20,8 ns | 21,0 ns |
| `StoreHotKeySameBucket` | 17,6 ns | 20,5 ns | 20,7 ns |
| `GetHitManyKeys` | 27,7 ns | 16,7 ns | 16,7 ns |
| `ParallelStoreDense` (4 CPU) | 21,7 ns | 21,8 ns | 27,4 ns |
| `StoreManyKeys` (un bucket nuevo por op) | 33,6 ns | 765 ns | 3193 ns |
| `StoreSingleKey` (un bucket nuevo por op) | 24,4 ns | 342 ns | 1933 ns |
| `ParallelStore` (4 CPU, un bucket nuevo por op) | 18,0 ns | 159 ns | 1035 ns |
| `ParallelMixed` (4 CPU) | 12,7 ns | 43,6 ns | 292 ns |

- Objetivo `Store` <= 3× el exacto: se cumple desde ~110 eventos por bucket en toda la cache, no por
  debajo. Barrido medido (100K keys, 1 h a 1 s, ε por defecto), exacto / sketch en ns: 1 evento por bucket
  34 / 759; 10: 34 / 463; 30: 36 / 277; 100: 37 / 119; 300: 43 / 57; 1000: 55 / 35; 10.000: 72 / 26. El
  coste de caducar (~10 µs por bucket: 16 slots) se reparte entre los eventos del bucket y crece como 1/ε.
  **Re-alcance del objetivo original, aceptado el 2026-11-24.**
- Peor caso, un bucket nuevo por operación: cada `Store` caduca un slot entero (850 celdas con 16 shards), 14× a
  23× el exacto. Es el coste de caducar: `StoreHighWaterJump` da 338 ns por slot y 559 µs por una ventana
  entera de 1800 slots, una vez por shard.
- Coste independiente de B: 34,0 ns (60 buckets) y 35,0 ns (1800), denso. En disperso, 451 y 711 ns: es
  localidad de memoria (anillo de 3,3 MB frente a 98 MB), no el algoritmo; con un anillo que cabe en caché en
  los dos casos (ε=0,02) sale 114 y 119 ns.
- RAM: 196.591.872 B medidos frente a 196.409.600 B estimados (+0,09%), igual con 100K y con 1M de keys.
- 0 allocs por operación en todas las filas del sketch.
- No se implementa la reconstrucción desde supervivientes (ahorro máximo 2× en un salto de HW, que es raro).
- Sin verificar: tests en 32 bit (solo vet de los paquetes nuevos); `Stats.Saturated` sigue sin cubrir.
**Fase 4 — docs y deuda. HECHA (2026-11-24, rama `feat/countmin-phase-4-docs`).** Alcance original: README
(sección sketch, sizing, tabla §6), contrato de la interface, Makefile (`PKG`), y opcional extraer
`internal/window` (toca raíz -> gate 3%). Resultado:
- Raíz: solo comentarios en `slidingcache.go` (doc de `SlidingCache`: implementación exacta y aproximada,
  comparar con `>=`), README y Makefile. El diff de `.go` de raíz son solo líneas `//`: el gate del 3% no aplica.
- README: sección `countminsketch` (cuándo usarlo y cuándo no, qué se mantiene, tabla de qué cambia, sizing,
  configuración, coste), viñeta en la intro, bloque «Public interface» y las tres correcciones de la deuda 7.
  Tabla de RAM comprobada celda a celda contra `layout()` / `memoryBytes()`; cifras de coste, del archivo de
  la Fase 3.
- `ExampleNew` compila el snippet del README (sin `// Output:`: lee el reloj).
- Makefile `PKG ?= .` en `test-bench`, `test-bench-stat`, `test-bench-save`, `test-fuzz`, `test-fuzz-one` y
  `test-fuzz-all`. Con el valor por defecto, `make -n` solo cambia en el `.` explícito de los tres de bench.
  Ejecutados con `PKG`: `test-fuzz`, `test-fuzz-one`, `test-fuzz-all` y `test-bench`. `test-bench-save` y
  `test-bench-stat` con `PKG`, solo `make -n`. `-fuzz` admite un único paquete.
- `BenchmarkStoreDensity`: el barrido de densidad de la Fase 3 pasa a ser un benchmark del repo. Mediana de
  10, exacto / sketch en ns: 1 evento por bucket 34 / 757; 10: 34 / 466; 30: 36 / 277; 100: 36 / 121; 300:
  43 / 56; 1000: 58 / 35; 10.000: 70 / 27. Mismo resultado que el barrido a mano: 3× a ~110 eventos por
  bucket, igualdad a ~450. Archivado en `benchmarks/countminsketch-phase4-density.txt` (commit 36f5516).
- `make ci` verde en go1.26.4 y go1.27.3 (cobertura 100% raíz, 98,6% y 98,2%). Fusión simulada con las PR
  #17 y #18: sin conflictos en `README.md`, `benchmarks/README.md` ni `slidingcache.go`.
- No hecho: `internal/window` (opcional; es un refactor de raíz con gate del 3%). Descartado por ahora
  (decisión del 2026-11-24); la deuda 5 sigue abierta.
**Fase 5 — conservative update. HECHA (2026-11-24, rama `feat/countmin-phase-5-conservative-update`), con
el gate de precisión re-alcanzado y aceptado: el original no se cumple tal como estaba escrito.** Alcance
original: solo sube las celdas por debajo del nuevo mínimo del slot y refleja el mismo delta en el agregado
(I1 se mantiene). Gate: nunca subestima, error medio <= 0,6× el clásico en uniforme y Zipf, modo A sigue
exacto. Coste asumido: `Store` deja de subir siempre +1.

Diseño:
- `Ring.Add`: m = la menor de las celdas de la key en su **slot**; suben en 1, en el slot y en el agregado,
  solo las filas cuya celda vale m. Sin API nueva. `Sketch` sigue clásico: es la referencia de los tests.
- El mínimo sale del slot, no del agregado. Con el agregado, una fila que otros slots ya habían subido no se
  toca; al caducar esos slots la key queda por debajo de su conteo real (hay test determinista).
- Nunca subestima: el agregado es un mínimo de sumas, >= la suma de los mínimos por slot, >= el conteo. Ninguna
  celda supera la del clásico, así que la cota (ε, δ) no cambia.
- Saturación: solo cuando ese mínimo ya vale 2^32−1 (antes, cualquier fila saturada).
- Efecto visible: dos `Store` seguidos de una key pueden devolver el mismo conteo (solo con colisiones).

Precisión, simulada fuera del repo (100K keys, 2719×5, 600 buckets): error medio con conservative update /
con el clásico. Carga = eventos por bucket / ancho.

| Eventos por bucket (carga) | Uniforme | Zipf 1,1, por key | Zipf 1,1, ponderado por eventos | `Store` que no sube (uniforme / Zipf) |
|---|---|---|---|---|
| 10 (0,004) | 1,00 | 0,99 | — | 0% / 0% |
| 100 (0,04) | 0,98 | 0,99 | 0,52-0,57 | 1% / 0% |
| 1000 (0,37) | 0,86 | 0,96 | 0,36-0,41 | 13% / 1% |
| 10.000 (3,7) | 0,55 | 0,77 | 0,19-0,22 | 41% / 6% |
| 100.000 (37) | 0,48 | 0,59-0,60 | 0,12-0,13 | 44% / 6% |

- Con 1 y con 16 shards sale lo mismo. Una semilla por fila.
- Workload de §4 (3 semillas, sobreconteo mediano): ε=1e-3, +300.000 -> +141.000 (0,47×); ε=1e-4, +19.300 ->
  +6.500 (0,34×, de 1,9× a 0,65× el valor real). La conclusión de §4 no cambia.
- **Gate de precisión:** <= 0,6× solo se cumple en uniforme desde ~10.000 eventos por bucket (carga >= 3,7).
  En Zipf por key llega a 0,59-0,60 con 100.000. Con <= 1000 eventos por bucket la mejora es <= 14%.
  Re-alcance aceptado el 2026-11-24: nunca por encima del clásico, celda a celda; <= 0,65× en uniforme y
  <= 0,8× en Zipf por key con carga >= 3,7 (~10.000 eventos por bucket con ε por defecto).
- En el repo: anillo de 64×4, 8 slots, 4000 keys, 5 semillas. Uniforme 0,55-0,57 (carga 3,7) y 0,51 (carga
  37); Zipf 0,62-0,65 (carga 3,7). Umbrales de los tests: 0,65 y 0,8.

Coste (M2, A/B intercalado en dos rondas, 20 muestras por lado, brazo sketch; exacto del archivo):

| Benchmark | antes | conservative update | exacto |
|---|---|---|---|
| `RingAdd` 2719×5 | 9,6 ns | 12,5 ns | — |
| `StoreDense` | 34,7 ns | 40,6 ns | 62,1 ns |
| `StoreMediumCardinalitySameBucket` (10K keys) | 25,3 ns | 31,5 ns | 27,4 ns |
| `StoreHotKeyNanos` | 20,9 ns | 24,9 ns | 17,6 ns |
| `StoreDensity`, 100 eventos por bucket | 119,9 ns | 123,1 ns | 36,3 ns |
| `StoreDensity`, 1000 | 35,1 ns | 40,4 ns | 58,3 ns |
| `StoreManyKeys` | 743 ns | 750 ns | 33,6 ns |
| `ParallelStoreDense` (4 CPU) | 22,9 ns | 23,7 ns | 21,7 ns |

- `Ring.Add` queda por encima del límite de 12 ns de la Fase 1, que se fijó para el clásico; aceptado el
  2026-11-24. `Count`, `Expire` y `Get` no cambian. 0 allocs.
- `Store` sube 4-6 ns donde manda la actualización de contadores (+17% a +24%) y ~1% donde manda la caducidad.
- El objetivo de 3× pasa de ~110 a ~120 eventos por bucket; la igualdad con el exacto, de ~450 a ~550.
- Idea no probada para bajar de 12 ns: camino recto para profundidad 5, sin bucle ni array de índices.

Verificación:
- `make ci` verde en go1.26.4 y go1.27.3 (cobertura 100% raíz, 98,6% y 98,4%); vet linux/386 de los paquetes
  nuevos; `-race -count=5`. Raíz sin tocar.
- Fuzz 30 s por target, 0 fallos: 10,1M y 3,5M ejecuciones en `internal/countmin`; 257K, 194K, 48,6K y 36,2K
  en `countminsketch`. `FuzzRingWindow` comprueba propiedades (>= conteo exacto tras caducar, cada celda <=
  la del clásico, I1), no un modelo espejo. El modo de colisiones corre ahora con profundidad 1, 2 o 3 y su
  cota superior es la estimación del clásico.
- Mutantes a mano: 19 míos y 15 de Pedro sobre `Ring.Add`, todos cazados. Huecos encontrados y cerrados: el
  fuzz de colisiones solo usaba profundidad 1, donde las dos actualizaciones coinciden (con la semilla nueva
  caza «mínimo del agregado» en 500 de 500 ejecuciones); «el mínimo ignora la última fila» solo caía a nivel
  de cache (test nuevo a nivel de anillo).
- CI de Linux en verde en la PR #23.
- Archivado con `make test-bench-save PR=countminsketch-phase5 PKG=./countminsketch
  BASELINE=benchmarks/countminsketch-phase3` (primera ejecución real del target con `PKG`; benchstat incluido),
  sobre el commit 1f42463. Coincide con el A/B: `StoreDense` 35,6 -> 40,2 ns, `StoreHotKeyNanos` 20,8 -> 24,9,
  `StoreMediumCardinalitySameBucket` 25,5 -> 31,2; densidad 100: 123 ns (3,4× el exacto). RAM igual:
  196.591.872 B. El README cita ya las cifras del archivo.
- Sin verificar: tests en 32 bit; rama `Stats.Saturated++`.

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
  La ausencia de colisiones se comprueba, no se supone: cada hash sumado una vez a un agregado debe contar 1.
- **Modo B, hash por defecto y sketch estrecho:** centinelas iguales, `got >= want` y `got <=` suma de los
  conteos del oráculo sobre las keys del fuzz.
- Tras cada op se comparan también `HighWater()` y los contadores de rechazo. Las celdas de `countmin` no son
  visibles desde la cache, así que I1 se comprueba reconstruyendo un agregado desde los slots vivos y
  comparando `Count` por key; I2 e I3, directamente.
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
- Salto de HW: pico de latencia bajo el lock de la shard, un slot por bucket saltado: medido 338 ns por slot
  y 559 µs por una ventana entera de 1800 slots (16 shards, ε por defecto), una vez por shard. Con
  `Shards: 1` el mismo salto recorre el anillo entero bajo el único lock.
- Tráfico disperso: con pocos eventos por bucket el coste de caducar domina (22× el exacto con 1 evento
  por bucket, 3× con ~110, igual con ~450; ε por defecto). El umbral crece como 1/ε.
- Primer `expireTo` de una cache virgen: la distancia al centinela desborda si se resta sin saturar.
- Saturación de celda (Precision grande + key muy caliente): el conteo se queda corto; contarlo en Stats.
- Cambiar ε/δ exige cache nueva. `New` reserva toda la ventana; después no se aloca nada.
- Config que pide más RAM que `MaxMemoryBytes`: error de `New`, no un fallo al reservar.
- `HashFunc` débil del usuario fusiona keys. Plataformas de 32 bit: `int` de retorno, igual que hoy.

## 10. Deuda técnica detectada

1. `go.mod` en `go 1.26` mientras los benchmarks archivados dicen go1.27.0 (se queda así por decisión).
2. Tags v1.4.0 y v1.5.0 en el mismo commit; sin baseline v1.4/v1.5; `BASELINE` del Makefile sigue en v1.2.0.
3. **Resuelta (Fase 4).** Makefile: `test-bench*` y `test-fuzz*` solo corrían el paquete raíz (`.`); ahora
   aceptan `PKG` (un solo paquete en los de fuzz).
4. **Resuelta (Fase 4).** La doc de `SlidingCache` prometía exactitud ("stores 1 and returns 1", "Returns 0
   if the key does not exist"); ahora distingue implementación exacta y aproximada.
5. Lógica de ventana no exportada en raíz -> duplicada hasta extraer `internal/window`.
6. Guard de rango (±2^(63−CountBits)) es regla de dominio colgada de un detalle de almacenamiento.
7. **Resuelta (Fase 4).** README obsoleto: "Future/GetFuture stay at 0 until MaxFutureSkew... land" (ya
   está); el export de ejemplo omitía `GetFuture`; `HighWater` decía "ever accepted" y el código "ever advanced".
8. `slidingcache.go:350`: el comentario de `layout` quedó encima de `maxFutureSkew`.
9. `SlidingCache` no incluye `Close/HighWater/Stats`: tras la interface se pierde ciclo de vida y diagnóstico.
10. FNV-1a por defecto: predecible y ~7× más lento que `maphash` en keys de 58 B. En raíz solo sesga shards.
11. **Resuelta (2026-11-23).** `make lint` fallaba en local con go1.27.3: golangci-lint 2.12.2 no
    leía el export data v5 de la stdlib 1.27. Subido a v2.15.0 en `tools/go.mod` (`go 1.26.0` mínimo, CI no
    cambia): 0 issues en raíz con go1.27.3 y con go1.26.4. En `.golangci.yml`, `gofumpt.extra-rules` (obsoleto
    en 2.15) pasa a `extra` con las tres reglas que ya activaba.

12. `isFuture` suma `bucket(clock()) + skew` sin guardar el desbordamiento: un `Clock` que devuelva un valor
    cercano a MaxInt64 lo hace negativo y rechaza todo como futuro. Está en raíz y replicado en `countminsketch`.
13. `GOOS=linux GOARCH=386 go vet ./...` falla en `main`: `slidingcache_test.go:772` pasa `1<<62 + 1` a un
    `int`. Los tests de raíz no compilan en 32 bit; los paquetes nuevos sí pasan vet. La CI solo corre amd64.

## 11. Preguntas abiertas

Ninguna. Pendiente de dato externo: el umbral real del consumidor, que la librería no recoge (§4).
