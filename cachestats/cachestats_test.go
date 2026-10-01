package cachestats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

func TestDiff(t *testing.T) {
	prev := slidingcache.Stats{
		Accepted: 100, Late: 10, Future: 1, OutOfRange: 2,
		GetHit: 50, GetMiss: 20, GetLate: 3, GetFuture: 4, Keys: 30,
	}

	tests := []struct {
		name string
		prev slidingcache.Stats
		cur  slidingcache.Stats
		want Delta
	}{
		{
			name: "incremento normal",
			prev: prev,
			cur: slidingcache.Stats{
				Accepted: 150, Late: 12, Future: 1, OutOfRange: 5,
				GetHit: 80, GetMiss: 21, GetLate: 3, GetFuture: 9, Keys: 35,
			},
			want: Delta{
				Accepted: 50, Late: 2, Future: 0, OutOfRange: 3,
				GetHit: 30, GetMiss: 1, GetLate: 0, GetFuture: 5, Keys: 35,
			},
		},
		{
			name: "intervalo sin tráfico da ceros, no el valor anterior",
			prev: prev,
			cur:  prev,
			want: Delta{Keys: 30},
		},
		{
			name: "Keys baja por el janitor y no es un reset",
			prev: prev,
			cur: slidingcache.Stats{
				Accepted: 101, Late: 10, Future: 1, OutOfRange: 2,
				GetHit: 50, GetMiss: 20, GetLate: 3, GetFuture: 4, Keys: 5,
			},
			want: Delta{Accepted: 1, Keys: 5},
		},
		{
			name: "un solo contador retrocede: reset de todos contra cero",
			prev: prev,
			cur: slidingcache.Stats{
				Accepted: 7, Late: 11, Future: 1, OutOfRange: 2,
				GetHit: 60, GetMiss: 20, GetLate: 3, GetFuture: 4, Keys: 3,
			},
			want: Delta{
				Accepted: 7, Late: 11, Future: 1, OutOfRange: 2,
				GetHit: 60, GetMiss: 20, GetLate: 3, GetFuture: 4, Keys: 3,
				Reset: true,
			},
		},
		{
			name: "base cero",
			prev: slidingcache.Stats{},
			cur:  prev,
			want: Delta{
				Accepted: 100, Late: 10, Future: 1, OutOfRange: 2,
				GetHit: 50, GetMiss: 20, GetLate: 3, GetFuture: 4, Keys: 30,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diff(tt.prev, tt.cur)
			if got != tt.want {
				t.Fatalf("Diff()\n got  %+v\n want %+v", got, tt.want)
			}
		})
	}
}

func TestDeltaTotals(t *testing.T) {
	d := Delta{
		Accepted: 1, Late: 2, Future: 3, OutOfRange: 4,
		GetHit: 10, GetMiss: 20, GetLate: 30, GetFuture: 40,
	}
	if got := d.StoreCalls(); got != 10 {
		t.Errorf("StoreCalls() = %d, want 10", got)
	}
	if got := d.GetCalls(); got != 100 {
		t.Errorf("GetCalls() = %d, want 100", got)
	}
}

// recorder acumula los Delta emitidos. Solo lo escribe la goroutine de Run;
// se lee cuando Run ha retornado.
type recorder struct {
	emissions atomic.Int64 // legible desde otras goroutines mientras Run corre
	n         int
	resets    int
	sum       Delta
	last      Delta
	emitted   chan struct{} // opcional: señaliza cada Emit sin bloquear
}

func (r *recorder) Emit(d Delta) {
	r.emissions.Add(1)
	r.n++
	if d.Reset {
		r.resets++
	}
	r.sum.Accepted += d.Accepted
	r.sum.Late += d.Late
	r.sum.Future += d.Future
	r.sum.OutOfRange += d.OutOfRange
	r.sum.GetHit += d.GetHit
	r.sum.GetMiss += d.GetMiss
	r.sum.GetLate += d.GetLate
	r.sum.GetFuture += d.GetFuture
	r.last = d
	if r.emitted != nil {
		select {
		case r.emitted <- struct{}{}:
		default:
		}
	}
}

// opCounts cuenta, por resultado esperado, las llamadas que hace el test.
type opCounts struct {
	accepted, late, future, outOfRange  atomic.Uint64
	getHit, getMiss, getLate, getFuture atomic.Uint64
}

// TestEmitterConservation ejercita los ocho resultados posibles de Store y Get
// desde varias goroutines mientras el Emitter emite, y comprueba que la suma
// de todos los Delta coincide exactamente con las llamadas hechas, resultado
// a resultado, y con el Stats final. Ejecutar con -race.
func TestEmitterConservation(t *testing.T) {
	const base int64 = 1_000_000 // segundos

	cache, err := slidingcache.New(slidingcache.Config{
		Precision:     time.Second,
		WindowSize:    60 * time.Second,
		EpochUnit:     slidingcache.EpochInSeconds,
		MaxFutureSkew: 60 * time.Second,
		Clock:         func() int64 { return base }, // reloj fijo: resultados deterministas
		Shards:        64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	rec := &recorder{}
	em, err := New(cache, rec, 2*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- em.Run(ctx) }()

	var ops opCounts

	// Fija el high-water mark en base; ningún Store posterior lo mueve.
	if n := cache.Store(base, "seed"); n != 1 {
		t.Fatalf("seed Store = %d, want 1", n)
	}
	ops.accepted.Add(1)

	keys := [...]string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"}

	// El tráfico dura hasta que el Emitter ha hecho minEmissions emisiones,
	// para que haya snapshots en mitad del tráfico, no solo al final.
	const minEmissions = 10
	deadline := time.Now().Add(10 * time.Second)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			key := keys[g]
			for i := int64(0); rec.emissions.Load() < minEmissions && time.Now().Before(deadline); i++ {
				cache.Store(base-i%30, key) // dentro de la ventana
				ops.accepted.Add(1)
				cache.Store(base-120, key) // <= HW-WindowSize
				ops.late.Add(1)
				cache.Store(base+3600, key) // > reloj+MaxFutureSkew
				ops.future.Add(1)
				cache.Store(1<<50, key) // fuera de ±2^43 s
				ops.outOfRange.Add(1)

				cache.Get(base, key) // la clave existe
				ops.getHit.Add(1)
				cache.Get(base, "absent")
				ops.getMiss.Add(1)
				cache.Get(base-120, key)
				ops.getLate.Add(1)
				cache.Get(base+3600, key)
				ops.getFuture.Add(1)
			}
		})
	}
	wg.Wait()

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run() = %v", err)
	}

	final := cache.Stats()
	want := Delta{
		Accepted: ops.accepted.Load(), Late: ops.late.Load(),
		Future: ops.future.Load(), OutOfRange: ops.outOfRange.Load(),
		GetHit: ops.getHit.Load(), GetMiss: ops.getMiss.Load(),
		GetLate: ops.getLate.Load(), GetFuture: ops.getFuture.Load(),
	}
	got := rec.sum

	if got != want {
		t.Fatalf("suma de Delta\n got  %+v\n want %+v", got, want)
	}
	fromStats := Diff(slidingcache.Stats{}, final)
	fromStats.Keys = 0 // Keys no se suma: es instantáneo
	if got != fromStats {
		t.Fatalf("suma de Delta distinta del Stats final\n got   %+v\n stats %+v", got, fromStats)
	}
	if rec.last.Keys != final.Keys {
		t.Errorf("Keys del último Delta = %d, Stats final = %d", rec.last.Keys, final.Keys)
	}
	if rec.resets != 0 {
		t.Errorf("%d Delta con Reset sobre una sola caché, want 0", rec.resets)
	}
	if rec.n < minEmissions {
		t.Errorf("solo %d emisiones; el test no cubrió varios intervalos", rec.n)
	}
	t.Logf("%d emisiones, %d Store, %d Get", rec.n, got.StoreCalls(), got.GetCalls())
}

type fakeSource struct {
	mu    sync.Mutex
	stats slidingcache.Stats
}

func (f *fakeSource) Stats() slidingcache.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func (f *fakeSource) set(s slidingcache.Stats) {
	f.mu.Lock()
	f.stats = s
	f.mu.Unlock()
}

func TestWithCurrentBaseline(t *testing.T) {
	src := &fakeSource{}
	src.set(slidingcache.Stats{Accepted: 1000, GetHit: 500})

	rec := &recorder{}
	em, err := New(src, rec, time.Hour, WithCurrentBaseline())
	if err != nil {
		t.Fatal(err)
	}
	src.set(slidingcache.Stats{Accepted: 1040, GetHit: 507})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // solo el flush final
	if err := em.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.n != 1 || rec.last.Accepted != 40 || rec.last.GetHit != 7 {
		t.Fatalf("n=%d last=%+v, want un Delta con Accepted=40 GetHit=7", rec.n, rec.last)
	}
	if rec.last.Interval <= 0 {
		t.Errorf("Interval = %s, want > 0", rec.last.Interval)
	}
}

func TestRunRejectsConcurrentRun(t *testing.T) {
	rec := &recorder{emitted: make(chan struct{}, 1)}
	em, err := New(&fakeSource{}, rec, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- em.Run(ctx) }()
	<-rec.emitted // el primer Run está en marcha

	if err := em.Run(context.Background()); !errors.Is(err, ErrRunning) {
		t.Fatalf("segundo Run() = %v, want ErrRunning", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Tras retornar, puede volver a ejecutarse.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := em.Run(ctx2); err != nil {
		t.Fatalf("Run() tras retornar = %v", err)
	}
}

type fakeGauge struct {
	v    float64
	sets int
}

func (g *fakeGauge) Set(v float64) { g.v, g.sets = v, g.sets+1 }

func newFakeGaugeSink() (*GaugeSink, []*fakeGauge) {
	gs := make([]*fakeGauge, 11)
	for i := range gs {
		gs[i] = &fakeGauge{}
	}
	return &GaugeSink{
		StoreAccepted: gs[0], StoreLate: gs[1], StoreFuture: gs[2],
		StoreOutOfRange: gs[3], StoreCalls: gs[4],
		GetHit: gs[5], GetMiss: gs[6], GetLate: gs[7], GetFuture: gs[8],
		GetCalls: gs[9], Keys: gs[10],
	}, gs
}

func TestGaugeSink(t *testing.T) {
	sink, gs := newFakeGaugeSink()

	sink.Emit(Delta{
		Accepted: 1, Late: 2, Future: 3, OutOfRange: 4,
		GetHit: 5, GetMiss: 6, GetLate: 7, GetFuture: 8, Keys: 9,
	})
	want := []float64{1, 2, 3, 4, 10, 5, 6, 7, 8, 26, 9}
	for i, g := range gs {
		if g.v != want[i] {
			t.Errorf("gauge %d = %v, want %v", i, g.v, want[i])
		}
	}

	// Intervalo sin tráfico: los once gauges se fijan, los de delta a cero.
	sink.Emit(Delta{Keys: 9})
	for i, g := range gs {
		if g.sets != 2 {
			t.Errorf("gauge %d actualizado %d veces, want 2", i, g.sets)
		}
		if i != 10 && g.v != 0 {
			t.Errorf("gauge %d = %v tras intervalo vacío, want 0", i, g.v)
		}
	}
}

func TestNewValidation(t *testing.T) {
	src := &fakeSource{}
	sink := SinkFunc(func(Delta) {})
	incomplete, _ := newFakeGaugeSink()
	incomplete.GetCalls = nil

	tests := []struct {
		name     string
		src      StatsSource
		sink     Sink
		interval time.Duration
	}{
		{"src nil", nil, sink, time.Second},
		{"sink nil", src, nil, time.Second},
		{"intervalo cero", src, sink, 0},
		{"intervalo negativo", src, sink, -time.Second},
		{"GaugeSink incompleto", src, incomplete, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.src, tt.sink, tt.interval); err == nil {
				t.Fatal("New() = nil error, want error")
			}
		})
	}
}

// BenchmarkFlush mide un intervalo completo contra una caché real de 256
// shards con GaugeSink: debe reportar 0 allocs/op.
func BenchmarkFlush(b *testing.B) {
	cache, err := slidingcache.New(slidingcache.Config{
		Precision:  time.Second,
		WindowSize: time.Minute,
		Shards:     256,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	now := time.Now().UnixMilli()
	for i := range 10_000 {
		cache.Store(now, string(rune('a'+i%26))+string(rune('a'+i/26%26)))
	}

	sink, _ := newFakeGaugeSink()
	em, err := New(cache, sink, time.Second)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		em.flush()
	}
}
