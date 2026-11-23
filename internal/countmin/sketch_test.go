package countmin

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestEmptySketchCountsNothing(t *testing.T) {
	s := mustSketch(t, 64, 4)
	if got := s.Count([]byte("absent")); got != 0 {
		t.Fatalf("Count on an empty sketch = %d, want 0", got)
	}
	if got := s.CountHash(splitmix64(1)); got != 0 {
		t.Fatalf("CountHash on an empty sketch = %d, want 0", got)
	}
	if got := s.Total(); got != 0 {
		t.Fatalf("Total of an empty sketch = %d, want 0", got)
	}
}

func TestSingleItemIsCountedExactly(t *testing.T) {
	s := mustSketch(t, 64, 4)
	s.Add([]byte("only"))
	if got := s.Count([]byte("only")); got != 1 {
		t.Fatalf("Count after one Add = %d, want 1", got)
	}
	if got := s.Total(); got != 1 {
		t.Fatalf("Total after one Add = %d, want 1", got)
	}
}

func TestAddNWithZeroChangesNothing(t *testing.T) {
	s := mustSketch(t, 64, 4)
	s.AddN([]byte("item"), 0)
	if got := s.Count([]byte("item")); got != 0 {
		t.Fatalf("Count after AddN(0) = %d, want 0", got)
	}
	if got := s.Total(); got != 0 {
		t.Fatalf("Total after AddN(0) = %d, want 0", got)
	}
}

func TestCountsAreBoundedByTruthAndTotal(t *testing.T) {
	s := mustSketch(t, 2719, 5)
	s.Add([]byte("alice"))
	s.Add([]byte("bob"))
	s.AddN([]byte("alice"), 2)

	if got := s.Count([]byte("alice")); got < 3 || got > s.Total() {
		t.Errorf("Count(alice) = %d, want between 3 and Total %d", got, s.Total())
	}
	if got := s.Count([]byte("bob")); got < 1 || got > s.Total() {
		t.Errorf("Count(bob) = %d, want between 1 and Total %d", got, s.Total())
	}
	if got := s.Total(); got != 4 {
		t.Errorf("Total = %d, want 4", got)
	}
}

// TestNeverUnderestimates uses a sketch narrow enough that most keys share cells
// with others, so the estimates are actually inflated, and checks each against
// the exact count.
func TestNeverUnderestimates(t *testing.T) {
	const keys, events = 500, 20_000
	s := mustSketch(t, 16, 3)
	exact := make(map[uint64]uint64)
	rng := rand.New(rand.NewPCG(1, 2))
	for range events {
		h := splitmix64(rng.Uint64N(keys))
		s.AddHash(h)
		exact[h]++
	}
	if got := s.Total(); got != events {
		t.Fatalf("Total = %d, want %d", got, events)
	}
	inflated := 0
	for h, want := range exact {
		got := s.CountHash(h)
		if got < want {
			t.Fatalf("CountHash = %d below the true count %d", got, want)
		}
		if got > want {
			inflated++
		}
	}
	if inflated == 0 {
		t.Fatal("no estimate was inflated: the sketch is too wide to exercise collisions")
	}
}

func TestErrorBound(t *testing.T) {
	const (
		epsilon, delta = 0.01, 0.01
		keys, events   = 5_000, 200_000
	)
	streams := map[string]func(*rand.Rand) func() uint64{
		"uniform": func(rng *rand.Rand) func() uint64 {
			return func() uint64 { return rng.Uint64N(keys) }
		},
		"zipf": func(rng *rand.Rand) func() uint64 {
			zipf := rand.NewZipf(rng, 1.1, 1, keys-1)
			return zipf.Uint64
		},
	}
	for name, newStream := range streams {
		t.Run(name, func(t *testing.T) {
			s, err := New(epsilon, delta)
			if err != nil {
				t.Fatal(err)
			}
			next := newStream(rand.New(rand.NewPCG(3, 4)))
			exact := make(map[uint64]uint64)
			for range events {
				id := next()
				s.AddHash(splitmix64(id))
				exact[id]++
			}
			allowed := uint64(epsilon * events)
			var presentOver, absentOver int
			for id, want := range exact {
				if s.CountHash(splitmix64(id))-want > allowed {
					presentOver++
				}
			}
			const absentKeys = 10_000
			for id := uint64(keys); id < keys+absentKeys; id++ {
				if s.CountHash(splitmix64(id)) > allowed {
					absentOver++
				}
			}
			if frac := float64(presentOver) / float64(len(exact)); frac > delta {
				t.Errorf("%.4f of present keys exceed the error bound, want at most %v", frac, delta)
			}
			if frac := float64(absentOver) / absentKeys; frac > delta {
				t.Errorf("%.4f of absent keys exceed the error bound, want at most %v", frac, delta)
			}
		})
	}
}

func TestSketchCellSaturatesWhileTotalStaysExact(t *testing.T) {
	s := mustSketch(t, 8, 2)
	item := []byte("hot")
	s.AddN(item, math.MaxUint32)
	s.AddN(item, 5)
	s.Add(item)
	if got := s.Count(item); got != math.MaxUint32 {
		t.Fatalf("Count of a saturated item = %d, want %d", got, uint64(math.MaxUint32))
	}
	if got, want := s.Total(), uint64(math.MaxUint32)+6; got != want {
		t.Fatalf("Total = %d, want the exact sum %d", got, want)
	}
}

func TestSketchCellSurvivesAHugeAddN(t *testing.T) {
	s := mustSketch(t, 8, 2)
	h := splitmix64(7)
	s.AddHash(h)
	s.AddHash(h)
	s.AddN([]byte("other"), math.MaxUint64)
	s.AddN([]byte("other"), math.MaxUint64)
	if got := s.Count([]byte("other")); got != math.MaxUint32 {
		t.Fatalf("Count after AddN(MaxUint64) = %d, want %d", got, uint64(math.MaxUint32))
	}
	if got := s.CountHash(h); got < 2 {
		t.Fatalf("CountHash of another item = %d, want at least 2", got)
	}
}

func TestTotalSaturatesAtMaxUint64(t *testing.T) {
	s := mustSketch(t, 8, 2)
	s.AddN([]byte("a"), math.MaxUint64-1)
	s.AddN([]byte("b"), 2)
	if got := s.Total(); got != math.MaxUint64 {
		t.Fatalf("Total = %d, want %d", got, uint64(math.MaxUint64))
	}
	s.AddHash(splitmix64(1))
	if got := s.Total(); got != math.MaxUint64 {
		t.Fatalf("Total after one more = %d, want %d", got, uint64(math.MaxUint64))
	}
}

func TestSketchResetEmptiesAndKeepsWorking(t *testing.T) {
	s := mustSketch(t, 32, 3)
	for id := range uint64(100) {
		s.AddHash(splitmix64(id))
	}
	s.Add([]byte("item"))
	s.Reset()
	if got := s.Total(); got != 0 {
		t.Fatalf("Total after Reset = %d, want 0", got)
	}
	for id := range uint64(100) {
		if got := s.CountHash(splitmix64(id)); got != 0 {
			t.Fatalf("CountHash after Reset = %d, want 0", got)
		}
	}
	s.Add([]byte("item"))
	if got := s.Count([]byte("item")); got != 1 {
		t.Fatalf("Count after Reset and one Add = %d, want 1", got)
	}
}
