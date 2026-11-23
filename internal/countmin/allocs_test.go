package countmin

import "testing"

func TestHotPathsDoNotAllocate(t *testing.T) {
	s := mustSketch(t, 2719, 5)
	a := mustAggregate(t, 2719, 5)
	slot := a.NewSlot()
	item := []byte("00000000000000000042")
	h := splitmix64(42)
	var sink uint64

	operations := map[string]func(){
		"Sketch.Add":          func() { s.Add(item) },
		"Sketch.AddN":         func() { s.AddN(item, 3) },
		"Sketch.Count":        func() { sink += s.Count(item) },
		"Sketch.AddHash":      func() { s.AddHash(h) },
		"Sketch.CountHash":    func() { sink += s.CountHash(h) },
		"Sketch.Reset":        func() { s.Reset() },
		"Aggregate.Add":       func() { estimate, _ := a.Add(slot, h); sink += estimate },
		"Aggregate.Count":     func() { sink += a.Count(h) },
		"Aggregate.Sub":       func() { _ = a.Sub(slot) },
		"Aggregate.AddSketch": func() { _ = a.AddSketch(slot) },
		"Aggregate.Reset":     func() { a.Reset() },
		"ShardOf":             func() { sink += ShardOf(h, 4) },
	}
	for name, op := range operations {
		if allocs := testing.AllocsPerRun(100, op); allocs != 0 {
			t.Errorf("%s allocates %v times per call, want 0", name, allocs)
		}
	}
	_ = sink
}
