package countmin

import "testing"

func TestHotPathsDoNotAllocate(t *testing.T) {
	s := mustSketch(t, 2719, 5)
	r := mustRing(t, 2719, 5, 8)
	item := []byte("00000000000000000042")
	h := splitmix64(42)
	var sink uint64
	slotCells, aggCells := make([]uint32, 0, 2719*5), make([]uint64, 0, 2719*5)

	operations := map[string]func(){
		"Sketch.Add":       func() { s.Add(item) },
		"Sketch.AddN":      func() { s.AddN(item, 3) },
		"Sketch.Count":     func() { sink += s.Count(item) },
		"Sketch.AddHash":   func() { s.AddHash(h) },
		"Sketch.CountHash": func() { sink += s.CountHash(h) },
		"Sketch.Reset":     func() { s.Reset() },
		"Ring.Add":         func() { estimate, _ := r.Add(3, h); sink += estimate },
		"Ring.Count":       func() { sink += r.Count(h) },
		"Ring.Expire":      func() { r.Expire(3) },
		"Ring.Reset":       func() { r.Reset() },
		"Ring.AppendCells": func() {
			slotCells = r.AppendSlotCells(slotCells[:0], 3)
			aggCells = r.AppendAggregateCells(aggCells[:0])
		},
		"ShardOf": func() { sink += ShardOf(h, 4) },
	}
	for name, op := range operations {
		if allocs := testing.AllocsPerRun(100, op); allocs != 0 {
			t.Errorf("%s allocates %v times per call, want 0", name, allocs)
		}
	}
	_ = sink
}
