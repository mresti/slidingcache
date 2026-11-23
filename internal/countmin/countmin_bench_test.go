package countmin

import (
	"fmt"
	"hash/maphash"
	"testing"
)

// The sizes span a sketch that fits in L1, the 2719x5 of epsilon 0.001 and delta
// 0.01 that the Phase 1 gate is measured at, and one that spills out of L2.
var benchSizes = []struct{ width, depth int }{
	{22, 5},
	{2719, 5},
	{271829, 5},
}

const (
	benchKeyCount = 10_000
	benchSlots    = 8
	// benchSlotShift rotates the slot Aggregate.Add counts into every 4096
	// operations, the same rotation as the prototype the gate compares with.
	benchSlotShift = 12
)

var benchSink uint64

// benchKeys returns 20-byte keys, the shape the prototype benchmarks hash.
func benchKeys() [][]byte {
	keys := make([][]byte, benchKeyCount)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "%020d", uint64(i)*0x9e3779b97f4a7c15)
	}
	return keys
}

func benchHashes() []uint64 {
	seed := maphash.MakeSeed()
	hashes := make([]uint64, benchKeyCount)
	for i, key := range benchKeys() {
		hashes[i] = maphash.Bytes(seed, key)
	}
	return hashes
}

func benchName(width, depth int) string {
	return fmt.Sprintf("size=%dx%d", width, depth)
}

func mustBenchSketch(b *testing.B, width, depth int) *Sketch {
	b.Helper()
	s, err := NewSized(width, depth)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func mustBenchAggregate(b *testing.B, width, depth int) (*Aggregate, []*Sketch) {
	b.Helper()
	a, err := NewAggregate(width, depth)
	if err != nil {
		b.Fatal(err)
	}
	slots := make([]*Sketch, benchSlots)
	for i := range slots {
		slots[i] = a.NewSlot()
	}
	return a, slots
}

func BenchmarkSketchAddHash(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			s := mustBenchSketch(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.AddHash(hashes[i%benchKeyCount])
			}
		})
	}
}

func BenchmarkSketchCountHash(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			s := mustBenchSketch(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink += s.CountHash(hashes[i%benchKeyCount])
			}
		})
	}
}

func BenchmarkSketchAdd(b *testing.B) {
	keys := benchKeys()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			s := mustBenchSketch(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Add(keys[i%benchKeyCount])
			}
		})
	}
}

func BenchmarkSketchCount(b *testing.B) {
	keys := benchKeys()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			s := mustBenchSketch(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink += s.Count(keys[i%benchKeyCount])
			}
		})
	}
}

func BenchmarkAggregateAdd(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			a, slots := mustBenchAggregate(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				estimate, _ := a.Add(slots[(i>>benchSlotShift)%benchSlots], hashes[i%benchKeyCount])
				benchSink += estimate
			}
		})
	}
}

func BenchmarkAggregateCount(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			a, _ := mustBenchAggregate(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink += a.Count(hashes[i%benchKeyCount])
			}
		})
	}
}

// BenchmarkAggregateExpire measures expiring one slot: subtracting it from the
// aggregate and emptying it for reuse.
func BenchmarkAggregateExpire(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			a, slots := mustBenchAggregate(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				slot := slots[i%benchSlots]
				if err := a.Sub(slot); err != nil {
					b.Fatal(err)
				}
				slot.Reset()
			}
		})
	}
}

func BenchmarkShardOf(b *testing.B) {
	hashes := benchHashes()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchSink += ShardOf(hashes[i%benchKeyCount], 4)
	}
}
