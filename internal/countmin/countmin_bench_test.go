package countmin

import (
	"fmt"
	"hash/maphash"
	"testing"
)

// The sizes span a sketch that fits in L1, the 2719x5 of epsilon 0.001 and delta
// 0.01 that the Phase 1 gate is measured at, and one that spills out of L2. The
// 2719x7 of delta 0.001 measures a depth that Ring.Add has no fast path for.
var benchSizes = []struct{ width, depth int }{
	{22, 5},
	{2719, 5},
	{271829, 5},
	{2719, 7},
}

const (
	benchKeyCount = 10_000
	benchSlots    = 8
	// benchSlotShift rotates the slot Ring.Add counts into every 4096
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

func mustBenchRing(b *testing.B, width, depth int) *Ring {
	b.Helper()
	r, err := NewRing(width, depth, benchSlots)
	if err != nil {
		b.Fatal(err)
	}
	return r
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

func BenchmarkRingAdd(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			r := mustBenchRing(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				estimate, _ := r.Add((i>>benchSlotShift)%benchSlots, hashes[i%benchKeyCount])
				benchSink += estimate
			}
		})
	}
}

func BenchmarkRingCount(b *testing.B) {
	hashes := benchHashes()
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			r := mustBenchRing(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSink += r.Count(hashes[i%benchKeyCount])
			}
		})
	}
}

// BenchmarkRingExpire measures expiring one slot: subtracting it from the
// aggregate and emptying it for reuse.
func BenchmarkRingExpire(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(benchName(size.width, size.depth), func(b *testing.B) {
			r := mustBenchRing(b, size.width, size.depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Expire(i % benchSlots)
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
