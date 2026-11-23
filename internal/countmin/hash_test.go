package countmin

import (
	"fmt"
	"testing"
)

func TestSaltsAreOddAndDistinct(t *testing.T) {
	salts := append(rowSalts[:], shardSalt)
	seen := make(map[uint64]bool, len(salts))
	for _, salt := range salts {
		if salt%2 == 0 {
			t.Errorf("salt %#x is even", salt)
		}
		if seen[salt] {
			t.Errorf("salt %#x appears twice", salt)
		}
		seen[salt] = true
	}
}

// jointCollisions counts the pairs of keys whose first coordinate and second
// coordinate both coincide, given each key's coordinates, the second in [0, n).
func jointCollisions(first, second []uint64, n uint64) int {
	buckets := make(map[uint64]int)
	for k := range first {
		buckets[first[k]*n+second[k]]++
	}
	pairs := 0
	for _, c := range buckets {
		pairs += c * (c - 1) / 2
	}
	return pairs
}

// assertNearIndependent checks a pair count against keys*(keys-1)/2/cells, what
// independent uniform coordinates over cells joint values would give.
func assertNearIndependent(t *testing.T, what string, pairs, keys, cells int) {
	t.Helper()
	expected := float64(keys) * float64(keys-1) / 2 / float64(cells)
	if got := float64(pairs); got < 0.75*expected || got > 1.25*expected {
		t.Errorf("%s: %d colliding pairs, want within 25%% of %.0f", what, pairs, expected)
	}
}

// TestRowsAreIndependent checks that two keys colliding in one row are no more
// likely to collide in another, for every pair of rows of a depth-16 sketch. A
// non-power-of-two width makes sure the multiply-shift scaling is exercised.
func TestRowsAreIndependent(t *testing.T) {
	const (
		width = 100
		keys  = 4_473 // about 1000 expected joint collisions per row pair.
	)
	columns := make([][]uint64, MaxDepth)
	for r := range columns {
		columns[r] = make([]uint64, keys)
		for k := range keys {
			columns[r][k] = column(splitmix64(uint64(k)), rowSalts[r], width)
		}
	}
	for r1 := range MaxDepth {
		for r2 := r1 + 1; r2 < MaxDepth; r2++ {
			pairs := jointCollisions(columns[r1], columns[r2], width)
			assertNearIndependent(t, fmt.Sprintf("rows %d,%d", r1, r2), pairs, keys, width*width)
		}
	}
}

func TestShardIsIndependentOfColumns(t *testing.T) {
	const (
		width     = 100
		shardBits = 4
		keys      = 1_789 // about 1000 expected joint collisions per row.
	)
	shards := make([]uint64, keys)
	for k := range keys {
		shards[k] = ShardOf(splitmix64(uint64(k)), shardBits)
	}
	for r := range MaxDepth {
		columns := make([]uint64, keys)
		for k := range keys {
			columns[k] = column(splitmix64(uint64(k)), rowSalts[r], width)
		}
		pairs := jointCollisions(shards, columns, width)
		assertNearIndependent(t, fmt.Sprintf("shard and row %d", r), pairs, keys, width<<shardBits)
	}
}

func TestShardOfStaysInRange(t *testing.T) {
	for bits := uint(0); bits <= 32; bits++ {
		for id := range uint64(1000) {
			if got := ShardOf(splitmix64(id), bits); got >= 1<<bits {
				t.Fatalf("ShardOf(_, %d) = %d, want below %d", bits, got, uint64(1)<<bits)
			}
		}
	}
	for _, h := range []uint64{0, 1, splitmix64(9), ^uint64(0)} {
		if got := ShardOf(h, 0); got != 0 {
			t.Fatalf("ShardOf(%#x, 0) = %d, want 0", h, got)
		}
	}
}

func TestShardOfSpreadsEvenly(t *testing.T) {
	const (
		bits     = 4
		perShard = 1_000
	)
	counts := make([]int, 1<<bits)
	for id := range uint64(perShard << bits) {
		counts[ShardOf(splitmix64(id), bits)]++
	}
	for shard, c := range counts {
		if c < perShard*85/100 || c > perShard*115/100 {
			t.Errorf("shard %d got %d hashes, want within 15%% of %d", shard, c, perShard)
		}
	}
}
