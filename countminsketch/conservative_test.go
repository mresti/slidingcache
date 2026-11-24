package countminsketch

import (
	"fmt"
	"testing"

	"github.com/mresti/slidingcache/internal/countmin"
)

// cellsOf returns the index of h's cell in each row of a width x depth sketch,
// found from outside the package as the cells a single add raises in a fresh
// ring.
func cellsOf(t *testing.T, width, depth int, h uint64) []int {
	t.Helper()
	ring, err := countmin.NewRing(width, depth, 1)
	if err != nil {
		t.Fatalf("NewRing(%d, %d, 1): %v", width, depth, err)
	}
	ring.Add(0, h)
	var cells []int
	for j, c := range ring.AppendSlotCells(nil, 0) {
		if c != 0 {
			cells = append(cells, j)
		}
	}
	if len(cells) != depth {
		t.Fatalf("one add raised %d cells, want one per row of %d", len(cells), depth)
	}
	return cells
}

// hashSharingOnlyRow returns the first splitmix64 hash, searching from id 1000
// on, whose cell is partner's in row and differs from partner's in every other
// row.
func hashSharingOnlyRow(t *testing.T, width, depth int, partner uint64, row int) uint64 {
	t.Helper()
	return hashSharingRows(t, width, depth, partner, func(r int) bool { return r == row })
}

// hashSharingRows returns the first splitmix64 hash, searching from id 1000 on,
// whose cell is partner's in exactly the rows for which shared is true.
func hashSharingRows(t *testing.T, width, depth int, partner uint64, shared func(row int) bool) uint64 {
	t.Helper()
	want := cellsOf(t, width, depth, partner)
	for id := uint64(1000); id < 100_000; id++ {
		h := splitmix64(id)
		got := cellsOf(t, width, depth, h)
		matches := true
		for r := range depth {
			if (got[r] == want[r]) != shared(r) {
				matches = false
				break
			}
		}
		if matches {
			return h
		}
	}
	t.Fatalf("no hash shares the wanted rows with %#x at %dx%d", partner, width, depth)
	return 0
}

// TestStoreCanReturnTheSameCountTwice pins the consequence of the conservative
// update: a Store raises only the rows at the minimum of its bucket's slot, so
// when the key's smallest window cell is in a row that is not raised, the
// returned count does not move. On one shard of 4 columns, z is hot in an
// older bucket and shares every row of x but row 0; y is hot in the current
// bucket and shares x's row 0 only. Each Store of x raises every row but row 0
// of the current slot, while the window estimate is row 0, held at y's 5. The
// classic update, or a minimum read from the window instead of the slot,
// would raise row 0 too. Depth 5 is the default Delta's, which Ring.Add
// unrolls.
func TestStoreCanReturnTheSameCountTwice(t *testing.T) {
	for _, tt := range []struct {
		delta float64
		depth int
	}{{0.2, 2}, {0.01, 5}} {
		t.Run(fmt.Sprintf("depth=%d", tt.depth), func(t *testing.T) {
			cfg := secondsConfig(0)
			cfg.Shards, cfg.Epsilon, cfg.Delta = 1, 0.9, tt.delta
			layout := mustLayout(t, cfg)
			if layout.shards != 1 || layout.shardWidth != 4 || layout.depth != tt.depth {
				t.Fatalf("layout %+v, want one shard of 4 x %d", layout, tt.depth)
			}
			x := splitmix64(1)
			hashes := map[string]uint64{
				"x": x,
				"y": hashSharingOnlyRow(t, layout.shardWidth, layout.depth, x, 0),
				"z": hashSharingRows(t, layout.shardWidth, layout.depth, x, func(row int) bool { return row != 0 }),
			}
			c := newTestCache(t, cfg, WithHashFunc(func(key string) uint64 { return hashes[key] }))
			const older, current = 1_000, 1_010
			for range 5 {
				c.Store(older, "z")
				c.Store(current, "y")
			}

			first, second := c.Store(current, "x"), c.Store(current, "x")

			if first != 5 || second != 5 {
				t.Fatalf("two Stores of x returned %d and %d, want 5 and 5", first, second)
			}
			if got := c.Get(current, "x"); got != 5 {
				t.Fatalf("Get(x) = %d, want 5", got)
			}
		})
	}
}

// TestStoreRisesByOneWithACollisionFreeHash is the other side: with the hashes
// of Mode A, each alone in at least one row, every Store returns one more than
// the last, across buckets and with the other keys counted in between.
func TestStoreRisesByOneWithACollisionFreeHash(t *testing.T) {
	cfg := secondsConfig(0)
	requireCollisionFree(t, cfg, exactHashes())
	c := newTestCache(t, cfg, WithHashFunc(exactHash))
	k0 := testKeys[0]
	want := 0
	for epoch := int64(1_000); epoch < 1_040; epoch += 3 {
		for _, other := range testKeys[1:] {
			c.Store(epoch, other)
		}
		want++
		if got := c.Store(epoch, k0); got != want {
			t.Fatalf("Store %d of %s at %d = %d, want %d", want, k0, epoch, got, want)
		}
	}
	if got := c.Get(1_039, k0); got != want {
		t.Fatalf("Get(%s) = %d, want %d", k0, got, want)
	}
}
