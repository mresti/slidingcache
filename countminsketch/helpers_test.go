package countminsketch

import (
	"hash/maphash"
	"math"
	"math/bits"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/internal/countmin"
)

// testKeys are the only keys the differential tests use, so that the
// collision-free hash can map each of them to a fixed 64-bit value.
var testKeys = [...]string{"key-0", "key-1", "key-2", "key-3"}

// splitmix64 turns a small id into a well-mixed 64-bit hash, the same mixer
// internal/countmin's tests use for their fixed hashes.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

// exactHashIDs are fed to splitmix64 to give each of testKeys its fixed hash.
// They were picked so that every sharded config exercises both the shards and
// what happens inside one: key-0 and key-1 share a shard at every shard count up
// to 2^20, while key-2 lies in the other half of the shards from key-0 at every
// count above 1. requireShardsSharedAndSpanned asserts it for the configs used.
var exactHashIDs = [len(testKeys)]uint64{1, 1_420_910, 4, 3}

// exactHash is the WithHashFunc of Mode A: a fixed mapping from testKeys to
// fixed hashes, which requireCollisionFree proves collision-free for the
// sketch dimensions in use. Any other key is a bug in the test.
func exactHash(key string) uint64 {
	for i, k := range testKeys {
		if k == key {
			return splitmix64(exactHashIDs[i])
		}
	}
	panic("exactHash: key " + key + " is not one of testKeys")
}

func exactHashes() []uint64 {
	hashes := make([]uint64, len(testKeys))
	for i, key := range testKeys {
		hashes[i] = exactHash(key)
	}
	return hashes
}

func mustLayout(t *testing.T, cfg Config) windowLayout {
	t.Helper()
	layout, err := cfg.layout()
	if err != nil {
		t.Fatalf("layout of %+v: %v", cfg, err)
	}
	return layout
}

func shardBitsOf(layout windowLayout) uint {
	return uint(bits.TrailingZeros(uint(layout.shards)))
}

// requireCollisionFree proves the precondition of Mode A instead of assuming
// it: with every hash added once to a classic sketch of its shard's dimensions,
// each must be alone in at least one row among the hashes of its shard, which
// is exactly when its classic estimate is 1. The classic update is what makes
// that equivalence hold: under the conservative update of the ring a hash that
// shares every row can still read 1 after one add each. A hash alone in one row
// is the only one that ever raises that row's cell, and the conservative update
// raises it on each of the hash's adds: in each slot the cell holds the hash's
// count there, and the smallest of the hash's cells is never below that count.
// So every estimate in the window is the exact count, whatever the counts of
// the other keys.
func requireCollisionFree(t *testing.T, cfg Config, hashes []uint64) {
	t.Helper()
	layout := mustLayout(t, cfg)
	sketches := make(map[uint64]*countmin.Sketch)
	sketchOf := func(h uint64) *countmin.Sketch {
		shard := countmin.ShardOf(h, shardBitsOf(layout))
		if sketches[shard] == nil {
			s, err := countmin.NewSized(layout.shardWidth, layout.depth)
			if err != nil {
				t.Fatalf("NewSized(%d, %d): %v", layout.shardWidth, layout.depth, err)
			}
			sketches[shard] = s
		}
		return sketches[shard]
	}
	for _, h := range hashes {
		sketchOf(h).AddHash(h)
	}
	for i, h := range hashes {
		if got := sketchOf(h).CountHash(h); got != 1 {
			t.Fatalf("hash %d (%#x) estimates %d after one add each at %dx%d per shard, want 1: the hashes collide",
				i, h, got, layout.shardWidth, layout.depth)
		}
	}
}

// keysShareAndSpanShards reports whether, among more than one shard, the keys
// lie in at least two shards and at least two of them in the same one, so a
// test reaches both a lagging shard and keys interacting within one.
func keysShareAndSpanShards(c *Cache, keys []string) bool {
	if len(c.shards) == 1 {
		return true
	}
	seen := make(map[*shard]bool)
	for _, key := range keys {
		seen[c.shardOf(key)] = true
	}
	return len(seen) >= 2 && len(seen) < len(keys)
}

func requireShardsSharedAndSpanned(t *testing.T, c *Cache) {
	t.Helper()
	if !keysShareAndSpanShards(c, testKeys[:]) {
		t.Fatalf("over %d shards testKeys do not both span two shards and share one: pick other exactHashIDs",
			len(c.shards))
	}
}

// reseedUntilShardsSharedAndSpanned redraws the default hash's seed of a fresh
// cache until testKeys share and span its shards and, with more than one row,
// two keys of a shard collide partly. The seed is random, so this is how Mode
// B, which keeps the default hash, gets the same coverage as Mode A, and how
// it reaches the case the conservative update handles differently from the
// classic one: a key whose rows are not all shared.
func reseedUntilShardsSharedAndSpanned(t *testing.T, c *Cache, layout windowLayout) {
	t.Helper()
	const attempts = 1_000
	for range attempts {
		if keysShareAndSpanShards(c, testKeys[:]) && (layout.depth == 1 || keysCollidePartly(t, c, layout)) {
			return
		}
		c.seed = maphash.MakeSeed()
	}
	requireShardsSharedAndSpanned(t, c)
	t.Fatalf("no seed in %d made two of testKeys share some but not all of %d rows", attempts, layout.depth)
}

// keysCollidePartly reports whether two of testKeys in the same shard share
// the cell of at least one row but not of every row.
func keysCollidePartly(t *testing.T, c *Cache, layout windowLayout) bool {
	t.Helper()
	cells := make([][]int, len(testKeys))
	for i, key := range testKeys {
		cells[i] = cellsOf(t, layout.shardWidth, layout.depth, c.hashOf(key))
	}
	for i := range testKeys {
		for j := i + 1; j < len(testKeys); j++ {
			if c.shardOf(testKeys[i]) != c.shardOf(testKeys[j]) {
				continue
			}
			shared := 0
			for r := range layout.depth {
				if cells[i][r] == cells[j][r] {
					shared++
				}
			}
			if shared > 0 && shared < layout.depth {
				return true
			}
		}
	}
	return false
}

func newTestCache(t *testing.T, cfg Config, opts ...Option) *Cache {
	t.Helper()
	c, err := New(cfg, opts...)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// oracleConfig is the root configuration whose window, guard and range match
// cfg: TimestampBits 39..55 are CountBits 24..8. The sweep interval is long so
// the janitor never runs during a test; it changes no observable result anyway.
func oracleConfig(t *testing.T, cfg Config) slidingcache.Config {
	t.Helper()
	timestampBits := cfg.timestampBits()
	if timestampBits > 55 {
		t.Fatalf("TimestampBits %d has no root equivalent; compare against the model instead", timestampBits)
	}
	return slidingcache.Config{
		Precision:     cfg.Precision,
		WindowSize:    cfg.WindowSize,
		EpochUnit:     cfg.EpochUnit,
		MaxFutureSkew: cfg.MaxFutureSkew,
		Clock:         cfg.Clock,
		CountBits:     63 - timestampBits,
		SweepInterval: time.Hour,
	}
}

func newOracle(t *testing.T, cfg slidingcache.Config) *slidingcache.Cache {
	t.Helper()
	oracle, err := slidingcache.New(cfg)
	if err != nil {
		t.Fatalf("slidingcache.New(%+v): %v", cfg, err)
	}
	t.Cleanup(func() { _ = oracle.Close() })
	return oracle
}

// secondsConfig is the window most tests use: 10-second buckets, a 60-second
// window, epochs in seconds and a small sketch, which keeps a fuzz execution
// cheap while 4 fixed hashes stay collision-free.
func secondsConfig(timestampBits int) Config {
	return Config{
		Precision:     10 * time.Second,
		WindowSize:    60 * time.Second,
		EpochUnit:     slidingcache.EpochInSeconds,
		Epsilon:       0.01,
		TimestampBits: timestampBits,
	}
}

// fakeClock is a deterministic Config.Clock that counts how often the cache
// consulted it.
type fakeClock struct {
	now   int64
	reads int
}

func (f *fakeClock) read() int64 {
	f.reads++
	return f.now
}

// windowChecker checks I1, I2 and I3 on the window of every shard after every
// operation, and that the window an operation reached has applied its cutoff.
// It remembers the applied cutoff of each shard at the previous check for I3,
// and keeps the buffers I1 copies the ring's cells into; reusing them keeps the
// fuzz targets off the allocator.
type windowChecker struct {
	slotCells []uint32
	aggCells  []uint64
	liveSum   []uint64
	applied   []int64
}

func newWindowChecker() *windowChecker {
	return &windowChecker{}
}

// require checks every shard, each under its own lock.
func (wc *windowChecker) require(t *testing.T, c *Cache) {
	t.Helper()
	if wc.applied == nil {
		wc.applied = make([]int64, len(c.shards))
		for i := range wc.applied {
			wc.applied[i] = math.MinInt64
		}
	}
	for i, s := range c.shards {
		wc.requireShard(t, c, i, s)
	}
}

// requireReached is require after an operation that reached the lock of key's
// shard, single-threaded: that shard must have applied exactly the cutoff of
// the mark, which no other operation can have moved meanwhile.
func (wc *windowChecker) requireReached(t *testing.T, c *Cache, key string) {
	t.Helper()
	wc.require(t, c)
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if want := cutoffFor(c.highWater.Load(), c.windowSize); s.window.applied != want {
		t.Fatalf("the shard of %s applied %d after an operation reached it, want the cutoff %d",
			key, s.window.applied, want)
	}
}

func (wc *windowChecker) requireShard(t *testing.T, c *Cache, i int, s *shard) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	w := &s.window
	highWater := c.highWater.Load()
	if w.applied < wc.applied[i] {
		t.Fatalf("I3: shard %d applied went back from %d to %d", i, wc.applied[i], w.applied)
	}
	wc.applied[i] = w.applied
	if limit := cutoffFor(highWater, c.windowSize); w.applied > limit {
		t.Fatalf("shard %d applied %d is ahead of the cutoff %d of HW %d", i, w.applied, limit, highWater)
	}
	requireSlotsPlacedAndBounded(t, w, highWater, c.windowSize)
	wc.requireAggregateIsSumOfLiveSlots(t, w)
}

// requireSlotsPlacedAndBounded checks I2 slot by slot: a live slot sits at the
// index of its timestamp inside (applied, applied + WindowSize] and at or below
// HW. For a shard that has seen the latest mark the two bounds coincide.
func requireSlotsPlacedAndBounded(t *testing.T, w *window, highWater, windowSize int64) {
	t.Helper()
	if len(w.timestamps) != w.ring.Slots() {
		t.Fatalf("window tracks %d slots, its ring has %d", len(w.timestamps), w.ring.Slots())
	}
	upper := min(highWater, w.applied+windowSize)
	live := 0
	for i, timestamp := range w.timestamps {
		if timestamp == deadSlot {
			continue
		}
		live++
		if timestamp <= w.applied || timestamp > upper {
			t.Fatalf("I2: live slot %d has timestamp %d outside (%d, %d]", i, timestamp, w.applied, upper)
		}
		if w.index(timestamp) != i {
			t.Fatalf("I2: live slot %d holds timestamp %d, which belongs at %d", i, timestamp, w.index(timestamp))
		}
	}
	if live != w.live {
		t.Fatalf("the window counts %d live slots, %d timestamps are live", w.live, live)
	}
}

// requireAggregateIsSumOfLiveSlots checks I1 on the raw cells, independently
// of the ring's own bookkeeping: summing the cells of the live slots one by one
// must give exactly the aggregate's cells, and every cell of a dead slot must
// be zero.
func (wc *windowChecker) requireAggregateIsSumOfLiveSlots(t *testing.T, w *window) {
	t.Helper()
	wc.aggCells = w.ring.AppendAggregateCells(wc.aggCells[:0])
	wc.liveSum = append(wc.liveSum[:0], make([]uint64, len(wc.aggCells))...)
	for i, timestamp := range w.timestamps {
		wc.slotCells = w.ring.AppendSlotCells(wc.slotCells[:0], i)
		if len(wc.slotCells) != len(wc.aggCells) {
			t.Fatalf("slot %d has %d cells, the aggregate %d", i, len(wc.slotCells), len(wc.aggCells))
		}
		for j, cell := range wc.slotCells {
			if timestamp == deadSlot && cell != 0 {
				t.Fatalf("I1: dead slot %d holds %d in cell %d", i, cell, j)
			}
			wc.liveSum[j] += uint64(cell)
		}
	}
	for j, got := range wc.aggCells {
		if want := wc.liveSum[j]; got != want {
			t.Fatalf("I1: aggregate cell %d = %d, the live slots sum to %d", j, got, want)
		}
	}
}
