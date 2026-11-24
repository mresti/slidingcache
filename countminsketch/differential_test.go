package countminsketch

import (
	"math"
	"testing"
	"time"
)

// The fuzz targets below replay root's encodings, from FuzzStoreGetInvariants
// and FuzzFutureSkewInvariants, on a sketch cache and its reference at once.

const (
	fuzzKeyMask = len(testKeys) - 1
	fuzzReadBit = 1 << 2
	maxFuzzOps  = 256

	// oracleBits is the number of TimestampBits values with a root equivalent,
	// 39..55, onto which a fuzzed byte is folded.
	oracleBits = 55 - minTimestampBits + 1
)

// fuzzOperand derives a key and an epoch from one input byte as root does: the
// low bits pick the key and a sub-precision jitter, the high bits a signed
// bucket offset wide enough to cross the window in both directions.
func fuzzOperand(base int64, op byte, precision int64) (string, int64) {
	const bucketOffsetBias = 16
	key := testKeys[int(op)&fuzzKeyMask]
	epoch := base + (int64(op>>3)-bucketOffsetBias)*precision + int64(int(op)&fuzzKeyMask)
	return key, epoch
}

func foldOracleBits(b byte) int {
	return minTimestampBits + int(b)%oracleBits
}

// fuzzShardCounts are the Config.Shards a fuzzed byte selects among.
var fuzzShardCounts = [...]int{1, 2, 4}

func foldShards(b byte) int {
	return fuzzShardCounts[int(b)%len(fuzzShardCounts)]
}

// addForEveryShardCount adds a seed once per entry of fuzzShardCounts, so even
// the seed corpus that make ci runs covers every shard count.
func addForEveryShardCount(f *testing.F, seed func(shards byte) []any) {
	for i := range byte(len(fuzzShardCounts)) {
		f.Add(seed(i)...)
	}
}

type rangeSeed struct {
	base int64
	bits int
	ops  []byte
}

// rangeSeeds are root's FuzzStoreGetInvariants seeds, with CountBits c turned
// into TimestampBits 63-c, plus the low end of each width, so both ends of the
// representable range are reached at the default and at both extreme widths.
func rangeSeeds() []rangeSeed {
	maxTS := func(bits int) int64 { return 1<<uint(bits) - 1 }
	minTS := func(bits int) int64 { return -1 << uint(bits) }
	return []rangeSeed{
		{1_700_000_000, defaultTimestampBits, []byte{0, 1, 2, 200, 3}},
		{0, defaultTimestampBits, []byte{250, 251, 0, 5, 7}},
		{-1_000, defaultTimestampBits, []byte{0, 60, 120, 1, 255}},
		{1_700_000_000, defaultTimestampBits, []byte{128, 128, 128, 128, 128, 120, 120, 128, 132}},
		{1_700_000_000, defaultTimestampBits, []byte{128, 128, 136, 136, 144, 128, 120, 128}},
		{maxTS(defaultTimestampBits), defaultTimestampBits, []byte{248, 128, 0, 252, 8, 244}},
		{minTS(defaultTimestampBits) + 1, defaultTimestampBits, []byte{0, 8, 128, 248, 4, 252}},
		{1_700_000_000, 55, []byte{128, 128, 128, 120, 128, 136, 128}},
		{maxTS(55), 55, []byte{248, 128, 0, 252, 8, 244}},
		{minTS(55) + 1, 55, []byte{0, 8, 128, 248, 4, 252}},
		{1_700_000_000, minTimestampBits, []byte{128, 128, 120, 136, 128, 144, 128}},
		{maxTS(minTimestampBits), minTimestampBits, []byte{248, 128, 0, 252, 8, 244}},
		{minTS(minTimestampBits) + 1, minTimestampBits, []byte{0, 8, 128, 248, 4, 252}},
	}
}

// replayRangeOps drives a harness with root's encoding. The base is folded into
// the representable range, so offsets cannot overflow, while a base at either
// end still produces epochs both sides must reject.
func replayRangeOps(h *harness, bits int, base int64, ops []byte) {
	h.t.Helper()
	base %= int64(1) << uint(bits)
	if len(ops) > maxFuzzOps {
		ops = ops[:maxFuzzOps]
	}
	precision := h.sketch.precision
	for _, op := range ops {
		key, epoch := fuzzOperand(base, op, precision)
		if op&fuzzReadBit == 0 {
			h.store(epoch, key)
		} else {
			h.get(epoch, key)
		}
	}
	h.requireFinalCounts()
}

// FuzzExactAgreesWithOracle is Mode A: with a collision-free hash every return,
// the high-water mark and the counters must equal root's, at every
// TimestampBits root has an equivalent for and with 1, 2 or 4 shards.
func FuzzExactAgreesWithOracle(f *testing.F) {
	for _, s := range rangeSeeds() {
		addForEveryShardCount(f, func(shards byte) []any {
			return []any{s.base, byte(s.bits - minTimestampBits), shards, s.ops}
		})
	}
	f.Fuzz(func(t *testing.T, base int64, bits, shards byte, ops []byte) {
		cfg := shardedSecondsConfig(foldOracleBits(bits), foldShards(shards))
		h := newExactHarness(t, cfg, newOracle(t, oracleConfig(t, cfg)))
		replayRangeOps(h, cfg.timestampBits(), base, ops)
	})
}

// fuzzDepths are the sketch depths a fuzzed byte selects among in Mode B, and
// fuzzDepthDeltas the Delta giving each, ceil(ln(1/Delta)) rows. One row makes
// the conservative update the classic one; two and three rows exercise it.
var (
	fuzzDepths      = [...]int{1, 2, 3}
	fuzzDepthDeltas = [...]float64{0.9, 0.2, 0.06}
)

func foldDepth(b byte) int {
	return int(b) % len(fuzzDepths)
}

// addForEveryShardCountAndDepth adds a seed once per shard count and depth, so
// the seed corpus covers every combination.
func addForEveryShardCountAndDepth(f *testing.F, seed func(shards, depth byte) []any) {
	for d := range byte(len(fuzzDepths)) {
		addForEveryShardCount(f, func(shards byte) []any { return seed(shards, d) })
	}
}

// FuzzCollisionsBoundedByOracle is Mode B: the narrowest sketch New accepts,
// at 1, 2 or 3 rows, with the default hash, so collisions are frequent: the
// rule with one shard of 4 columns, common with 8 columns per shard.
// Rejections must still be identical and every count between root's and the
// classic estimate of the live events.
func FuzzCollisionsBoundedByOracle(f *testing.F) {
	for _, s := range rangeSeeds() {
		addForEveryShardCountAndDepth(f, func(shards, depth byte) []any {
			return []any{s.base, byte(s.bits - minTimestampBits), shards, depth, s.ops}
		})
	}
	slide := slidingStores()
	addForEveryShardCountAndDepth(f, func(shards, depth byte) []any {
		return []any{int64(1_700_000_000), byte(defaultTimestampBits - minTimestampBits), shards, depth, slide}
	})
	f.Fuzz(func(t *testing.T, base int64, bits, shards, depth byte, ops []byte) {
		cfg := narrowestConfig(t, foldOracleBits(bits), foldShards(shards), foldDepth(depth))
		replayRangeOps(newBoundedHarness(t, cfg), cfg.timestampBits(), base, ops)
	})
}

// slidingStores is a seed in root's encoding that slides the mark over all 32
// bucket offsets the encoding reaches, more than five times the 6 buckets of
// the window. Each bucket stores every key 1 to 3 times, a multiplicity that
// rotates with the bucket. A key counted while its counters are high from
// older buckets, which then expire, is what a minimum read from the aggregate
// instead of the slot undercounts. Which keys collide changes from run to run
// with the default hash's random seed; over so many buckets some partial
// collision is near certain at 2 and 3 rows.
func slidingStores() []byte {
	const buckets = 32
	multiplicities := [len(testKeys)]int{3, 1, 2, 1}
	var ops []byte
	for bucket := range buckets {
		offset := byte(bucket) << 3
		for k := range testKeys {
			for range multiplicities[(k+bucket)%len(testKeys)] {
				ops = append(ops, offset|byte(k))
			}
		}
	}
	return ops
}

// narrowestConfig is a sketch of fuzzDepths[depthIndex] rows and as few
// columns as New allows: 4 with one shard, minShardWidth per shard with more.
func narrowestConfig(t *testing.T, timestampBits, shards, depthIndex int) Config {
	t.Helper()
	cfg := shardedSecondsConfig(timestampBits, shards)
	cfg.Epsilon, cfg.Delta = 0.9, fuzzDepthDeltas[depthIndex]
	wantWidth, wantDepth := 4, fuzzDepths[depthIndex]
	if shards > 1 {
		// Just under e/Epsilon = minShardWidth*shards, so the ceiling lands on it.
		cfg.Epsilon = math.E / (float64(minShardWidth*shards) - 0.5)
		wantWidth = minShardWidth
	}
	if layout := mustLayout(t, cfg); layout.shardWidth != wantWidth || layout.depth != wantDepth {
		t.Fatalf("narrowest config with %d shards is %dx%d per shard, want %dx%d",
			shards, layout.shardWidth, layout.depth, wantWidth, wantDepth)
	}
	return cfg
}

// wideBits are the ranges root cannot express, 56..62, plus the disabled guard,
// which a fuzzed byte selects as the value one past 62.
const wideBits = maxTimestampBits - 56 + 2

func wideConfig(b byte) Config {
	return wideShardedConfig(b, 0)
}

func wideShardedConfig(b byte, shards int) Config {
	bits := 56 + int(b)%wideBits
	if bits > maxTimestampBits {
		cfg := shardedSecondsConfig(0, shards)
		cfg.DisableRangeGuard = true
		return cfg
	}
	return shardedSecondsConfig(bits, shards)
}

// FuzzWideRangeAgreesWithModel is Mode A beyond root's ranges, against the
// naive model, with the same encoding and seeds at both ends of each width.
func FuzzWideRangeAgreesWithModel(f *testing.F) {
	ops := [][]byte{{248, 128, 0, 252, 8, 244}, {0, 8, 128, 248, 4, 252}, {128, 128, 120, 136, 128, 144}}
	for b := range byte(wideBits) {
		bits := wideConfig(b).timestampBits()
		addForEveryShardCount(f, func(shards byte) []any { return []any{int64(1)<<uint(bits) - 1, b, shards, ops[0]} })
		addForEveryShardCount(f, func(shards byte) []any { return []any{-int64(1)<<uint(bits) + 1, b, shards, ops[1]} })
		addForEveryShardCount(f, func(shards byte) []any { return []any{int64(1_700_000_000), b, shards, ops[2]} })
	}
	f.Fuzz(func(t *testing.T, base int64, bits, shards byte, ops []byte) {
		cfg := wideShardedConfig(bits, foldShards(shards))
		h := newExactHarness(t, cfg, newReferenceModel(cfg))
		replayRangeOps(h, cfg.timestampBits(), base, ops)
	})
}

const (
	// fuzzClockBit makes an op move the clocks forward instead of touching the
	// caches; fuzzGetBit makes it a read. The remaining bits carry the offset.
	fuzzClockBit = 1 << 7
	fuzzGetBit   = 1 << 6
	fuzzOffsets  = 1 << 6
)

// FuzzFutureSkewAgreesWithOracle replays root's future-guard encoding on both
// caches in Mode A. Each cache has its own fake clock, moved in lockstep, so
// the targets also pin that both consult the clock on exactly the same calls.
func FuzzFutureSkewAgreesWithOracle(f *testing.F) {
	seeds := []struct {
		base int64
		ops  []byte
	}{
		{1_700_000_000, []byte{32, 40, 63, 0, 96}},
		{1_700_000_000, []byte{63, 63, 128 | 16, 63, 96 | 63}},
		{0, []byte{32, 128 | 1, 33, 128 | 63, 34, 96}},
		{-1_000_000, []byte{0, 1, 62, 63, 128 | 4, 63, 96}},
		{1_700_000_000, []byte{36, 36, 63, 63, 36, 96 | 36, 128 | 8, 36, 63}},
		// Offset 35 is +30s, exactly the skew: accepted, while 36 is one bucket
		// past it. Then the same pair for Get, before and after moving the clock
		// a bucket so the boundary lies above the mark.
		{1_700_000_000, []byte{35, 36, 64 | 35, 64 | 36, 128 | 1, 64 | 35, 64 | 36}},
		// Two Stores in the bucket that is the mark: the second is not beyond it,
		// so neither cache may read the clock for it. The encoding only moves
		// the clock forward, so the clock read is what tells them apart.
		{1_700_000_000, []byte{35, 35, 32, 35}},
	}
	for _, seed := range seeds {
		addForEveryShardCount(f, func(shards byte) []any { return []any{seed.base, shards, seed.ops} })
	}
	f.Fuzz(func(t *testing.T, base int64, shards byte, ops []byte) {
		requireFutureGuardAgreement(t, base, foldShards(shards), ops)
	})
}

func requireFutureGuardAgreement(t *testing.T, base int64, shards int, ops []byte) {
	t.Helper()
	const (
		skewSeconds = 30
		baseRange   = int64(1) << 40
	)
	base %= baseRange
	if len(ops) > maxFuzzOps {
		ops = ops[:maxFuzzOps]
	}

	sketchClock, oracleClock := &fakeClock{now: base}, &fakeClock{now: base}
	cfg := shardedSecondsConfig(0, shards)
	cfg.MaxFutureSkew = skewSeconds * time.Second
	cfg.Clock = oracleClock.read
	oracle := newOracle(t, oracleConfig(t, cfg))
	cfg.Clock = sketchClock.read
	h := newExactHarness(t, cfg, oracle)
	precision := h.sketch.precision

	for _, op := range ops {
		if op&fuzzClockBit != 0 {
			step := int64(op%fuzzOffsets) * precision
			sketchClock.now += step
			oracleClock.now += step
			continue
		}
		key := testKeys[int(op)&fuzzKeyMask]
		epoch := sketchClock.now + (int64(op%fuzzOffsets)-fuzzOffsets/2)*precision
		if op&fuzzGetBit != 0 {
			h.get(epoch, key)
		} else {
			h.store(epoch, key)
		}
		if sketchClock.reads != oracleClock.reads {
			t.Fatalf("after op %d at epoch %d the sketch read the clock %d times, the oracle %d",
				op, epoch, sketchClock.reads, oracleClock.reads)
		}
	}
}
