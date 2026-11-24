package countminsketch

import (
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
// TimestampBits root has an equivalent for.
func FuzzExactAgreesWithOracle(f *testing.F) {
	for _, s := range rangeSeeds() {
		f.Add(s.base, byte(s.bits-minTimestampBits), s.ops)
	}
	f.Fuzz(func(t *testing.T, base int64, bits byte, ops []byte) {
		cfg := secondsConfig(foldOracleBits(bits))
		h := newExactHarness(t, cfg, newOracle(t, oracleConfig(t, cfg)))
		replayRangeOps(h, cfg.timestampBits(), base, ops)
	})
}

// FuzzCollisionsBoundedByOracle is Mode B: a sketch of 4 columns by 1 row with
// the default hash, where collisions are the rule. Rejections must still be
// identical and every count between root's and the window's total.
func FuzzCollisionsBoundedByOracle(f *testing.F) {
	for _, s := range rangeSeeds() {
		f.Add(s.base, byte(s.bits-minTimestampBits), s.ops)
	}
	f.Fuzz(func(t *testing.T, base int64, bits byte, ops []byte) {
		cfg := secondsConfig(foldOracleBits(bits))
		cfg.Epsilon, cfg.Delta = 0.9, 0.9
		replayRangeOps(newBoundedHarness(t, cfg), cfg.timestampBits(), base, ops)
	})
}

// wideBits are the ranges root cannot express, 56..62, plus the disabled guard,
// which a fuzzed byte selects as the value one past 62.
const wideBits = maxTimestampBits - 56 + 2

func wideConfig(b byte) Config {
	bits := 56 + int(b)%wideBits
	if bits > maxTimestampBits {
		cfg := secondsConfig(0)
		cfg.DisableRangeGuard = true
		return cfg
	}
	return secondsConfig(bits)
}

// FuzzWideRangeAgreesWithModel is Mode A beyond root's ranges, against the
// naive model, with the same encoding and seeds at both ends of each width.
func FuzzWideRangeAgreesWithModel(f *testing.F) {
	ops := [][]byte{{248, 128, 0, 252, 8, 244}, {0, 8, 128, 248, 4, 252}, {128, 128, 120, 136, 128, 144}}
	for b := range byte(wideBits) {
		bits := wideConfig(b).timestampBits()
		f.Add(int64(1)<<uint(bits)-1, b, ops[0])
		f.Add(-int64(1)<<uint(bits)+1, b, ops[1])
		f.Add(int64(1_700_000_000), b, ops[2])
	}
	f.Fuzz(func(t *testing.T, base int64, bits byte, ops []byte) {
		cfg := wideConfig(bits)
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
	}
	for _, seed := range seeds {
		f.Add(seed.base, seed.ops)
	}
	f.Fuzz(func(t *testing.T, base int64, ops []byte) {
		requireFutureGuardAgreement(t, base, ops)
	})
}

func requireFutureGuardAgreement(t *testing.T, base int64, ops []byte) {
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
	cfg := secondsConfig(0)
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
