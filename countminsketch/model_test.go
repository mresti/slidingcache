package countminsketch

import (
	"math/big"
	"time"

	"github.com/mresti/slidingcache"
)

// referenceModel is a deliberately naive implementation of the exact window
// semantics: one unsorted slice of timestamps per key and a linear scan, like
// the root package's windowModel, with configurable range bounds. It maps
// epochs onto buckets with math/big, so it shares neither code nor int64
// wraparound with the cache: an epoch whose bucket overflows int64 there is
// simply out of range here.
//
// It serves where the root cache cannot: TimestampBits 56..62 and the disabled
// guard, and the upper bound of Mode B. It has no future guard.
type referenceModel struct {
	precision    int64
	windowSize   int64
	bucketDiv    *big.Int
	minTimestamp *big.Int
	maxTimestamp *big.Int
	highWater    int64
	observed     bool
	events       map[string][]int64
	stats        slidingcache.Stats
}

func newReferenceModel(cfg Config) *referenceModel {
	bits := uint(cfg.timestampBits())
	precision := int64(cfg.Precision / time.Second)
	unitsPerSecond := map[slidingcache.EpochUnit]int64{
		slidingcache.EpochInSeconds: 1,
		slidingcache.EpochInMillis:  1_000,
		slidingcache.EpochInNanos:   1_000_000_000,
	}[cfg.EpochUnit]
	one := big.NewInt(1)
	return &referenceModel{
		precision:    precision,
		windowSize:   int64(cfg.WindowSize / time.Second),
		bucketDiv:    new(big.Int).Mul(big.NewInt(precision), big.NewInt(unitsPerSecond)),
		minTimestamp: new(big.Int).Neg(new(big.Int).Lsh(one, bits)),
		maxTimestamp: new(big.Int).Sub(new(big.Int).Lsh(one, bits), one),
		events:       make(map[string][]int64),
	}
}

// bucket returns the bucket timestamp of epoch and whether it is in range. Big
// Div is Euclidean, which for a positive divisor rounds toward negative
// infinity.
func (m *referenceModel) bucket(epoch int64) (int64, bool) {
	ts := new(big.Int).Div(big.NewInt(epoch), m.bucketDiv)
	ts.Mul(ts, big.NewInt(m.precision))
	if ts.Cmp(m.minTimestamp) < 0 || ts.Cmp(m.maxTimestamp) > 0 {
		return 0, false
	}
	return ts.Int64(), true
}

func (m *referenceModel) alive(timestamp int64) bool {
	return !m.observed || timestamp > m.highWater-m.windowSize
}

func (m *referenceModel) Store(epoch int64, key string) int {
	timestamp, ok := m.bucket(epoch)
	if !ok {
		m.stats.OutOfRange++
		return slidingcache.LateEvent
	}
	if !m.observed || timestamp > m.highWater {
		m.highWater, m.observed = timestamp, true
	}
	if !m.alive(timestamp) {
		m.stats.Late++
		return slidingcache.LateEvent
	}
	m.stats.Accepted++
	m.events[key] = append(m.events[key], timestamp)
	return m.count(key)
}

func (m *referenceModel) Get(epoch int64, key string) int {
	timestamp, ok := m.bucket(epoch)
	if !ok || !m.alive(timestamp) {
		m.stats.GetLate++
		return slidingcache.LateEvent
	}
	m.stats.GetHit++
	return m.count(key)
}

func (m *referenceModel) HighWater() (hw int64, ok bool) {
	if !m.observed {
		return noObservedHighWater(m.windowSize), false
	}
	return m.highWater, true
}

func (m *referenceModel) Stats() slidingcache.Stats { return m.stats }

func (m *referenceModel) count(key string) int {
	live := 0
	for _, timestamp := range m.events[key] {
		if m.alive(timestamp) {
			live++
		}
	}
	return live
}
