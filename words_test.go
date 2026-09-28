package slidingcache

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// TestNewPicksFourByteWordsWhenTheWindowFits pins the width New gives a Cache's
// buckets: 4-byte words when WindowSize spans at most the 2^(32-CountBits)
// seconds the low bits of a timestamp tell apart, 8-byte words otherwise, on
// both sides of the limit. The limit counts seconds whatever the Precision, and
// the layout, which decides the accepted range, is the same at both widths.
func TestNewPicksFourByteWordsWhenTheWindowFits(t *testing.T) {
	cases := []struct {
		name       string
		countBits  int
		precision  time.Duration
		window     time.Duration
		wantNarrow bool
	}{
		{"default CountBits, 30-minute window", 0, time.Second, 1800 * time.Second, true},
		{"default CountBits, one-hour window", 0, time.Second, time.Hour, true},
		{"default CountBits, longest 4-byte window", 0, time.Second, 4096 * time.Second, true},
		{"default CountBits, one second past it", 0, time.Second, 4097 * time.Second, false},
		{"default CountBits, one-day window", 0, time.Second, 24 * time.Hour, false},
		{"coarse Precision, window within the limit in seconds", 0, 10 * time.Second, 4090 * time.Second, true},
		{"coarse Precision, few buckets but a window past it", 0, time.Minute, 24 * time.Hour, false},
		{"narrowest CountBits, longest 4-byte window", minCountBits, time.Second, 1 << 24 * time.Second, true},
		{"narrowest CountBits, one second past it", minCountBits, time.Second, (1<<24 + 1) * time.Second, false},
		{"widest CountBits, longest 4-byte window", maxCountBits, time.Second, 256 * time.Second, true},
		{"widest CountBits, one second past it", maxCountBits, time.Second, 257 * time.Second, false},
		{"widest CountBits, 30-minute window", maxCountBits, time.Second, 1800 * time.Second, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Precision: tc.precision, WindowSize: tc.window, CountBits: tc.countBits}
			c := newTestCache(t, cfg)

			if got := c.narrowShards != nil; got != tc.wantNarrow || (c.wideShards != nil) == tc.wantNarrow {
				t.Fatalf("%d shards of 4-byte words and %d of 8-byte ones, want 4-byte words: %t",
					len(c.narrowShards), len(c.wideShards), tc.wantNarrow)
			}
			if want := newBucketLayout(cfg.countBits()); c.layout != want {
				t.Fatalf("layout = %+v, want %+v whatever the width", c.layout, want)
			}
		})
	}
}

// TestNarrowAndWideWordsAgreeOnEveryCall replays one random operation sequence,
// the same for every cache, on a Cache New gave 4-byte words and on the same
// configuration forced onto 8-byte words, next to the reference model. Every
// Store and Get must return the same on the three, and the two caches must keep
// the same Stats and retain the same events in the same number of words for
// every key, checked every stateCheckEvery steps: the width changes where a
// bucket is stored, never what the cache does.
//
// The sequence writes keys in order, in bursts that spill a word, out of order
// and late; lets them idle; jumps the window forward by up to three windows;
// sweeps; and runs long enough for the high-water mark to cross the span of a
// 4-byte word several times, so the low bits the words keep wrap under keys
// that are written throughout, under keys that come back from idle and in the
// middle of their windows, at the widest window that fits. Configurations past
// the 4-byte limit run on the one Cache New builds, against the model.
func TestNarrowAndWideWordsAgreeOnEveryCall(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		ops  int
	}{
		{"default CountBits, 5-minute window", Config{Precision: time.Second, WindowSize: 300 * time.Second}, 30_000},
		{"narrowest CountBits, spilling", Config{
			Precision: time.Second, WindowSize: 300 * time.Second, CountBits: minCountBits,
		}, 10_000},
		{"window at the 4-byte limit", Config{Precision: time.Second, WindowSize: 4096 * time.Second}, 30_000},
		{"Precision above a second", Config{Precision: 7 * time.Second, WindowSize: 600 * time.Second}, 30_000},
		{"widest CountBits at its 4-byte limit", Config{
			Precision: time.Second, WindowSize: 256 * time.Second, CountBits: maxCountBits,
		}, 30_000},
		{"past the 4-byte limit: default CountBits", Config{
			Precision: time.Second, WindowSize: 4097 * time.Second,
		}, 10_000},
		{"past the 4-byte limit: widest CountBits", Config{
			Precision: time.Second, WindowSize: 1800 * time.Second, CountBits: maxCountBits,
		}, 10_000},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.EpochUnit = EpochInSeconds
			caches := []*Cache{newTestCache(t, cfg)}
			if caches[0].narrowShards != nil {
				caches = append(caches, newTestCacheWithWords(t, cfg, wideWordBits))
			}
			replay := newWordsReplay(t, cfg, caches, rand.New(rand.NewPCG(uint64(i), 42)))
			for op := range tc.ops {
				replay.step()
				if op%stateCheckEvery == 0 {
					replay.requireSameState()
				}
			}
			replay.requireSameState()
			replay.requireFinalCountsAgree()
		})
	}
}

// stateCheckEvery is how many steps apart the replay compares the caches'
// Stats and retained entries, besides every return value. The counters only
// ever grow, so a difference, once there, stays for the next check to see.
const stateCheckEvery = 64

// wordsReplay drives the operation sequence of
// TestNarrowAndWideWordsAgreeOnEveryCall.
type wordsReplay struct {
	t      *testing.T
	rng    *rand.Rand
	caches []*Cache
	model  *windowModel
	keys   []string
	now    int64
	window int64
	burst  int
}

func newWordsReplay(t *testing.T, cfg Config, caches []*Cache, rng *rand.Rand) *wordsReplay {
	precision, window := int64(cfg.Precision/time.Second), int64(cfg.WindowSize/time.Second)
	return &wordsReplay{
		t:      t,
		rng:    rng,
		caches: caches,
		model:  newWindowModel(precision, window, caches[0].layout),
		keys:   []string{"hot-a", "hot-b", "warm", "cold-a", "cold-b", "rare"},
		now:    1_700_000_000,
		window: window,
		burst:  burstFor(caches[0].layout),
	}
}

// burstFor is the most events a burst stores into one bucket of one key: enough
// to spill a word into two more where a word holds a few hundred events, and a
// few otherwise, since a word holding a million would take the replay millions
// of stores to fill.
func burstFor(l bucketLayout) int {
	if l.maxCount < 1<<10 {
		return 2*l.maxCount + 1
	}
	return 16
}

func (r *wordsReplay) step() {
	// The smaller of two draws favors the first keys, the hot ones.
	key := r.keys[min(r.rng.IntN(len(r.keys)), r.rng.IntN(len(r.keys)))]
	switch n := r.rng.IntN(100); {
	case n < 40:
		r.store(r.now-r.rng.Int64N(3), key)
	case n < 45:
		for range 1 + r.rng.IntN(r.burst) {
			r.store(r.now, key)
		}
	case n < 58:
		r.store(r.now-r.rng.Int64N(r.window+r.window/4), key)
	case n < 70:
		r.now += 1 + r.rng.Int64N(4)
	case n < 71:
		r.now += r.rng.Int64N(3 * r.window)
	case n < 97:
		r.get(r.now-r.rng.Int64N(r.window+r.window/4), key)
	default:
		for _, c := range r.caches {
			c.sweep()
		}
	}
}

func (r *wordsReplay) store(epoch int64, key string) {
	r.t.Helper()
	want := r.model.store(epoch, key)
	for i, c := range r.caches {
		if got := c.Store(epoch, key); got != want {
			r.t.Fatalf("cache %d (%s): Store(%d, %s) = %d, want %d", i, wordsOf(c), epoch, key, got, want)
		}
	}
}

func (r *wordsReplay) get(epoch int64, key string) {
	r.t.Helper()
	want := r.model.get(epoch, key)
	for i, c := range r.caches {
		if got := c.Get(epoch, key); got != want {
			r.t.Fatalf("cache %d (%s): Get(%d, %s) = %d, want %d", i, wordsOf(c), epoch, key, got, want)
		}
	}
}

// requireSameState compares the caches with the first one: their Stats, and
// the events and words each key physically retains.
func (r *wordsReplay) requireSameState() {
	r.t.Helper()
	first := r.caches[0]
	for _, c := range r.caches[1:] {
		if got, want := c.Stats(), first.Stats(); got != want {
			r.t.Fatalf("%s: Stats = %+v, want %+v as on %s", wordsOf(c), got, want, wordsOf(first))
		}
		for _, key := range r.keys {
			got, gotOK := c.shapeOf(key)
			want, wantOK := first.shapeOf(key)
			if gotOK != wantOK || got.events != want.events || got.words != want.words {
				r.t.Fatalf("%s: key %s retains %d events in %d words (present %t), want %d in %d (present %t) as on %s",
					wordsOf(c), key, got.events, got.words, gotOK, want.events, want.words, wantOK, wordsOf(first))
			}
		}
	}
}

func (r *wordsReplay) requireFinalCountsAgree() {
	r.t.Helper()
	for _, key := range r.keys {
		r.get(r.model.highWater, key)
	}
}

func wordsOf(c *Cache) string {
	if c.narrowShards != nil {
		return "4-byte words"
	}
	return "8-byte words"
}

// TestStoreKeepsEveryLiveEventAcrossTheWrap pins the steps shard.store runs on
// an entry, prune, rebase, makeRoom when needed, and the record, at the edge of
// what a word reaches: on the entries of every shape the search tests use,
// whose timestamps put the wrap of a word's low bits anywhere, under a cutoff
// anywhere from just below the base to just below the newest bucket, an event
// anywhere from one past the cutoff to a word's whole reach after it must leave
// exactly the events alive under the cutoff plus the new one, sorted, with an
// exact total, and the base one past the cutoff.
func TestStoreKeepsEveryLiveEventAcrossTheWrap(t *testing.T) {
	t.Run("4-byte words", testStoreKeepsEveryLiveEventAcrossTheWrap[uint32])
	t.Run("8-byte words", testStoreKeepsEveryLiveEventAcrossTheWrap[uint64])
}

func testStoreKeepsEveryLiveEventAcrossTheWrap[W word](t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	layouts := []bucketLayout{newBucketLayout(minCountBits), testLayout, newBucketLayout(maxCountBits)}
	for _, layout := range layouts {
		reach := reachOf[W](layout)
		windowBuckets := int(min(reach, 1<<12))
		for trial := range 300 {
			e := randomSearchEntry[W](rng, layout, trial)
			if len(e.retained()) == 0 {
				continue
			}
			newest := e.timestamp(layout, e.buckets[len(e.buckets)-1])
			cutoff := e.base - 1 + rng.Int64N(newest-e.base+1)
			timestamp := cutoff + 1 + rng.Int64N(reach+1)
			if trial%3 == 0 {
				timestamp = cutoff + 1 + reach - rng.Int64N(2)
			}
			if !layout.inRange(timestamp) {
				continue
			}
			describe := fmt.Sprintf("countBits %d, trial %d: event %d under cutoff %d on base %d",
				layout.countBits, trial, timestamp, cutoff, e.base)

			want := []int64{timestamp}
			for _, event := range e.expanded(layout) {
				if event > cutoff {
					want = append(want, event)
				}
			}
			slices.Sort(want)

			e.prune(layout, cutoff)
			e.rebase(cutoff)
			if e.needsRoom(layout, timestamp) {
				e.makeRoom(windowBuckets)
			}
			e.insert(layout, timestamp)

			if got := e.expanded(layout); !slices.Equal(got, want) {
				t.Fatalf("%s: events = %v, want %v", describe, got, want)
			}
			requireEntryInvariants(t, layout, e)
			if e.base != cutoff+1 {
				t.Fatalf("%s: base = %d, want %d", describe, e.base, cutoff+1)
			}
		}
	}
}
