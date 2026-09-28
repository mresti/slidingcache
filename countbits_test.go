package slidingcache

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestConfigCountBitsValidation pins the accepted values of Config.CountBits and
// the layout each of them selects, including the zero value that stands for the
// default width. A one-minute window fits 4-byte words at every width.
func TestConfigCountBitsValidation(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		cases := []struct {
			countBits int
			want      bucketLayout
		}{
			{0, newBucketLayout(defaultCountBits)},
			{minCountBits, newBucketLayout(minCountBits)},
			{defaultCountBits, newBucketLayout(defaultCountBits)},
			{maxCountBits, newBucketLayout(maxCountBits)},
		}
		for _, tc := range cases {
			t.Run(fmt.Sprint(tc.countBits), func(t *testing.T) {
				c, err := New(Config{Precision: time.Second, WindowSize: time.Minute, CountBits: tc.countBits})
				if err != nil {
					t.Fatalf("New with CountBits %d returned error: %v", tc.countBits, err)
				}
				defer func() { _ = c.Close() }()

				if c.layout != tc.want {
					t.Fatalf("layout = %+v, want %+v", c.layout, tc.want)
				}
				if len(c.narrowShards) != defaultShards || c.wideShards != nil {
					t.Fatalf("%d shards of 4-byte words and %d of 8-byte ones, want %d and none",
						len(c.narrowShards), len(c.wideShards), defaultShards)
				}
				for _, s := range c.narrowShards {
					if s.layout != tc.want {
						t.Fatalf("shard layout = %+v, want %+v", s.layout, tc.want)
					}
				}
			})
		}
	})

	t.Run("rejected", func(t *testing.T) {
		for _, countBits := range []int{-1, 1, minCountBits - 1, maxCountBits + 1, 32, 64} {
			t.Run(fmt.Sprint(countBits), func(t *testing.T) {
				c, err := New(Config{Precision: time.Second, WindowSize: time.Minute, CountBits: countBits})
				if err == nil {
					_ = c.Close()
					t.Fatalf("New with CountBits %d succeeded, want an error", countBits)
				}
				if !strings.Contains(err.Error(), "CountBits") {
					t.Fatalf("error %q does not mention CountBits", err)
				}
			})
		}
	})
}

// TestBucketLayoutBounds checks, for every documented width, that a layout's
// bounds are the documented ones and, at both word widths, that a word packs and
// unpacks the timestamps at both ends of its reach from bases across the whole
// range and on both sides of the wrap of its low bits, that the bucket checks
// tell its timestamp from every other one in reach, and that the words order by
// timestamp alone relative to the base, which is what lets the searches compare
// them without unpacking.
func TestBucketLayoutBounds(t *testing.T) {
	for _, bits := range []int{8, 12, 16, 20, 24} {
		t.Run(fmt.Sprint(bits), func(t *testing.T) {
			l := newBucketLayout(bits)

			if got, want := l.maxCount, 1<<bits-1; got != want {
				t.Fatalf("maxCount = %d, want %d", got, want)
			}
			if got, want := l.minTimestamp, int64(-1)<<(63-bits); got != want {
				t.Fatalf("minTimestamp = %d, want %d", got, want)
			}
			if got, want := l.maxTimestamp, int64(1)<<(63-bits)-1; got != want {
				t.Fatalf("maxTimestamp = %d, want %d", got, want)
			}
			requireRangeBoundaries(t, l)

			t.Run("4-byte words", func(t *testing.T) { requireWordsWithinReach[uint32](t, l) })
			t.Run("8-byte words", func(t *testing.T) { requireWordsWithinReach[uint64](t, l) })
		})
	}
}

func requireWordsWithinReach[W word](t *testing.T, l bucketLayout) {
	t.Helper()
	reach := reachOf[W](l)
	for _, base := range []int64{l.minTimestamp, -1, 0, 1_700_000_000, l.maxTimestamp - reach} {
		e := &entry[W]{base: base}
		timestamps := wordReach(e, reach)
		requireWordRoundTrip(t, l, e, timestamps)
		requireTimestampOrdersWords(t, l, e, timestamps)
		requireIncrementCountsOneEvent(t, l, e, timestamps)
	}
}

// wordReach is, in order, the timestamps at both ends of what a word of e
// reaches from its base, just inside them, and on both sides of the first
// multiple of that span after the base: where the low bits a 4-byte word keeps
// wrap, and where the sign bit of an 8-byte word flips.
func wordReach[W word](e *entry[W], reach int64) []int64 {
	wrap := e.base + (reach+1-e.base&reach)&reach
	timestamps := []int64{e.base, e.base + 1, e.base + reach - 1, e.base + reach}
	if wrap > e.base+1 && wrap < e.base+reach-1 {
		timestamps = append(timestamps, wrap-1, wrap)
		slices.Sort(timestamps)
	}
	return timestamps
}

func requireWordRoundTrip[W word](t *testing.T, l bucketLayout, e *entry[W], timestamps []int64) {
	t.Helper()

	for _, timestamp := range timestamps {
		for _, count := range []int{1, l.maxCount} {
			w := W(l.newWord(timestamp, count))
			if got := e.timestamp(l, w); got != timestamp {
				t.Fatalf("base %d: newWord(%d, %d) unpacks to timestamp %d", e.base, timestamp, count, got)
			}
			if got := l.count(int64(w)); got != count {
				t.Fatalf("base %d: newWord(%d, %d) unpacks to count %d", e.base, timestamp, count, got)
			}
			if got, want := l.full(int64(w)), count == l.maxCount; got != want {
				t.Fatalf("base %d: newWord(%d, %d) full = %t, want %t", e.base, timestamp, count, got, want)
			}
			floor := int64(W(l.floor(timestamp)))
			if got, want := l.accepts(int64(w), floor), count < l.maxCount; got != want {
				t.Fatalf("base %d: accepts(newWord(%d, %d), floor(%d)) = %t, want %t",
					e.base, timestamp, count, timestamp, got, want)
			}
			if !l.sameBucket(int64(w), floor) {
				t.Fatalf("base %d: newWord(%d, %d) is not in the bucket of its own floor", e.base, timestamp, count)
			}
			for _, other := range timestamps {
				if other != timestamp && l.sameBucket(int64(w), int64(W(l.floor(other)))) {
					t.Fatalf("base %d: newWord(%d, %d) is in the bucket of %d", e.base, timestamp, count, other)
				}
			}
		}
	}
}

// requireTimestampOrdersWords asserts that, relative to the floor of the base, a
// full count never outranks a later timestamp, across the wrap of the low bits
// too, and that the floor of a timestamp sits at or below every word of that
// timestamp.
func requireTimestampOrdersWords[W word](t *testing.T, l bucketLayout, e *entry[W], timestamps []int64) {
	t.Helper()

	base := W(l.floor(e.base))
	for i := 1; i < len(timestamps); i++ {
		older := W(l.newWord(timestamps[i-1], l.maxCount)) - base
		newer := W(l.newWord(timestamps[i], 1)) - base
		if older >= newer {
			t.Fatalf(
				"base %d: newWord(%d, %d) is not below newWord(%d, 1) relative to the base",
				e.base, timestamps[i-1], l.maxCount, timestamps[i],
			)
		}
	}
	for _, timestamp := range timestamps {
		if got, want := W(l.floor(timestamp)), W(l.newWord(timestamp, 0)); got != want {
			t.Fatalf("base %d: floor(%d) = %d, want %d", e.base, timestamp, got, want)
		}
		if W(l.floor(timestamp))-base > W(l.newWord(timestamp, 1))-base {
			t.Fatalf("base %d: floor(%d) is above the first word of its own timestamp", e.base, timestamp)
		}
	}
}

// requireIncrementCountsOneEvent pins what the record methods rest on: counting
// one more event is an increment of the whole word, which must move the count
// and leave the timestamp alone, across the whole reach.
func requireIncrementCountsOneEvent[W word](t *testing.T, l bucketLayout, e *entry[W], timestamps []int64) {
	t.Helper()

	for _, timestamp := range timestamps {
		counted := W(l.newWord(timestamp, 1))
		counted++

		if got := e.timestamp(l, counted); got != timestamp {
			t.Fatalf("base %d: timestamp after incrementing the word = %d, want %d", e.base, got, timestamp)
		}
		if got := l.count(int64(counted)); got != 2 {
			t.Fatalf("base %d: count after incrementing the word = %d, want 2", e.base, got)
		}
	}
}

func requireRangeBoundaries(t *testing.T, l bucketLayout) {
	t.Helper()

	cases := []struct {
		timestamp int64
		want      bool
	}{
		{l.minTimestamp - 1, false},
		{l.minTimestamp, true},
		{l.maxTimestamp, true},
		{l.maxTimestamp + 1, false},
	}
	for _, tc := range cases {
		if got := l.inRange(tc.timestamp); got != tc.want {
			t.Fatalf("inRange(%d) = %t, want %t", tc.timestamp, got, tc.want)
		}
	}
}

// TestCountBitsSpillAcrossLayouts drives a Cache of each width past the count a
// single word holds. The events are all counted whatever the width; only the
// number of words the key occupies changes, and a narrow count buys nothing but
// spills.
func TestCountBitsSpillAcrossLayouts(t *testing.T) {
	const (
		epoch  = 100
		events = 1000
	)

	for _, bits := range []int{8, 12, 24} {
		t.Run(fmt.Sprint(bits), func(t *testing.T) {
			c := newTestCache(t, Config{
				Precision:  time.Second,
				WindowSize: 10 * time.Second,
				EpochUnit:  EpochInSeconds,
				CountBits:  bits,
			})

			for i := range events {
				if got, want := c.Store(epoch, "k"), i+1; got != want {
					t.Fatalf("Store %d = %d, want %d", i, got, want)
				}
			}
			if got := c.Get(epoch, "k"); got != events {
				t.Fatalf("Get = %d, want %d", got, events)
			}
			wantWords := (events + c.layout.maxCount - 1) / c.layout.maxCount
			if got := c.bucketBreadth("k"); got != wantWords {
				t.Fatalf("words retained = %d, want %d", got, wantWords)
			}

			expired := int64(epoch + 11)
			if got := c.Store(expired, "k"); got != 1 {
				t.Fatalf("Store past the window = %d, want 1", got)
			}
			if got := c.bucketBreadth("k"); got != 1 {
				t.Fatalf("words retained after the spilled bucket expired = %d, want 1", got)
			}

			if got := c.Store(c.layout.maxTimestamp+1, "k"); got != LateEvent {
				t.Fatalf("Store above the representable range = %d, want %d", got, LateEvent)
			}
			if got := c.Store(expired, "k"); got != 2 {
				t.Fatalf("Store after the rejected epoch = %d, want 2 (the high-water mark must not have moved)", got)
			}
		})
	}
}

// TestStoreGetMatchModelAcrossCountBits replays one fixed operation sequence
// against the reference model at the narrowest, default and widest widths, with
// bases chosen so that some epochs fall past the end of the layout's range and
// both implementations must reject them. It is the deterministic counterpart of
// FuzzStoreGetInvariants, which fuzzes the width as well.
func TestStoreGetMatchModelAcrossCountBits(t *testing.T) {
	ops := make([]byte, 256)
	for i := range ops {
		ops[i] = byte(37*i + 11)
	}

	for _, bits := range []int{minCountBits, defaultCountBits, maxCountBits} {
		t.Run(fmt.Sprint(bits), func(t *testing.T) {
			layout := newBucketLayout(bits)
			for _, base := range []int64{1_700_000_000, layout.maxTimestamp, layout.minTimestamp + 1} {
				t.Run(fmt.Sprint(base), func(t *testing.T) {
					requireModelAgreement(t, bits, base, ops)
				})
			}
		})
	}
}
