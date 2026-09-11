package slidingcache

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestConfigCountBitsValidation pins the accepted values of Config.CountBits and
// the layout each of them selects, including the zero value that stands for the
// default width.
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
				for _, s := range c.shards {
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

// TestBucketLayoutBounds checks, for every documented width, that a layout packs
// and unpacks the extremes of its own range and that the packed words order by
// timestamp alone, which is what lets lowerBound compare them without unpacking.
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

			requireBucketRoundTrip(t, l)
			requireTimestampOrdersWords(t, l)
			requireIncrementCountsOneEvent(t, l)
			requireRangeBoundaries(t, l)
		})
	}
}

func requireBucketRoundTrip(t *testing.T, l bucketLayout) {
	t.Helper()

	for _, timestamp := range []int64{l.minTimestamp, -1, 0, 1_700_000_000, l.maxTimestamp} {
		for _, count := range []int{1, l.maxCount} {
			b := l.newBucket(timestamp, count)
			if got := l.timestamp(b); got != timestamp {
				t.Fatalf("newBucket(%d, %d) unpacks to timestamp %d", timestamp, count, got)
			}
			if got := l.count(b); got != count {
				t.Fatalf("newBucket(%d, %d) unpacks to count %d", timestamp, count, got)
			}
			if got, want := l.full(b), count == l.maxCount; got != want {
				t.Fatalf("newBucket(%d, %d).full = %t, want %t", timestamp, count, got, want)
			}
			if got, want := l.accepts(b, timestamp), count < l.maxCount; got != want {
				t.Fatalf("accepts(newBucket(%d, %d), %d) = %t, want %t", timestamp, count, timestamp, got, want)
			}
			if l.accepts(b, timestamp-1) {
				t.Fatalf("newBucket(%d, %d) accepts an event of another bucket", timestamp, count)
			}
		}
	}
}

// requireTimestampOrdersWords asserts that a full count never outranks a later
// timestamp, and that the floor of a timestamp sits at or below every word of
// that timestamp.
func requireTimestampOrdersWords(t *testing.T, l bucketLayout) {
	t.Helper()

	timestamps := []int64{l.minTimestamp, -1, 0, 1_700_000_000, l.maxTimestamp}
	for i := 1; i < len(timestamps); i++ {
		older := l.newBucket(timestamps[i-1], l.maxCount)
		newer := l.newBucket(timestamps[i], 1)
		if older >= newer {
			t.Fatalf(
				"newBucket(%d, %d) is not below newBucket(%d, 1)",
				timestamps[i-1], l.maxCount, timestamps[i],
			)
		}
	}
	for _, timestamp := range timestamps {
		if got, want := l.floor(timestamp), l.newBucket(timestamp, 0); got != want {
			t.Fatalf("floor(%d) = %d, want %d", timestamp, got, want)
		}
		if l.floor(timestamp) > l.newBucket(timestamp, 1) {
			t.Fatalf("floor(%d) is above the first word of its own timestamp", timestamp)
		}
	}
}

// requireIncrementCountsOneEvent pins what the record methods rest on: counting
// one more event is an increment of the whole word, which must move the count
// and leave the timestamp alone.
func requireIncrementCountsOneEvent(t *testing.T, l bucketLayout) {
	t.Helper()

	const timestamp = 1_700_000_000
	counted := l.newBucket(timestamp, 1)
	counted++

	if got := l.timestamp(counted); got != timestamp {
		t.Fatalf("timestamp after incrementing the word = %d, want %d", got, timestamp)
	}
	if got := l.count(counted); got != 2 {
		t.Fatalf("count after incrementing the word = %d, want 2", got)
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

			if got := c.Store(c.layout.maxTimestamp+1, "k"); got != lateEvent {
				t.Fatalf("Store above the representable range = %d, want %d", got, lateEvent)
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
