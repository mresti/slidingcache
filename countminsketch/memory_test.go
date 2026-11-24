package countminsketch

import (
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mustMemoryBytes(t *testing.T, cfg Config) uint64 {
	t.Helper()
	estimate, ok := mustLayout(t, cfg).memoryBytes()
	if !ok {
		t.Fatalf("the estimate of %+v overflows", cfg)
	}
	return estimate
}

// allocatedBytes adds up what the rings and timestamps of c hold, read from
// the structures New built rather than from the formula of the estimate.
func allocatedBytes(c *Cache) uint64 {
	var total uint64
	for _, s := range c.shards {
		ring := s.window.ring
		slotCells := uint64(ring.Slots()) * uint64(len(ring.AppendSlotCells(nil, 0)))
		total += slotCells*slotCellBytes +
			uint64(len(s.window.timestamps))*timestampBytes +
			uint64(len(ring.AppendAggregateCells(nil)))*aggregateCellBytes
	}
	return total
}

func TestMemoryEstimateIsWhatNewAllocates(t *testing.T) {
	configs := map[string]Config{
		"defaults":          validConfig(),
		"one shard":         {Precision: time.Second, WindowSize: time.Minute, Shards: 1},
		"uneven columns":    {Precision: 7 * time.Second, WindowSize: 50 * time.Second, Epsilon: 0.05, Shards: 4},
		"narrowest":         {Precision: time.Second, WindowSize: time.Second, Epsilon: 0.9, Delta: 0.9},
		"many shards, rows": {Precision: time.Second, WindowSize: time.Minute, Delta: 1e-6, Shards: 64},
	}
	for name, cfg := range configs {
		t.Run(name, func(t *testing.T) {
			if got, want := allocatedBytes(newTestCache(t, cfg)), mustMemoryBytes(t, cfg); got != want {
				t.Fatalf("New allocated %d bytes of cells and timestamps, the estimate is %d", got, want)
			}
		})
	}
}

func TestMaxMemoryBytesIsInclusive(t *testing.T) {
	cfg := validConfig()
	estimate := mustMemoryBytes(t, cfg)

	cfg.MaxMemoryBytes = int64(estimate)
	newTestCache(t, cfg)

	cfg.MaxMemoryBytes = int64(estimate) - 1
	_, err := New(cfg)
	if err == nil {
		t.Fatalf("New with MaxMemoryBytes one below the estimate %d succeeded", estimate)
	}
	for _, part := range []string{
		strconv.FormatUint(estimate, 10) + " bytes",
		"cap of " + strconv.FormatUint(estimate-1, 10),
		"raise MaxMemoryBytes", "larger Epsilon or Precision", "smaller WindowSize",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not mention %q", err, part)
		}
	}
}

// TestHugeConfigIsRejectedBeforeAllocating asks for tens of gigabytes under the
// default cap. A huge make can succeed lazily, so returning an error quickly
// proves nothing: the test also checks that New allocated almost nothing.
func TestHugeConfigIsRejectedBeforeAllocating(t *testing.T) {
	cfg := Config{Precision: time.Second, WindowSize: 6_000 * time.Second, Epsilon: 1e-5}
	if estimate := mustMemoryBytes(t, cfg); estimate < 10<<30 {
		t.Fatalf("the config needs %d bytes, want tens of GB for the test to mean anything", estimate)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c, err := New(cfg)
	runtime.ReadMemStats(&after)

	if err == nil || c != nil {
		t.Fatalf("New = (%v, %v), want the memory cap's error", c, err)
	}
	if !strings.Contains(err.Error(), "cap of "+strconv.Itoa(defaultMaxMemoryBytes)) {
		t.Fatalf("error %q is not the default memory cap's", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("New allocated %d bytes before failing, want almost none", allocated)
	}
}

func TestMaxMemoryBytesZeroSelectsOneGiB(t *testing.T) {
	cfg := Config{Precision: time.Second, WindowSize: 2_000 * time.Second, Epsilon: 1e-4}
	estimate := mustMemoryBytes(t, cfg)
	if estimate <= defaultMaxMemoryBytes {
		t.Fatalf("the config needs %d bytes, want more than the default cap", estimate)
	}
	if _, err := New(cfg); err == nil {
		t.Fatalf("New of %d bytes under the default cap succeeded", estimate)
	}
	cfg.WindowSize /= 100
	if estimate := mustMemoryBytes(t, cfg); estimate > defaultMaxMemoryBytes {
		t.Fatalf("the smaller config needs %d bytes, want it under the default cap", estimate)
	}
	newTestCache(t, cfg)
}

// TestDefaultMaxMemoryBytesIsExactlyOneGiB pins the default cap on both sides
// with layouts that are only sized, never allocated. Every estimate is a
// multiple of 4 bytes, so the smallest one above 1 GiB is 1 GiB + 4.
func TestDefaultMaxMemoryBytesIsExactlyOneGiB(t *testing.T) {
	const oneGiB = 1 << 30
	tests := []struct {
		name     string
		layout   windowLayout
		estimate uint64
		fits     bool
	}{
		{
			name:     "exactly 1 GiB",
			layout:   windowLayout{shards: 1, shardWidth: 1<<26 - 1, depth: 1, buckets: 2},
			estimate: oneGiB,
			fits:     true,
		},
		{
			name:     "smallest estimate above",
			layout:   windowLayout{shards: 1, shardWidth: 89_478_485, depth: 1, buckets: 1},
			estimate: oneGiB + 4,
			fits:     false,
		},
	}
	limit := Config{}.maxMemoryBytes()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if estimate, ok := tt.layout.memoryBytes(); !ok || estimate != tt.estimate {
				t.Fatalf("memoryBytes() = (%d, %t), want (%d, true)", estimate, ok, tt.estimate)
			}
			if err := tt.layout.checkMemory(limit); (err == nil) != tt.fits {
				t.Fatalf("checkMemory(default) of %d bytes = %v, want fits=%t", tt.estimate, err, tt.fits)
			}
		})
	}
}

// TestMemoryEstimateOverflowCountsAsAboveTheCap uses layouts no valid Config
// reaches, the bounds of Dimensions keep the product below 2^64, to show that
// the estimate reports an overflow instead of wrapping to a small number.
func TestMemoryEstimateOverflowCountsAsAboveTheCap(t *testing.T) {
	layouts := map[string]windowLayout{
		"product of the cells": {shards: 1 << 20, shardWidth: math.MaxInt32, depth: 16, buckets: 1 << 20},
		"sum of the parts":     {shards: 1, shardWidth: 1 << 30, depth: 1 << 30, buckets: 2},
	}
	for name, layout := range layouts {
		t.Run(name, func(t *testing.T) {
			if estimate, ok := layout.memoryBytes(); ok {
				t.Fatalf("memoryBytes() = %d, want an overflow", estimate)
			}
			err := layout.checkMemory(math.MaxUint64)
			if err == nil || !strings.Contains(err.Error(), "more than 2^64 bytes") {
				t.Fatalf("checkMemory = %v, want the overflow error", err)
			}
		})
	}
}
