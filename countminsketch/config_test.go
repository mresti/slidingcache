package countminsketch

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/mresti/slidingcache"
)

func validConfig() Config {
	return Config{Precision: time.Second, WindowSize: time.Minute}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	clock := func() int64 { return 0 }
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero Precision", func(c *Config) { c.Precision = 0 }},
		{"sub-second Precision", func(c *Config) { c.Precision = 500 * time.Millisecond }},
		{"fractional Precision", func(c *Config) { c.Precision = 1500 * time.Millisecond }},
		{"zero WindowSize", func(c *Config) { c.WindowSize = 0 }},
		{"fractional WindowSize", func(c *Config) { c.WindowSize = 90500 * time.Millisecond }},
		{"WindowSize below Precision", func(c *Config) { c.Precision, c.WindowSize = time.Minute, time.Second }},
		{"more than 2^20 buckets", func(c *Config) { c.WindowSize = (maxBuckets + 1) * time.Second }},
		{"2^20 buckets plus a partial one", func(c *Config) {
			c.Precision, c.WindowSize = 2*time.Second, (2*maxBuckets+1)*time.Second
		}},
		{"EpochUnit above the range", func(c *Config) { c.EpochUnit = slidingcache.EpochInSeconds + 1 }},
		{"negative EpochUnit", func(c *Config) { c.EpochUnit = -1 }},
		{"negative MaxFutureSkew", func(c *Config) { c.MaxFutureSkew = -time.Second }},
		{"fractional MaxFutureSkew", func(c *Config) { c.MaxFutureSkew = 1500 * time.Millisecond }},
		{"Clock without MaxFutureSkew", func(c *Config) { c.Clock = clock }},
		{"negative Epsilon", func(c *Config) { c.Epsilon = -0.01 }},
		{"Epsilon of 1", func(c *Config) { c.Epsilon = 1 }},
		{"Epsilon above 1", func(c *Config) { c.Epsilon = 2 }},
		{"NaN Epsilon", func(c *Config) { c.Epsilon = math.NaN() }},
		{"infinite Epsilon", func(c *Config) { c.Epsilon = math.Inf(1) }},
		{"negative Delta", func(c *Config) { c.Delta = -0.01 }},
		{"Delta of 1", func(c *Config) { c.Delta = 1 }},
		{"NaN Delta", func(c *Config) { c.Delta = math.NaN() }},
		{"Delta needing more than 16 rows", func(c *Config) { c.Delta = 1e-8 }},
		{"TimestampBits below 39", func(c *Config) { c.TimestampBits = minTimestampBits - 1 }},
		{"TimestampBits above 62", func(c *Config) { c.TimestampBits = maxTimestampBits + 1 }},
		{"negative TimestampBits", func(c *Config) { c.TimestampBits = -1 }},
		{"DisableRangeGuard with TimestampBits", func(c *Config) { c.DisableRangeGuard, c.TimestampBits = true, 50 }},
		{"negative Shards", func(c *Config) { c.Shards = -1 }},
		{"Shards above 2^20", func(c *Config) { c.Shards = maxShards + 1 }},
		{"negative MaxMemoryBytes", func(c *Config) { c.MaxMemoryBytes = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			c, err := New(cfg)
			if err == nil {
				t.Fatalf("New(%+v) succeeded, want an error", cfg)
			}
			if c != nil {
				t.Fatal("New returned a cache alongside an error")
			}
			if !strings.HasPrefix(err.Error(), "countminsketch: ") {
				t.Fatalf("error %q lacks the package prefix", err)
			}
		})
	}
}

func TestNewAcceptsTheBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"defaults", func(*Config) {}},
		{"WindowSize equal to Precision", func(c *Config) { c.WindowSize = c.Precision }},
		// New allocates the whole ring, so the 2^20-bucket cases use the narrowest
		// sketch, 4 x 1 cells: 16 MiB of slots instead of 57 GB at the default.
		{"exactly 2^20 buckets", func(c *Config) {
			c.WindowSize, c.Epsilon, c.Delta = maxBuckets*time.Second, 0.9, 0.9
		}},
		{"2^20 buckets rounded up", func(c *Config) {
			c.Precision, c.WindowSize, c.Epsilon, c.Delta = 2*time.Second, (2*maxBuckets-1)*time.Second, 0.9, 0.9
		}},
		{"TimestampBits 39", func(c *Config) { c.TimestampBits = minTimestampBits }},
		{"TimestampBits 62", func(c *Config) { c.TimestampBits = maxTimestampBits }},
		{"DisableRangeGuard alone", func(c *Config) { c.DisableRangeGuard = true }},
		{"MaxFutureSkew without Clock", func(c *Config) { c.MaxFutureSkew = time.Minute }},
		{"narrow sketch", func(c *Config) { c.Epsilon, c.Delta = 0.9, 0.9 }},
		{"nanos", func(c *Config) { c.EpochUnit = slidingcache.EpochInNanos }},
		{"seconds", func(c *Config) { c.EpochUnit = slidingcache.EpochInSeconds }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			newTestCache(t, cfg)
		})
	}
}

func TestDefaultsSelectDocumentedValues(t *testing.T) {
	width, depth, err := validConfig().dimensions()
	if err != nil || width != 2719 || depth != 5 {
		t.Fatalf("default dimensions = %dx%d, %v; want 2719x5 from Epsilon 0.001 and Delta 0.01", width, depth, err)
	}
	ranges := []struct {
		name     string
		cfg      Config
		min, max int64
	}{
		{"TimestampBits 0", validConfig(), -1 << 43, 1<<43 - 1},
		{
			"DisableRangeGuard",
			Config{Precision: time.Second, WindowSize: time.Minute, DisableRangeGuard: true},
			-1 << 62, 1<<62 - 1,
		},
	}
	for _, r := range ranges {
		c := newTestCache(t, r.cfg)
		if c.minTimestamp != r.min || c.maxTimestamp != r.max {
			t.Fatalf("%s accepts [%d, %d], want [%d, %d]", r.name, c.minTimestamp, c.maxTimestamp, r.min, r.max)
		}
	}
}

// TestDefaultUnitIsMillis checks that a Config omitting EpochUnit reads epochs
// as milliseconds, as the root package does.
func TestDefaultUnitIsMillis(t *testing.T) {
	c := newTestCache(t, validConfig())
	c.Store(1_700_000_000_999, "k")
	if hw, _ := c.HighWater(); hw != 1_700_000_000 {
		t.Fatalf("HighWater() = %d after a millisecond epoch, want 1700000000", hw)
	}
}

func TestWithHashFuncRejectsNil(t *testing.T) {
	c, err := New(validConfig(), WithHashFunc(nil))
	if err == nil || c != nil {
		t.Fatalf("New with a nil hash = (%v, %v), want an error and no cache", c, err)
	}
}

func TestFailingOptionAbortsNew(t *testing.T) {
	errOption := errors.New("option failed")
	c, err := New(validConfig(), func(*Cache) error { return errOption })
	if !errors.Is(err, errOption) || c != nil {
		t.Fatalf("New = (%v, %v), want the option's error and no cache", c, err)
	}
}

func TestWithHashFuncIsUsedForEveryKey(t *testing.T) {
	c := newTestCache(t, validConfig(), WithHashFunc(func(string) uint64 { return 42 }))
	c.Store(1_000, "a")
	if got := c.Store(1_000, "b"); got != 2 {
		t.Fatalf("Store of another key with the same hash = %d, want 2: equal hashes are one key", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := newTestCache(t, validConfig())
	for range 3 {
		if err := c.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
	}
	if got := c.Store(1_000, "k"); got != 1 {
		t.Fatalf("Store after Close = %d, want 1: Close stops nothing", got)
	}
}

func TestShardsRoundUpToAPowerOfTwo(t *testing.T) {
	for requested, want := range map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 16: 16, 17: 32} {
		cfg := validConfig()
		cfg.Shards = requested
		if got := len(newTestCache(t, cfg).shards); got != want {
			t.Errorf("Shards %d gives %d shards, want %d", requested, got, want)
		}
	}
}

// TestDefaultShardsLeaveEveryShardEnoughColumns walks the default down from 16
// as the sketch narrows, to 1 when not even 2 shards get minShardWidth columns:
// a default never makes New fail.
func TestDefaultShardsLeaveEveryShardEnoughColumns(t *testing.T) {
	tests := []struct {
		epsilon      float64
		totalColumns int
		wantShards   int
	}{
		{0, 2719, 16},
		{0.01, 272, 16},
		{0.05, 55, 4},
		{0.1, 28, 2},
		{0.19, 15, 2},
		{0.2, 14, 1},
		{0.9, 4, 1},
	}
	for _, tt := range tests {
		cfg := validConfig()
		cfg.Epsilon = tt.epsilon
		layout := mustLayout(t, cfg)
		if total, _, _ := cfg.dimensions(); total != tt.totalColumns {
			t.Fatalf("Epsilon %v gives %d columns, want %d", tt.epsilon, total, tt.totalColumns)
		}
		if layout.shards != tt.wantShards {
			t.Errorf("Epsilon %v defaults to %d shards, want %d", tt.epsilon, layout.shards, tt.wantShards)
		}
		if layout.shards > 1 && layout.shardWidth < minShardWidth {
			t.Errorf("Epsilon %v leaves %d columns per shard, below %d", tt.epsilon, layout.shardWidth, minShardWidth)
		}
		if got := len(newTestCache(t, cfg).shards); got != tt.wantShards {
			t.Errorf("New with Epsilon %v built %d shards, want %d", tt.epsilon, got, tt.wantShards)
		}
	}
}

// TestShardsAcceptTheMaximum sizes 2^20 shards without building them, which
// would need gigabytes: the bound is inclusive.
func TestShardsAcceptTheMaximum(t *testing.T) {
	cfg := validConfig()
	cfg.Shards, cfg.Epsilon = maxShards, 3e-7
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate with Shards 2^20: %v", err)
	}
	if layout := mustLayout(t, cfg); layout.shards != maxShards || layout.shardWidth < minShardWidth {
		t.Fatalf("Shards 2^20 gives %d shards of %d columns, want 2^20 of at least %d",
			layout.shards, layout.shardWidth, minShardWidth)
	}
}

func TestShardsTooManyForEpsilonAreRejected(t *testing.T) {
	cfg := validConfig()
	cfg.Epsilon, cfg.Shards = 0.05, 8
	_, err := New(cfg)
	if err == nil {
		t.Fatal("New with 7 columns per shard succeeded, want an error")
	}
	parts := []string{
		"Epsilon 0.05", "55 columns", "7 per shard", "8 Shards", "minimum of 8", "fewer Shards", "smaller Epsilon",
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not mention %q", err, part)
		}
	}

	cfg.Shards = 4
	if c := newTestCache(t, cfg); len(c.shards) != 4 {
		t.Fatalf("New with 14 columns per shard built %d shards, want 4", len(c.shards))
	}
	cfg.Epsilon, cfg.Shards = 0.9, 1
	newTestCache(t, cfg)
}

// TestTwoShardsNeedMinShardWidthColumnsEach pins the column check at the
// smallest shard count it applies to.
func TestTwoShardsNeedMinShardWidthColumnsEach(t *testing.T) {
	cfg := validConfig()
	cfg.Shards, cfg.Epsilon = 2, 0.2
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "7 per shard") {
		t.Fatalf("New with 2 Shards of 7 columns = %v, want the minimum-columns error", err)
	}

	cfg.Epsilon = 0.19
	if layout := mustLayout(t, cfg); layout.shards != 2 || layout.shardWidth != minShardWidth {
		t.Fatalf("Epsilon 0.19 with 2 Shards gives %d shards of %d columns, want 2 of %d",
			layout.shards, layout.shardWidth, minShardWidth)
	}
	newTestCache(t, cfg)
}
