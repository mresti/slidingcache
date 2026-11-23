package countmin

import (
	"math"
	"strconv"
	"testing"
)

func TestDimensions(t *testing.T) {
	tests := []struct {
		name               string
		epsilon, delta     float64
		wantWidth, wantDep int
	}{
		{"plan default", 0.001, 0.01, 2719, 5},
		{"ten times narrower epsilon", 0.0001, 0.01, 27183, 5},
		{"delta close to one keeps one row", 0.5, 0.99, 6, 1},
		{"delta just above e^-16 needs sixteen rows", 0.1, math.Exp(-16) * 1.0001, 28, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			width, depth, err := Dimensions(tt.epsilon, tt.delta)
			if err != nil {
				t.Fatalf("Dimensions(%v, %v): %v", tt.epsilon, tt.delta, err)
			}
			if width != tt.wantWidth || depth != tt.wantDep {
				t.Fatalf("Dimensions(%v, %v) = %d x %d, want %d x %d",
					tt.epsilon, tt.delta, width, depth, tt.wantWidth, tt.wantDep)
			}
		})
	}
}

func TestDimensionsRejectsInvalidParameters(t *testing.T) {
	invalid := []float64{0, 1, -0.5, 1.5, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, bad := range invalid {
		t.Run("epsilon="+strconv.FormatFloat(bad, 'g', -1, 64), func(t *testing.T) {
			if _, _, err := Dimensions(bad, 0.01); err == nil {
				t.Fatalf("Dimensions(%v, 0.01) succeeded, want an error", bad)
			}
		})
		t.Run("delta="+strconv.FormatFloat(bad, 'g', -1, 64), func(t *testing.T) {
			if _, _, err := Dimensions(0.01, bad); err == nil {
				t.Fatalf("Dimensions(0.01, %v) succeeded, want an error", bad)
			}
		})
	}
	beyond := []struct {
		name           string
		epsilon, delta float64
	}{
		{"delta needing seventeen rows", 0.1, math.Exp(-16) / 1.0001},
		{"tiny delta", 0.1, 1e-300},
		{"epsilon whose width exceeds MaxUint32", 1e-10, 0.01},
		{"denormal epsilon whose width is infinite", 5e-324, 0.01},
	}
	for _, tt := range beyond {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := Dimensions(tt.epsilon, tt.delta); err == nil {
				t.Fatalf("Dimensions(%v, %v) succeeded, want an error", tt.epsilon, tt.delta)
			}
		})
	}
}

func TestNewMatchesDimensions(t *testing.T) {
	s, err := New(0.001, 0.01)
	if err != nil {
		t.Fatal(err)
	}
	if s.width != 2719 || s.depth != 5 || len(s.cells) != 2719*5 {
		t.Fatalf("New(0.001, 0.01) is %d x %d with %d cells, want 2719 x 5", s.width, s.depth, len(s.cells))
	}
	if _, err := New(0, 0.01); err == nil {
		t.Fatal("New(0, 0.01) succeeded, want an error")
	}
}

func TestSizedConstructorsValidateDimensions(t *testing.T) {
	type size struct{ width, depth int }
	invalid := []size{{0, 5}, {-1, 5}, {10, 0}, {10, -1}, {10, MaxDepth + 1}}
	if strconv.IntSize == 64 {
		// A variable, not a constant, so the file still compiles where int is 32 bits.
		tooWide := uint64(math.MaxUint32) + 1
		invalid = append(invalid, size{int(tooWide), 1})
	}
	for _, sz := range invalid {
		if _, err := NewSized(sz.width, sz.depth); err == nil {
			t.Errorf("NewSized(%d, %d) succeeded, want an error", sz.width, sz.depth)
		}
		if _, err := NewAggregate(sz.width, sz.depth); err == nil {
			t.Errorf("NewAggregate(%d, %d) succeeded, want an error", sz.width, sz.depth)
		}
	}
	for _, sz := range []size{{1, 1}, {7, MaxDepth}, {2719, 5}} {
		s, err := NewSized(sz.width, sz.depth)
		if err != nil {
			t.Fatalf("NewSized(%d, %d): %v", sz.width, sz.depth, err)
		}
		a, err := NewAggregate(sz.width, sz.depth)
		if err != nil {
			t.Fatalf("NewAggregate(%d, %d): %v", sz.width, sz.depth, err)
		}
		if len(s.cells) != sz.width*sz.depth || len(a.cells) != sz.width*sz.depth {
			t.Fatalf("%d x %d gave %d sketch and %d aggregate cells, want exactly %d",
				sz.width, sz.depth, len(s.cells), len(a.cells), sz.width*sz.depth)
		}
	}
}

// TestMemoryLimitOfA32BitInt exercises the size check that only a 32-bit int can
// trip, with the limit such an int imposes.
func TestMemoryLimitOfA32BitInt(t *testing.T) {
	const maxInt32 = math.MaxInt32
	if err := checkMemory(math.MaxUint32, 1, sketchCellBytes, maxInt32); err == nil {
		t.Fatal("4 x 2^32 bytes fit in a 32-bit int, want an error")
	}
	if err := checkMemory(1<<27, 2, aggregateCellBytes, maxInt32); err == nil {
		t.Fatal("2^31 bytes fit in a 32-bit int, want an error")
	}
	if err := checkMemory(1<<27, 2, sketchCellBytes, maxInt32); err != nil {
		t.Fatalf("2^30 bytes rejected for a 32-bit int: %v", err)
	}
}
