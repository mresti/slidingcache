// Package cachestats publishes slidingcache.Cache.Stats counters as
// interval deltas, intended to be emitted as gauge-type metrics.
//
// Stats counters are monotonic throughout the cache's lifetime and are never
// reset. An Emitter stores the previous snapshot, calculates the difference
// with the current one at each interval, and passes it to a Sink. Guarantees:
//
//   - In each Delta, Accepted+Late+Future+OutOfRange == StoreCalls() and
//     GetHit+GetMiss+GetLate+GetFuture == GetCalls(), exactly: totals
//     are derived from the exact same differences, never from another Stats read.
//   - Without an intervening Reset, the sum of Deltas emitted between two points
//     in time equals the Stats difference between them. Run performs a final flush
//     upon cancellation, ensuring no calls are lost or duplicated.
//   - The emission path (Stats, Diff, Emit) allocates no memory.
package cachestats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mresti/slidingcache"
)

// StatsSource is the only thing Emitter requires from the cache.
// *slidingcache.Cache satisfies it; tests can inject a mock source.
type StatsSource interface {
	Stats() slidingcache.Stats
}

// Delta is the change in Stats counters between two consecutive snapshots,
// plus the instantaneous value of Keys.
type Delta struct {
	// Store calls during the interval, broken down by result.
	Accepted   uint64
	Late       uint64
	Future     uint64
	OutOfRange uint64

	// Get calls during the interval, broken down by result.
	GetHit    uint64
	GetMiss   uint64
	GetLate   uint64
	GetFuture uint64

	// Keys is the number of keys held by the cache at the time of the
	// snapshot. It is an instantaneous value, not a delta: it drops
	// legitimately when the janitor sweeps expired keys.
	Keys int

	// Interval is the actual time elapsed between the two snapshots, measured
	// with the monotonic clock. It differs from the configured interval if the
	// Sink was delayed, if a tick was dropped, or during the final flush.
	// Divide by it to derive rates.
	Interval time.Duration

	// Reset indicates that a counter regressed compared to the previous snapshot.
	// With a single cache this never happens; it only occurs if the source switched
	// to another cache (rebuilt with New). The Delta is then calculated from zero.
	Reset bool
}

// StoreCalls is the total number of Store calls during the interval.
func (d Delta) StoreCalls() uint64 {
	return d.Accepted + d.Late + d.Future + d.OutOfRange
}

// GetCalls is the total number of Get calls during the interval.
func (d Delta) GetCalls() uint64 {
	return d.GetHit + d.GetMiss + d.GetLate + d.GetFuture
}

// Diff calculates the change between two snapshots of the same cache. It is a
// pure function and allocates no memory.
//
// If any counter in cur is lower than in prev, the counters have reset and
// the Delta is calculated against zero for all of them at once—not just for
// the one that regressed—so that the StoreCalls and GetCalls identities are
// preserved. Keys does not participate in this detection because it is a gauge.
func Diff(prev, cur slidingcache.Stats) Delta {
	reset := regressed(prev, cur)
	if reset {
		prev = slidingcache.Stats{}
	}
	return Delta{
		Accepted:   cur.Accepted - prev.Accepted,
		Late:       cur.Late - prev.Late,
		Future:     cur.Future - prev.Future,
		OutOfRange: cur.OutOfRange - prev.OutOfRange,
		GetHit:     cur.GetHit - prev.GetHit,
		GetMiss:    cur.GetMiss - prev.GetMiss,
		GetLate:    cur.GetLate - prev.GetLate,
		GetFuture:  cur.GetFuture - prev.GetFuture,
		Keys:       cur.Keys,
		Reset:      reset,
	}
}

func regressed(prev, cur slidingcache.Stats) bool {
	return cur.Accepted < prev.Accepted ||
		cur.Late < prev.Late ||
		cur.Future < prev.Future ||
		cur.OutOfRange < prev.OutOfRange ||
		cur.GetHit < prev.GetHit ||
		cur.GetMiss < prev.GetMiss ||
		cur.GetLate < prev.GetLate ||
		cur.GetFuture < prev.GetFuture
}

// Sink receives one Delta per interval. Emit is always called from the same
// goroutine, never concurrently. It receives the Delta by value: passing it by
// pointer through an interface would cause it to escape to the heap on every interval.
type Sink interface {
	Emit(Delta)
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(Delta)

// Emit calls f(d).
func (f SinkFunc) Emit(d Delta) { f(d) }

// ErrRunning is returned by Run when another Run is already in progress
// on the same Emitter.
var ErrRunning = errors.New("cachestats: Run ya está en ejecución en este Emitter")

// Emitter reads Stats from a source at each interval and passes the difference from
// the previous read to a Sink. Create one per cache, tied to its lifetime: if the
// cache is rebuilt with New, create a new Emitter for the new cache.
type Emitter struct {
	src      StatsSource
	sink     Sink
	interval time.Duration

	running atomic.Bool

	// State of the goroutine running Run; running serializes access.
	prev slidingcache.Stats
	last time.Time
}

// Option configures an Emitter in New.
type Option func(*Emitter)

// WithCurrentBaseline uses the source's current Stats as the initial snapshot
// instead of zero. Use this when attaching to a cache that has already been
// receiving traffic for some time, so that the first Delta does not carry its
// entire history. Without this option, the baseline is zero, which is correct if
// the Emitter is created alongside the cache: counters start at zero in slidingcache.New.
func WithCurrentBaseline() Option {
	return func(e *Emitter) { e.prev = e.src.Stats() }
}

// New creates an Emitter. interval must be positive; Stats locks each
// shard in turn, so an interval in seconds, rather than milliseconds, is recommended.
// If sink implements a Validate() error method, New invokes it and returns
// its error, ensuring a misconfigured Sink fails at startup rather than on
// the first interval.
func New(src StatsSource, sink Sink, interval time.Duration, opts ...Option) (*Emitter, error) {
	switch {
	case src == nil:
		return nil, errors.New("cachestats: StatsSource nil")
	case sink == nil:
		return nil, errors.New("cachestats: Sink nil")
	case interval <= 0:
		return nil, fmt.Errorf("cachestats: el intervalo debe ser > 0, recibido %s", interval)
	}
	if v, ok := sink.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}

	e := &Emitter{src: src, sink: sink, interval: interval}
	for _, opt := range opts {
		opt(e)
	}
	e.last = time.Now()
	return e, nil
}

// Run emits a Delta at each interval until ctx is canceled. Upon cancellation,
// it emits a final Delta with whatever was accumulated since the previous tick and
// returns nil. If another Run is already in progress on the same Emitter, it returns
// ErrRunning without doing anything. It can be called again once the previous one has returned.
//
// When Run returns, the Sink receives no further calls: wait for it to return
// before closing the exporter to avoid losing the final flush.
func (e *Emitter) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer e.running.Store(false)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.flush()
			return nil
		case <-ticker.C:
			e.flush()
		}
	}
}

// flush reads a snapshot, calculates the Delta relative to the previous one,
// and emits it. A missed tick caused by a delayed Sink does not throw anything
// off: the next Delta covers a longer interval, and Interval reflects this.
func (e *Emitter) flush() {
	cur := e.src.Stats()
	now := time.Now()

	d := Diff(e.prev, cur)
	d.Interval = now.Sub(e.last)
	e.prev, e.last = cur, now

	e.sink.Emit(d)
}

// Gauge is what GaugeSink requires from a gauge. It is satisfied
// by prometheus.Gauge, including the one returned by GaugeVec.WithLabelValues.
type Gauge interface {
	Set(float64)
}

// GaugeSink dumps each Delta into eleven gauges: the eight counters and the two
// totals as interval differences, and Keys as an instantaneous value.
// All fields are required; New verifies this via Validate.
//
// It resolves labels when constructing it (WithLabelValues once), not on each
// Emit: this way, emitting performs no map lookups and allocates no memory.
type GaugeSink struct {
	StoreAccepted   Gauge
	StoreLate       Gauge
	StoreFuture     Gauge
	StoreOutOfRange Gauge
	StoreCalls      Gauge

	GetHit    Gauge
	GetMiss   Gauge
	GetLate   Gauge
	GetFuture Gauge
	GetCalls  Gauge

	Keys Gauge
}

// Emit sets the eleven gauges at each interval, including when the value is
// zero: a gauge that is not updated retains its last value, and an interval
// with no traffic would appear to repeat the traffic of the previous one.
//
// The conversion from uint64 to float64 is exact up to 2^53, well above
// any number of calls in an interval.
func (g *GaugeSink) Emit(d Delta) {
	g.StoreAccepted.Set(float64(d.Accepted))
	g.StoreLate.Set(float64(d.Late))
	g.StoreFuture.Set(float64(d.Future))
	g.StoreOutOfRange.Set(float64(d.OutOfRange))
	g.StoreCalls.Set(float64(d.StoreCalls()))

	g.GetHit.Set(float64(d.GetHit))
	g.GetMiss.Set(float64(d.GetMiss))
	g.GetLate.Set(float64(d.GetLate))
	g.GetFuture.Set(float64(d.GetFuture))
	g.GetCalls.Set(float64(d.GetCalls()))

	g.Keys.Set(float64(d.Keys))
}

// Validate verifies that all gauges are assigned.
func (g *GaugeSink) Validate() error {
	fields := [...]struct {
		name  string
		gauge Gauge
	}{
		{"StoreAccepted", g.StoreAccepted},
		{"StoreLate", g.StoreLate},
		{"StoreFuture", g.StoreFuture},
		{"StoreOutOfRange", g.StoreOutOfRange},
		{"StoreCalls", g.StoreCalls},
		{"GetHit", g.GetHit},
		{"GetMiss", g.GetMiss},
		{"GetLate", g.GetLate},
		{"GetFuture", g.GetFuture},
		{"GetCalls", g.GetCalls},
		{"Keys", g.Keys},
	}
	var errs []error
	for _, f := range fields {
		if f.gauge == nil {
			errs = append(errs, fmt.Errorf("cachestats: GaugeSink.%s es nil", f.name))
		}
	}
	return errors.Join(errs...)
}
