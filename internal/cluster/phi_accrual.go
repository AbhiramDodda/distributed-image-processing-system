package cluster

import (
	"math"
	"sync"
	"time"
)

// PhiAccrual is an adaptive failure detector (Hayashibara et al., "The φ Accrual
// Failure Detector"). Rather than a fixed heartbeat timeout, it models the
// recent distribution of inter-arrival times and outputs phi: a continuously
// rising suspicion level.
//
// phi = -log10(P(a heartbeat is later than the time elapsed since the last one)).
// So phi = 1 means ~10% chance the node is wrongly suspected, phi = 8 means
// ~1e-8. Callers compare phi against a threshold instead of a wall-clock gap.
//
// The win over the two-stage detector in membership.go is adaptivity: a link
// with jittery heartbeats develops a wider interval distribution and tolerates
// longer gaps before phi rises, while a steady link is judged dead sooner — both
// without hand-tuned timeouts.
type PhiAccrual struct {
	mu sync.Mutex
	window *intervalWindow
	minStdDev float64 // nanoseconds; floor on std dev, avoids overconfidence
	firstInterval float64 // nanoseconds; seeds the window before real samples
	lastTs time.Time
}

// NewPhiAccrual builds a detector. windowSize bounds how many recent intervals
// shape the distribution; minStdDev floors the modelled jitter (so a perfectly
// regular sender is not judged with unrealistic confidence); firstInterval is
// the assumed cadence used to seed the window on the very first heartbeat.
// Non-positive arguments fall back to sane defaults.
func NewPhiAccrual(windowSize int, minStdDev, firstInterval time.Duration) *PhiAccrual {
	if windowSize <= 0 {
		windowSize = 100
	}
	if minStdDev <= 0 {
		minStdDev = 100 * time.Millisecond
	}
	if firstInterval <= 0 {
		firstInterval = time.Second
	}
	return &PhiAccrual{
		window: newIntervalWindow(windowSize),
		minStdDev: float64(minStdDev),
		firstInterval: float64(firstInterval),
	}
}

// Heartbeat records an arrival at time now. The first arrival only sets the
// baseline (and seeds the window with the assumed cadence); subsequent arrivals
// feed the measured interval into the distribution.
func (p *PhiAccrual) Heartbeat(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastTs.IsZero() {
		// Seed with two samples so the window has a defined mean immediately;
		// variance stays 0 and is handled by the minStdDev floor.
		p.window.push(p.firstInterval)
		p.window.push(p.firstInterval)
		p.lastTs = now
		return
	}
	interval := float64(now.Sub(p.lastTs))
	if interval > 0 {
		p.window.push(interval)
	}
	p.lastTs = now
}

// Phi returns the current suspicion level at time now. It is 0 before any
// heartbeat has been seen (no basis for an opinion) and rises as the gap since
// the last heartbeat grows relative to the learned interval distribution.
func (p *PhiAccrual) Phi(now time.Time) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastTs.IsZero() {
		return 0
	}
	elapsed := float64(now.Sub(p.lastTs))
	mean := p.window.mean()
	stdDev := math.Max(p.window.stdDev(), p.minStdDev)
	return phi(elapsed, mean, stdDev)
}

// Available reports whether the node is still trusted at the given threshold.
func (p *PhiAccrual) Available(now time.Time, threshold float64) bool {
	return p.Phi(now) < threshold
}

// phi computes the suspicion value using the logistic approximation of the
// Gaussian CDF tail (as in Akka's implementation): accurate to a few percent and
// far cheaper than erfc.
func phi(elapsed, mean, stdDev float64) float64 {
	y := (elapsed - mean) / stdDev
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	if elapsed > mean {
		return -math.Log10(e / (1 + e))
	}
	return -math.Log10(1 - 1/(1+e))
}

// intervalWindow is a fixed-capacity sliding window of interval samples with
// O(1) mean and variance via running sums.
type intervalWindow struct {
	values []float64
	cap int
	idx int
	filled bool
	sum float64
	sumSq float64
}

func newIntervalWindow(capacity int) *intervalWindow {
	return &intervalWindow{values: make([]float64, capacity), cap: capacity}
}

func (w *intervalWindow) push(v float64) {
	if w.filled {
		old := w.values[w.idx]
		w.sum -= old
		w.sumSq -= old * old
	}
	w.values[w.idx] = v
	w.sum += v
	w.sumSq += v * v
	w.idx++
	if w.idx == w.cap {
		w.idx = 0
		w.filled = true
	}
}

func (w *intervalWindow) len() int {
	if w.filled {
		return w.cap
	}
	return w.idx
}

func (w *intervalWindow) mean() float64 {
	n := w.len()
	if n == 0 {
		return 0
	}
	return w.sum / float64(n)
}

func (w *intervalWindow) stdDev() float64 {
	n := w.len()
	if n == 0 {
		return 0
	}
	mean := w.sum / float64(n)
	variance := w.sumSq/float64(n) - mean*mean
	if variance < 0 { // guard against float rounding near zero
		variance = 0
	}
	return math.Sqrt(variance)
}
