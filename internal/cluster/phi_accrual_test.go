package cluster_test

import (
	"sync"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
)

var phiBase = time.Unix(1_700_000_000, 0)

// feed drives a detector from phiBase with the given inter-arrival intervals and
// returns the timestamp of the last heartbeat.
func feed(d *cluster.PhiAccrual, intervals ...time.Duration) time.Time {
	t := phiBase
	d.Heartbeat(t)
	for _, iv := range intervals {
		t = t.Add(iv)
		d.Heartbeat(t)
	}
	return t
}

func repeat(iv time.Duration, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = iv
	}
	return out
}

func TestPhi_noOpinionBeforeHeartbeat(t *testing.T) {
	d := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	if got := d.Phi(phiBase); got != 0 {
		t.Fatalf("phi before any heartbeat = %v, want 0", got)
	}
}

func TestPhi_lowRightAfterHeartbeat(t *testing.T) {
	d := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	last := feed(d, repeat(time.Second, 20)...)
	if got := d.Phi(last); got > 1 {
		t.Fatalf("phi immediately after heartbeat = %v, want < 1", got)
	}
}

func TestPhi_risesWithElapsedTime(t *testing.T) {
	d := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	last := feed(d, repeat(time.Second, 20)...)

	prev := -1.0
	for _, elapsed := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second, 3 * time.Second} {
		got := d.Phi(last.Add(elapsed))
		if got < prev {
			t.Fatalf("phi not monotonic: at %v got %v, previous %v", elapsed, got, prev)
		}
		prev = got
	}
}

func TestPhi_steadySenderCrossesThreshold(t *testing.T) {
	d := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	last := feed(d, repeat(time.Second, 30)...)

	// At the expected cadence, still trusted.
	if !d.Available(last.Add(time.Second), 8.0) {
		t.Fatalf("steady sender wrongly suspected at 1 interval, phi=%v", d.Phi(last.Add(time.Second)))
	}
	// Several intervals late, firmly suspected.
	if d.Available(last.Add(5*time.Second), 8.0) {
		t.Fatalf("steady sender still trusted 5s late, phi=%v", d.Phi(last.Add(5*time.Second)))
	}
}

// The defining property: a jittery sender (wide interval distribution) tolerates
// a longer silence than a steady one before reaching the same suspicion.
func TestPhi_adaptsToJitter(t *testing.T) {
	steady := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	feed(steady, repeat(time.Second, 40)...)
	lastSteady := phiBase.Add(40 * time.Second)

	jittery := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	alt := make([]time.Duration, 0, 40)
	for range 20 {
		alt = append(alt, 200*time.Millisecond, 1800*time.Millisecond)
	}
	lastJittery := feed(jittery, alt...)

	const elapsed = 2 * time.Second
	phiSteady := steady.Phi(lastSteady.Add(elapsed))
	phiJittery := jittery.Phi(lastJittery.Add(elapsed))

	if phiJittery >= phiSteady {
		t.Fatalf("jitter not tolerated: phiJittery=%v, phiSteady=%v (want jittery lower)", phiJittery, phiSteady)
	}
	// Same silence: steady node is dead, jittery node still trusted.
	if steady.Available(lastSteady.Add(elapsed), 8.0) {
		t.Fatalf("steady node should be suspected at %v, phi=%v", elapsed, phiSteady)
	}
	if !jittery.Available(lastJittery.Add(elapsed), 8.0) {
		t.Fatalf("jittery node should still be trusted at %v, phi=%v", elapsed, phiJittery)
	}
}

func TestPhi_windowEvictsOldIntervals(t *testing.T) {
	// A small window must forget an early burst of slow heartbeats once enough
	// fast ones arrive, so the mean reflects only recent behaviour.
	d := cluster.NewPhiAccrual(4, 10*time.Millisecond, time.Second)
	feed(d, 5*time.Second, 5*time.Second, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)
	last := phiBase.Add(5*time.Second + 5*time.Second + 400*time.Millisecond)

	// Mean is now ~100ms; a 1s silence is 10 intervals late => high suspicion.
	if d.Available(last.Add(time.Second), 8.0) {
		t.Fatalf("window did not adapt to faster cadence, phi=%v", d.Phi(last.Add(time.Second)))
	}
}

func TestPhi_concurrentAccess(t *testing.T) {
	d := cluster.NewPhiAccrual(100, 100*time.Millisecond, time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			now := time.Now()
			for range 200 {
				now = now.Add(time.Millisecond)
				d.Heartbeat(now)
				_ = d.Phi(now.Add(time.Second))
			}
		})
	}
	wg.Wait()
}
