package cluster_test

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
)

var testLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func newMembership(suspect, dead time.Duration) *cluster.Membership {
	ring := cluster.NewRing(10)
	return cluster.NewMembership(ring, suspect, dead, testLog)
}

func TestMembership_register(t *testing.T) {
	m := newMembership(10*time.Second, 20*time.Second)
	if err := m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	nodes := m.ActiveNodes()
	if len(nodes) != 1 {
		t.Fatalf("ActiveNodes() = %d, want 1", len(nodes))
	}
	if nodes[0].ID != "w1" {
		t.Errorf("node ID = %q, want w1", nodes[0].ID)
	}
	if nodes[0].State != cluster.NodeActive {
		t.Errorf("node state = %q, want Active", nodes[0].State)
	}
}

func TestMembership_heartbeat_updates_metrics(t *testing.T) {
	m := newMembership(10*time.Second, 20*time.Second)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	hb := cluster.Heartbeat{
		NodeID: "w1",
		Metrics: cluster.NodeMetrics{ActiveTasks: 5, CPUPct: 80.0},
	}
	if err := m.Heartbeat(hb); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	nodes := m.ActiveNodes()
	if nodes[0].Metrics.ActiveTasks != 5 {
		t.Errorf("ActiveTasks = %d, want 5", nodes[0].Metrics.ActiveTasks)
	}
}

func TestMembership_heartbeat_unknownNode(t *testing.T) {
	m := newMembership(10*time.Second, 20*time.Second)
	err := m.Heartbeat(cluster.Heartbeat{NodeID: "ghost"})
	if err == nil {
		t.Fatal("expected error for heartbeat from unknown node")
	}
}

func TestMembership_activeToSuspect(t *testing.T) {
	const suspect = 5 * time.Millisecond
	m := newMembership(suspect, 100*time.Millisecond)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(suspect + 5*time.Millisecond)
	m.Tick()

	all := m.AllNodes()
	if len(all) == 0 {
		t.Fatal("no nodes after Tick")
	}
	if all[0].State != cluster.NodeSuspect {
		t.Errorf("state after suspect timeout = %q, want Suspect", all[0].State)
	}
	if len(m.ActiveNodes()) != 0 {
		t.Error("ActiveNodes() should be empty when node is Suspect")
	}
}

func TestMembership_suspectToDead(t *testing.T) {
	const (
		suspect = 5 * time.Millisecond
		dead = 10 * time.Millisecond
	)
	m := newMembership(suspect, dead)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(dead + 5*time.Millisecond)
	m.Tick() // Active -> Suspect
	m.Tick() // Suspect -> Dead

	all := m.AllNodes()
	if all[0].State != cluster.NodeDead {
		t.Errorf("state after dead timeout = %q, want Dead", all[0].State)
	}
}

func TestMembership_recoveryFromSuspect(t *testing.T) {
	const suspect = 5 * time.Millisecond
	m := newMembership(suspect, 100*time.Millisecond)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(suspect + 5*time.Millisecond)
	m.Tick() // -> Suspect

	if err := m.Heartbeat(cluster.Heartbeat{NodeID: "w1"}); err != nil {
		t.Fatalf("Heartbeat after Suspect: %v", err)
	}
	nodes := m.ActiveNodes()
	if len(nodes) != 1 || nodes[0].State != cluster.NodeActive {
		t.Errorf("after recovery heartbeat: state = %q, want Active", nodes[0].State)
	}
}

func TestMembership_recoveryFromDead(t *testing.T) {
	const (
		suspect = 5 * time.Millisecond
		dead = 10 * time.Millisecond
	)
	m := newMembership(suspect, dead)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(dead + 5*time.Millisecond)
	m.Tick()
	m.Tick() // -> Dead

	// Worker restarts and sends heartbeat
	if err := m.Heartbeat(cluster.Heartbeat{NodeID: "w1"}); err != nil {
		t.Fatalf("Heartbeat from dead node: %v", err)
	}
	nodes := m.ActiveNodes()
	if len(nodes) != 1 || nodes[0].State != cluster.NodeActive {
		t.Errorf("after dead-node recovery: state = %q, want Active", nodes[0].State)
	}
}

func TestMembership_failureEvents(t *testing.T) {
	const suspect = 5 * time.Millisecond
	m := newMembership(suspect, 100*time.Millisecond)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(suspect + 5*time.Millisecond)
	m.Tick()

	select {
	case ev := <-m.Events():
		if ev.NodeID != "w1" {
			t.Errorf("event NodeID = %q, want w1", ev.NodeID)
		}
		if ev.NewState != cluster.NodeSuspect {
			t.Errorf("event NewState = %q, want Suspect", ev.NewState)
		}
		if ev.OldState != cluster.NodeActive {
			t.Errorf("event OldState = %q, want Active", ev.OldState)
		}
	default:
		t.Fatal("no failure event emitted after Active->Suspect transition")
	}
}

func TestMembership_reRegister_resetsToActive(t *testing.T) {
	const suspect = 5 * time.Millisecond
	m := newMembership(suspect, 100*time.Millisecond)
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})

	time.Sleep(suspect + 5*time.Millisecond)
	m.Tick() // -> Suspect

	// Re-register acts as recovery
	m.Register(cluster.RegisterRequest{ID: "w1", Address: "localhost:9001"})
	if all := m.AllNodes(); all[0].State != cluster.NodeActive {
		t.Errorf("re-register did not restore Active, got %q", all[0].State)
	}
}

// TestMembership_concurrentHeartbeatAndTick runs heartbeats from many workers
// while Tick() drives the failure detector and readers query state — all at
// once. Purpose: surface data races under -race. The events channel is drained
// so emit() never blocks the detector.
func TestMembership_concurrentHeartbeatAndTick(t *testing.T) {
	m := newMembership(20*time.Millisecond, 40*time.Millisecond)
	const nodes = 20
	for i := 0; i < nodes; i++ {
		m.Register(cluster.RegisterRequest{ID: fmt.Sprintf("w%d", i), Address: "x"})
	}

	stop := make(chan struct{})

	// Drain failure events so the bounded channel never wedges.
	go func() {
		for {
			select {
			case <-m.Events():
			case <-stop:
				return
			}
		}
	}()

	var wg sync.WaitGroup

	// Heartbeat senders keep nodes alive
	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Heartbeat(cluster.Heartbeat{
					NodeID: id,
					Metrics: cluster.NodeMetrics{ActiveTasks: j},
				})
			}
		}(fmt.Sprintf("w%d", i))
	}

	// Detector ticking
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			m.Tick()
		}
	}()

	// Readers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			_ = m.AllNodes()
			_ = m.ActiveNodes()
		}
	}()

	wg.Wait()
	close(stop)
}
