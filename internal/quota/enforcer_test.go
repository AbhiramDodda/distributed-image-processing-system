package quota

import (
	"sync"
	"testing"
)

func TestEnforcer_deniesUnknownTenant(t *testing.T) {
	e := NewEnforcer()
	if _, err := e.Reserve("nobody", Request{CPUCores: 1}); err == nil {
		t.Fatal("expected reservation for unconfigured tenant to be denied")
	}
}

func TestEnforcer_reserveAndRelease(t *testing.T) {
	e := NewEnforcer()
	e.SetQuota("t", TenantQuota{MaxCPUCores: 8, MaxMemoryGB: 32, MaxGPUCount: 2, MaxActiveJobs: 4})

	r, err := e.Reserve("t", Request{CPUCores: 4, MemoryGB: 16, GPUCount: 1})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if u := e.Usage("t"); u.CPUCores != 4 || u.MemoryGB != 16 || u.GPUCount != 1 || u.ActiveJobs != 1 {
		t.Fatalf("usage after reserve = %+v", u)
	}
	r.Release()
	if u := e.Usage("t"); u != (Usage{}) {
		t.Fatalf("usage after release = %+v, want zero", u)
	}
}

func TestEnforcer_rejectsEachDimension(t *testing.T) {
	base := TenantQuota{MaxCPUCores: 4, MaxMemoryGB: 16, MaxGPUCount: 1, MaxActiveJobs: 2}
	cases := map[string]Request{
		"cpu": {CPUCores: 5},
		"memory": {MemoryGB: 17},
		"gpu": {GPUCount: 2},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			e := NewEnforcer()
			e.SetQuota("t", base)
			if _, err := e.Reserve("t", req); err == nil {
				t.Fatalf("expected %s over-limit request to be rejected", name)
			}
		})
	}
}

func TestEnforcer_activeJobLimit(t *testing.T) {
	e := NewEnforcer()
	e.SetQuota("t", TenantQuota{MaxCPUCores: 100, MaxMemoryGB: 100, MaxGPUCount: 100, MaxActiveJobs: 2})
	if _, err := e.Reserve("t", Request{CPUCores: 1}); err != nil {
		t.Fatalf("reserve 1: %v", err)
	}
	if _, err := e.Reserve("t", Request{CPUCores: 1}); err != nil {
		t.Fatalf("reserve 2: %v", err)
	}
	if _, err := e.Reserve("t", Request{CPUCores: 1}); err == nil {
		t.Fatal("expected third reservation to hit active-job limit")
	}
}

func TestEnforcer_doubleReleaseIsNoop(t *testing.T) {
	e := NewEnforcer()
	e.SetQuota("t", TenantQuota{MaxCPUCores: 8, MaxMemoryGB: 8, MaxGPUCount: 8, MaxActiveJobs: 8})
	r, _ := e.Reserve("t", Request{CPUCores: 4, MemoryGB: 4, GPUCount: 1})
	r.Release()
	r.Release()
	if u := e.Usage("t"); u != (Usage{}) {
		t.Fatalf("double release corrupted usage: %+v", u)
	}
}

// A ceiling that fits either job alone but not both must admit exactly one when
// they race.
func TestEnforcer_concurrentReserveRespectsCeiling(t *testing.T) {
	e := NewEnforcer()
	e.SetQuota("t", TenantQuota{MaxCPUCores: 6, MaxMemoryGB: 100, MaxGPUCount: 100, MaxActiveJobs: 100})

	const goroutines = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for range goroutines {
		wg.Go(func() {
			if _, err := e.Reserve("t", Request{CPUCores: 4}); err == nil {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if admitted != 1 {
		t.Fatalf("admitted %d concurrent reservations, want exactly 1", admitted)
	}
	if u := e.Usage("t"); u.CPUCores != 4 {
		t.Fatalf("cpu usage = %.1f, want 4", u.CPUCores)
	}
}
