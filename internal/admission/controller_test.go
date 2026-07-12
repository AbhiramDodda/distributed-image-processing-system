package admission

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAdmit_failsClosedForUnknownTenant(t *testing.T) {
	c := New(Config{MaxInFlight: 10})
	if _, err := c.Admit("ghost"); !errors.Is(err, ErrRejected) {
		t.Fatalf("unknown tenant admitted or wrong error: %v", err)
	}
}

func TestAdmit_partitionsCapacityByWeight(t *testing.T) {
	// 3:1 weights over a cap of 8 -> shares of 6 and 2.
	c := New(Config{MaxInFlight: 8})
	c.SetClass("big", Class{Weight: 3})
	c.SetClass("small", Class{Weight: 1})

	got := map[string]int{}
	for _, tn := range []string{"big", "small"} {
		for {
			if _, err := c.Admit(tn); err != nil {
				break
			}
			got[tn]++
		}
	}
	if got["big"] != 6 || got["small"] != 2 {
		t.Fatalf("weighted shares = big:%d small:%d, want big:6 small:2", got["big"], got["small"])
	}
}

// A tenant is shed at its own share even when the platform as a whole has room:
// the point of per-tenant partitioning is isolation, not global utilisation.
func TestAdmit_isolatesTenantsBelowGlobalCap(t *testing.T) {
	c := New(Config{MaxInFlight: 10})
	c.SetClass("noisy", Class{Weight: 1})
	c.SetClass("quiet", Class{Weight: 1})

	// Equal weights over 10 -> share 5 each. noisy can take only 5 despite 10 free.
	admitted := 0
	for {
		if _, err := c.Admit("noisy"); err != nil {
			break
		}
		admitted++
	}
	if admitted != 5 {
		t.Fatalf("noisy admitted %d, want 5 (its share) -- it ate into quiet's guarantee", admitted)
	}
	// quiet's slice is untouched.
	if _, err := c.Admit("quiet"); err != nil {
		t.Fatalf("quiet rejected despite reserved share: %v", err)
	}
}

func TestRelease_freesSlotAndIsIdempotent(t *testing.T) {
	c := New(Config{MaxInFlight: 1})
	c.SetClass("t", Class{Weight: 1})

	tk, err := c.Admit("t")
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if _, err := c.Admit("t"); !errors.Is(err, ErrRejected) {
		t.Fatal("second admit should be shed at cap 1")
	}
	tk.Release()
	tk.Release() // idempotent: must not free a second phantom slot
	if s := c.Stats(); s.InFlight != 0 {
		t.Fatalf("in-flight after release = %d, want 0", s.InFlight)
	}
	if _, err := c.Admit("t"); err != nil {
		t.Fatalf("admit after release: %v", err)
	}
}

func TestStats_countsAdmittedAndRejected(t *testing.T) {
	c := New(Config{MaxInFlight: 2})
	c.SetClass("t", Class{Weight: 1})
	c.Admit("t")
	c.Admit("t")
	c.Admit("t") // shed
	s := c.Stats()
	if s.Admitted != 2 || s.Rejected != 1 || s.InFlight != 2 {
		t.Fatalf("stats = %+v, want admitted 2 rejected 1 in-flight 2", s)
	}
}

// Under a concurrent burst the controller must never admit past the global cap
// and must keep each tenant within its share -- the property that keeps the
// scheduler's working set bounded no matter the arrival rate.
func TestAdmit_concurrentBurstStaysBounded(t *testing.T) {
	const cap = 16
	c := New(Config{MaxInFlight: cap})
	tenants := []string{"a", "b", "c", "d"}
	for _, tn := range tenants {
		c.SetClass(tn, Class{Weight: 1}) // share 4 each
	}

	var admitted int64
	var mu sync.Mutex
	held := map[string]*Ticket{}
	perTenant := map[string]*int64{}
	for _, tn := range tenants {
		var n int64
		perTenant[tn] = &n
	}

	var wg sync.WaitGroup
	for _, tn := range tenants {
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(tn string, i int) {
				defer wg.Done()
				tk, err := c.Admit(tn)
				if err != nil {
					return
				}
				atomic.AddInt64(&admitted, 1)
				atomic.AddInt64(perTenant[tn], 1)
				mu.Lock()
				held[tn+"-"+itoa(i)] = tk
				mu.Unlock()
			}(tn, i)
		}
	}
	wg.Wait()

	if got := atomic.LoadInt64(&admitted); got > cap {
		t.Fatalf("admitted %d concurrently, exceeds global cap %d", got, cap)
	}
	for _, tn := range tenants {
		if n := atomic.LoadInt64(perTenant[tn]); n > 4 {
			t.Fatalf("tenant %q admitted %d, exceeds its share 4", tn, n)
		}
	}
	if s := c.Stats(); s.InFlight != int(admitted) {
		t.Fatalf("in-flight %d disagrees with admitted %d", s.InFlight, admitted)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
