package cache

import "testing"

// TestClockPolicy_secondChance pins the defining behaviour: an id touched since
// the hand last passed it survives one extra eviction (its reference bit buys a
// second chance) while an untouched neighbour is reclaimed.
func TestClockPolicy_secondChance(t *testing.T) {
	p := newClock()
	p.Add("a") // all three arrive with the reference bit set
	p.Add("b")
	p.Add("c")

	// First eviction: the sweep starts at "a", clears a/b/c on the way round, and
	// wraps to evict "a" (now clear). b and c are left with cleared bits.
	if id, ok := p.Victim(); !ok || id != "a" {
		t.Fatalf("first victim = %q,%v; want a,true", id, ok)
	}
	p.Remove("a")

	// Touch b: its bit is set again, so the next sweep skips it and evicts c.
	p.Access("b")
	if id, ok := p.Victim(); !ok || id != "c" {
		t.Fatalf("second victim = %q,%v; want c,true (b earned a second chance)", id, ok)
	}
}

// TestClockPolicy_emptyAndSingle covers the degenerate ends: no victim when
// empty, and a lone entry is selectable so the Cache's Len()>1 guard is the only
// thing protecting a solitary oversized object.
func TestClockPolicy_emptyAndSingle(t *testing.T) {
	p := newClock()
	if _, ok := p.Victim(); ok {
		t.Fatal("empty policy returned a victim")
	}
	p.Add("solo")
	if id, ok := p.Victim(); !ok || id != "solo" {
		t.Fatalf("single-entry victim = %q,%v; want solo,true", id, ok)
	}
	p.Remove("solo")
	if p.Len() != 0 {
		t.Fatalf("Len after removing last = %d, want 0", p.Len())
	}
	if _, ok := p.Victim(); ok {
		t.Fatal("victim returned after removing the last entry")
	}
}

// newClock is the concrete constructor for white-box policy tests.
func newClock() *clockPolicy { return NewClock().(*clockPolicy) }
