package cache

import "container/list"

// clockPolicy is the CLOCK (a.k.a. second-chance) approximation of LRU. Every
// resident id carries a single reference bit and sits on a circular list; a
// hand sweeps the circle looking for a victim. On a hit we merely set the id's
// reference bit (an O(1) flag write, no list splice as LRU needs); when a victim
// is required the hand advances, clearing set bits and granting each such id a
// second chance, and evicts the first id it finds with the bit already clear.
//
// Why bother: Access is the hot path (one per cache hit), and Clock's Access is
// a plain bit set with no pointer surgery and no contention on list head/tail —
// so under a read-heavy, high-hit-rate workload it approximates LRU's decisions
// while touching far less shared structure. The trade is fidelity: Clock only
// approximates recency, so on some patterns its hit rate trails true LRU. The
// comparison harness (policy_compare_test.go) exists to measure exactly that.
type clockEntry struct {
	id string
	ref bool
}

type clockPolicy struct {
	ring *list.List // circular sweep order; values are *clockEntry
	els map[string]*list.Element
	hand *list.Element // next entry the sweep will inspect
}

// NewClock returns a CLOCK (second-chance) eviction policy.
func NewClock() EvictionPolicy {
	return &clockPolicy{ring: list.New(), els: make(map[string]*list.Element)}
}

func (p *clockPolicy) Add(id string) {
	if el, ok := p.els[id]; ok {
		el.Value.(*clockEntry).ref = true
		return
	}
	// New entries arrive with the reference bit set, so a freshly admitted object
	// survives at least until the hand reaches it once.
	p.els[id] = p.ring.PushBack(&clockEntry{id: id, ref: true})
}

func (p *clockPolicy) Access(id string) {
	if el, ok := p.els[id]; ok {
		el.Value.(*clockEntry).ref = true
	}
}

func (p *clockPolicy) Victim() (string, bool) {
	if p.ring.Len() == 0 {
		return "", false
	}
	if p.hand == nil {
		p.hand = p.ring.Front()
	}
	// Sweep: an id whose bit is set gets it cleared and is skipped (its second
	// chance); the first id already clear is the victim. Terminates in at most two
	// passes since a full pass clears every bit.
	for {
		e := p.hand.Value.(*clockEntry)
		if !e.ref {
			return e.id, true
		}
		e.ref = false
		p.hand = p.advance(p.hand)
	}
}

func (p *clockPolicy) Remove(id string) {
	el, ok := p.els[id]
	if !ok {
		return
	}
	// Keep the hand valid: if it points at the id being removed, step it forward
	// first (to nil when this was the last entry).
	if p.hand == el {
		if next := p.advance(el); next == el {
			p.hand = nil
		} else {
			p.hand = next
		}
	}
	p.ring.Remove(el)
	delete(p.els, id)
}

func (p *clockPolicy) Len() int {
	return p.ring.Len()
}

// advance returns the element after el, wrapping past the list end back to the
// front so the sweep is circular.
func (p *clockPolicy) advance(el *list.Element) *list.Element {
	if next := el.Next(); next != nil {
		return next
	}
	return p.ring.Front()
}
