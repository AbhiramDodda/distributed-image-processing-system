package cache

import "container/list"

// EvictionPolicy decides the order in which cached objects are reclaimed. It
// tracks only object ids; the Cache owns byte accounting and the on-disk files.
// Every method is invoked with the Cache lock held, so implementations need no
// synchronization of their own.
//
// The split lets the reclamation discipline be swapped (LRU vs Clock, below)
// without touching the fetch/single-flight/persistence machinery in the Cache.
type EvictionPolicy interface {
	// Add registers a newly admitted id as resident (and most-preferred to keep).
	Add(id string)
	// Access records a hit on an already-resident id.
	Access(id string)
	// Victim names the next id to evict without removing it, or ok=false when the
	// policy is empty. Selection may mutate internal bookkeeping (e.g. Clock's
	// reference bits) as a side effect.
	Victim() (id string, ok bool)
	// Remove drops id from the policy after its object has been evicted.
	Remove(id string)
	// Len reports the number of resident ids.
	Len() int
}

// lruPolicy is exact least-recently-used order backed by a doubly linked list:
// the front is most-recently-used, the back is the eviction candidate. Access
// and Add are O(1) list splices; Victim reads the tail. This reproduces the
// cache's original behaviour precisely.
type lruPolicy struct {
	ll *list.List // front = MRU, back = eviction candidate; values are ids
	els map[string]*list.Element
}

// NewLRU returns a least-recently-used eviction policy.
func NewLRU() EvictionPolicy {
	return &lruPolicy{ll: list.New(), els: make(map[string]*list.Element)}
}

func (p *lruPolicy) Add(id string) {
	if el, ok := p.els[id]; ok {
		p.ll.MoveToFront(el)
		return
	}
	p.els[id] = p.ll.PushFront(id)
}

func (p *lruPolicy) Access(id string) {
	if el, ok := p.els[id]; ok {
		p.ll.MoveToFront(el)
	}
}

func (p *lruPolicy) Victim() (string, bool) {
	back := p.ll.Back()
	if back == nil {
		return "", false
	}
	return back.Value.(string), true
}

func (p *lruPolicy) Remove(id string) {
	if el, ok := p.els[id]; ok {
		p.ll.Remove(el)
		delete(p.els, id)
	}
}

func (p *lruPolicy) Len() int {
	return p.ll.Len()
}
