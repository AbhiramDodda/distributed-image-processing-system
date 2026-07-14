package cache

import "container/list"

// s3fifo is the S3-FIFO eviction policy (Yang et al., SOSP 2023, "FIFO queues
// are all you need for cache eviction"): a scan-resistant discipline built from
// three FIFO queues instead of an LRU list.
//
//   - small (S): every newly admitted id enters here. Sized to ~10% of residents.
//   - main  (M): the long-lived set; only objects proven to be reused reach it.
//   - ghost (G): fingerprints (ids only, no data) of objects evicted from small,
//     bounded to roughly the resident count.
//
// Why it resists scans: a one-shot scan floods small with objects that are never
// reused. Each leaves small with its reference count still zero, so it is evicted
// straight to ghost and NEVER enters main -- the scan cannot displace the hot
// working set living in main. An object is promoted to main only by being reused
// while in small, or by being re-requested while its fingerprint is still in
// ghost (a "we've seen this before" signal). That two-strike admission is exactly
// what plain LRU/Clock lack: they evict by blind recency, so a scan long enough
// to exceed the cache evicts the hot set it is about to reuse (see the loop-scan
// and hot+scan rows in the sweep).
type s3node struct {
	id string
	freq int8 // saturating reference count, 0..3
	el *list.Element // this node's element in whichever queue holds it
	inMain bool
}

type s3fifo struct {
	small *list.List // FIFO of *s3node; probationary
	main *list.List // FIFO of *s3node; protected
	nodes map[string]*s3node // resident objects (small ∪ main)
	ghost *list.List // FIFO of string ids; recently evicted from small
	ghostSet map[string]*list.Element
	ghostCap int // running peak of resident count; bounds the ghost queue
}

// NewS3FIFO returns a scan-resistant S3-FIFO eviction policy.
func NewS3FIFO() EvictionPolicy {
	return &s3fifo{
		small: list.New(),
		main: list.New(),
		nodes: make(map[string]*s3node),
		ghost: list.New(),
		ghostSet: make(map[string]*list.Element),
	}
}

func (p *s3fifo) Add(id string) {
	if nd, ok := p.nodes[id]; ok { // defensive: a re-Add behaves as a touch
		p.bump(nd)
		return
	}
	if el, ok := p.ghostSet[id]; ok {
		// Seen and evicted before -> admit straight into main (freq 0): a repeat
		// request is the signal that earns a spot past the scan-shield.
		p.ghost.Remove(el)
		delete(p.ghostSet, id)
		nd := &s3node{id: id, inMain: true}
		nd.el = p.main.PushBack(nd)
		p.nodes[id] = nd
	} else {
		nd := &s3node{id: id}
		nd.el = p.small.PushBack(nd)
		p.nodes[id] = nd
	}
	if n := len(p.nodes); n > p.ghostCap {
		p.ghostCap = n
	}
}

func (p *s3fifo) Access(id string) {
	if nd, ok := p.nodes[id]; ok {
		p.bump(nd)
	}
}

func (p *s3fifo) bump(nd *s3node) {
	if nd.freq < 3 {
		nd.freq++
	}
}

// Victim runs the S3-FIFO reclamation until exactly one resident object is
// evicted, and returns it. Promotions (small->main) and second-chance decays
// (within main) mutate the queues but evict nothing, so the loop continues until
// a genuine victim is found. The victim is removed from the resident set here; a
// following Remove(id) is a harmless no-op (see Remove).
func (p *s3fifo) Victim() (string, bool) {
	if len(p.nodes) == 0 {
		return "", false
	}
	for {
		target := (p.small.Len() + p.main.Len()) / 10
		if target < 1 {
			target = 1
		}
		switch {
		case p.small.Len() >= target:
			if id, evicted := p.evictSmall(); evicted {
				return id, true
			}
		case p.main.Len() > 0:
			if id, evicted := p.evictMain(); evicted {
				return id, true
			}
		case p.small.Len() > 0:
			// main is empty but small is under target: still must shed from small.
			if id, evicted := p.evictSmall(); evicted {
				return id, true
			}
		default:
			return "", false
		}
	}
}

// evictSmall pops the oldest small entry. A referenced entry is promoted to main
// (no eviction); an unreferenced one is evicted and its fingerprint recorded in
// ghost. Returns evicted=false when it only promoted.
func (p *s3fifo) evictSmall() (string, bool) {
	front := p.small.Front()
	if front == nil {
		return "", false
	}
	nd := front.Value.(*s3node)
	p.small.Remove(front)
	if nd.freq > 0 {
		nd.inMain = true
		nd.el = p.main.PushBack(nd)
		return "", false
	}
	delete(p.nodes, nd.id)
	p.pushGhost(nd.id)
	return nd.id, true
}

// evictMain pops the oldest main entry. A referenced entry is decayed and
// reinserted (its second chance); an unreferenced one is evicted. Main evictions
// are not recorded in ghost. Returns evicted=false when it only decayed.
func (p *s3fifo) evictMain() (string, bool) {
	front := p.main.Front()
	if front == nil {
		return "", false
	}
	nd := front.Value.(*s3node)
	p.main.Remove(front)
	if nd.freq > 0 {
		nd.freq--
		nd.el = p.main.PushBack(nd)
		return "", false
	}
	delete(p.nodes, nd.id)
	return nd.id, true
}

func (p *s3fifo) pushGhost(id string) {
	if _, ok := p.ghostSet[id]; ok {
		return
	}
	p.ghostSet[id] = p.ghost.PushBack(id)
	for p.ghost.Len() > p.ghostCap {
		old := p.ghost.Front()
		if old == nil {
			break
		}
		p.ghost.Remove(old)
		delete(p.ghostSet, old.Value.(string))
	}
}

func (p *s3fifo) Remove(id string) {
	nd, ok := p.nodes[id]
	if !ok {
		return // already evicted by Victim, or never resident
	}
	if nd.inMain {
		p.main.Remove(nd.el)
	} else {
		p.small.Remove(nd.el)
	}
	delete(p.nodes, id)
}

func (p *s3fifo) Len() int {
	return len(p.nodes)
}
