package cache

import "testing"

// s3 is the concrete constructor for white-box policy tests.
func s3() *s3fifo { return NewS3FIFO().(*s3fifo) }

// TestS3FIFO_protectsHotSetFromScan is the headline claim: on a hot-set +
// interleaved scan trace under a cache too small to hold the scan, S3-FIFO keeps
// the reused hot set resident where plain LRU lets the scan flush it. We drive
// the trace through the shared simulate() harness against both policies and
// assert S3-FIFO's hit rate strictly beats LRU's.
func TestS3FIFO_protectsHotSetFromScan(t *testing.T) {
	const (
		total = 400 // working set: keys[0:hot] hot, the rest cold scan fodder
		hot = 8
		budget = 20 // objects; far smaller than the cold scan span
	)
	keys := make([]string, total)
	sizes := make(map[string]int64, total)
	for i := range keys {
		keys[i] = string(rune('A'+i/26)) + string(rune('a'+i%26)) + itoa(i)
		sizes[keys[i]] = 1 // unit objects -> byte budget == object budget
	}
	trace := hotScanTrace(keys, hot, 20000)

	lru := simulate(NewLRU(), sizes, budget, trace)
	s3f := simulate(NewS3FIFO(), sizes, budget, trace)
	t.Logf("hot+scan hit rate: LRU=%.1f%% S3FIFO=%.1f%% (delta %+.1f pp)", lru, s3f, s3f-lru)

	if s3f <= lru {
		t.Fatalf("S3-FIFO did not beat LRU on hot+scan: S3FIFO=%.1f%% LRU=%.1f%%", s3f, lru)
	}
}

// TestS3FIFO_ghostPromotesOnSecondRequest pins the two-strike admission: an id
// that passes once through small (freq 0) leaves only a ghost fingerprint and is
// NOT resident; requesting it again while the fingerprint survives admits it
// straight into main.
func TestS3FIFO_ghostPromotesOnSecondRequest(t *testing.T) {
	p := s3()
	p.Add("x") // enters small, freq 0
	// Evict it out of small; unreferenced -> goes to ghost, leaves residency.
	if id, ok := p.Victim(); !ok || id != "x" {
		t.Fatalf("victim = %q,%v; want x,true", id, ok)
	}
	if _, resident := p.nodes["x"]; resident {
		t.Fatal("x should have left the resident set (only a ghost fingerprint remains)")
	}
	if _, ghosted := p.ghostSet["x"]; !ghosted {
		t.Fatal("x should have a ghost fingerprint after eviction from small")
	}
	// Second request while ghosted: admit straight to main.
	p.Add("x")
	nd, ok := p.nodes["x"]
	if !ok {
		t.Fatal("x not resident after second request")
	}
	if !nd.inMain {
		t.Fatal("x should have been promoted into main via its ghost fingerprint")
	}
	if _, stillGhost := p.ghostSet["x"]; stillGhost {
		t.Fatal("ghost fingerprint should be consumed on promotion")
	}
}

// itoa is a tiny base-10 formatter to keep the test key generation dependency-free.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
