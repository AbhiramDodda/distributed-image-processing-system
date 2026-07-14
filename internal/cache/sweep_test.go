package cache

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The sweep replays synthetic access traces over the REAL object catalogue
// (keys + byte sizes) captured from the local MinIO demo dataset, and reports
// LRU vs Clock hit rates across access patterns and cache budgets. It exercises
// the actual policy implementations; only the disk/fetch layer is modelled away,
// since a hit/miss is fully determined by (trace, sizes, budget, policy).
//
// Point PETABYTE_CACHE_MANIFEST at a TSV of "<size>\t<key>" lines (one per
// object) to run it; the test self-skips otherwise so it never gates CI:
//
//	find train -type f -name part.1 -printf '%s\t%p\n' \
//	  | sed -E 's#/[0-9a-f-]{36}/part\.1##' > manifest.tsv
//	PETABYTE_CACHE_MANIFEST=manifest.tsv go test -run TestPolicy_sweep -v ./internal/cache/

type object struct {
	key string
	size int64
}

func loadManifest(t *testing.T) []object {
	t.Helper()
	path := os.Getenv("PETABYTE_CACHE_MANIFEST")
	if path == "" {
		t.Skip("set PETABYTE_CACHE_MANIFEST to a '<size>\\t<key>' TSV to run the real-data sweep")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer f.Close()
	var objs []object
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			continue
		}
		size, err := strconv.ParseInt(line[:tab], 10, 64)
		if err != nil {
			continue
		}
		objs = append(objs, object{key: line[tab+1:], size: size})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan manifest: %v", err)
	}
	// Sort by key so the working-set subset and trace generation are deterministic
	// regardless of filesystem walk order.
	sort.Slice(objs, func(i, j int) bool { return objs[i].key < objs[j].key })
	return objs
}

// simulate replays trace against policy under a byte budget, mirroring
// Cache.Get -> admit -> evictLocked precisely (miss: Add then reclaim; hit:
// Access). It returns the observed hit rate as a percentage.
func simulate(policy EvictionPolicy, sizes map[string]int64, budget int64, trace []string) float64 {
	resident := make(map[string]int64, len(trace))
	var cur int64
	var hits, misses int
	for _, key := range trace {
		if _, ok := resident[key]; ok {
			policy.Access(key)
			hits++
			continue
		}
		misses++
		sz := sizes[key]
		resident[key] = sz
		policy.Add(key)
		cur += sz
		for cur > budget && policy.Len() > 1 {
			id, ok := policy.Victim()
			if !ok {
				break
			}
			policy.Remove(id)
			cur -= resident[id]
			delete(resident, id)
		}
	}
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

// --- trace generators over a fixed working set of keys ---

func zipfTrace(keys []string, n int, s float64) []string {
	rng := rand.New(rand.NewSource(42))
	z := rand.NewZipf(rng, s, 1, uint64(len(keys)-1))
	tr := make([]string, n)
	for i := range tr {
		tr[i] = keys[z.Uint64()]
	}
	return tr
}

func uniformTrace(keys []string, n int) []string {
	rng := rand.New(rand.NewSource(42))
	tr := make([]string, n)
	for i := range tr {
		tr[i] = keys[rng.Intn(len(keys))]
	}
	return tr
}

// loopScanTrace walks the working set in order, repeatedly -- the classic
// scan/epoch pattern where a working set larger than the cache defeats recency
// (LRU evicts exactly the block it is about to reuse next pass).
func loopScanTrace(keys []string, passes int) []string {
	tr := make([]string, 0, len(keys)*passes)
	for range passes {
		tr = append(tr, keys...)
	}
	return tr
}

// hotScanTrace models the workload scan resistance is actually for: a small hot
// set that is reused constantly, interleaved with a long one-shot scan over the
// cold remainder. Each step is either a hot re-read (prob ~0.5) or the next cold
// object in a linear sweep. LRU/Clock let the scan evict the hot set by blind
// recency; S3-FIFO parks single-touch cold objects in the small/ghost queues and
// keeps the reused hot set in main. hot = size of the hot set (first `hot` keys);
// the scan ranges over the rest.
func hotScanTrace(keys []string, hot, n int) []string {
	if hot < 1 {
		hot = 1
	}
	if hot > len(keys) {
		hot = len(keys)
	}
	rng := rand.New(rand.NewSource(42))
	cold := keys[hot:]
	tr := make([]string, n)
	scan := 0
	for i := range tr {
		if len(cold) == 0 || rng.Intn(2) == 0 {
			tr[i] = keys[rng.Intn(hot)]
		} else {
			tr[i] = cold[scan%len(cold)]
			scan++
		}
	}
	return tr
}

func TestPolicy_sweep(t *testing.T) {
	objs := loadManifest(t)
	// Fixed working set: first W objects by sorted key (spans many shards, since
	// keys sort as train/<shard>/<name> and shards interleave classes).
	const W = 8000
	if len(objs) > W {
		objs = objs[:W]
	}
	keys := make([]string, len(objs))
	sizes := make(map[string]int64, len(objs))
	var wsBytes int64
	for i, o := range objs {
		keys[i] = o.key
		sizes[o.key] = o.size
		wsBytes += o.size
	}
	t.Logf("working set: %d objects, %.1f MiB total (avg %d B/object)",
		len(keys), float64(wsBytes)/(1<<20), wsBytes/int64(len(keys)))

	const accesses = 80000
	type pattern struct {
		name string
		trace []string
	}
	patterns := []pattern{
		{"zipf s=1.05", zipfTrace(keys, accesses, 1.05)},
		{"zipf s=1.2", zipfTrace(keys, accesses, 1.2)},
		{"zipf s=1.5", zipfTrace(keys, accesses, 1.5)},
		{"uniform", uniformTrace(keys, accesses)},
		{"loop-scan", loopScanTrace(keys, 10)},
		{"hot+scan", hotScanTrace(keys, 200, accesses)},
	}
	budgetFracs := []float64{0.05, 0.10, 0.20, 0.40}

	type namedPolicy struct {
		name string
		make func() EvictionPolicy
	}
	policies := []namedPolicy{
		{"LRU", NewLRU},
		{"Clock", NewClock},
		{"S3FIFO", NewS3FIFO},
	}

	// Header: pattern | budget | one column per policy | best.
	var hdr strings.Builder
	fmt.Fprintf(&hdr, "%-12s %7s", "pattern", "budget")
	for _, pol := range policies {
		fmt.Fprintf(&hdr, " %9s", pol.name+"%")
	}
	fmt.Fprintf(&hdr, "  %-8s", "best")
	t.Logf("%s", hdr.String())
	t.Logf("%s", strings.Repeat("-", hdr.Len()))

	for _, p := range patterns {
		for _, f := range budgetFracs {
			budget := int64(float64(wsBytes) * f)
			var row strings.Builder
			fmt.Fprintf(&row, "%-12s %6.0f%%", p.name, f*100)
			best, bestRate := "", -1.0
			for _, pol := range policies {
				rate := simulate(pol.make(), sizes, budget, p.trace)
				fmt.Fprintf(&row, " %8.1f%%", rate)
				if rate > bestRate {
					best, bestRate = pol.name, rate
				}
			}
			fmt.Fprintf(&row, "  %-8s", best)
			t.Logf("%s", row.String())
		}
	}
}
