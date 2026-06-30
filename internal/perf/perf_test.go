package perf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/config"
	"github.com/abhiramd/petabyte-platform/internal/coordinator"
	"github.com/abhiramd/petabyte-platform/internal/metadata"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
	"github.com/abhiramd/petabyte-platform/internal/storage"
)

var discardLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// ---- latency / throughput helpers ----

type latencyStats struct {
	name       string
	samples    int
	min        time.Duration
	p50        time.Duration
	p90        time.Duration
	p95        time.Duration
	p99        time.Duration
	p100       time.Duration
	mean       time.Duration
	throughput float64 // ops/sec, sequential
}

// pct uses the nearest-rank method on a pre-sorted slice.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// measure runs fn n times sequentially, timing each call, and returns stats.
func measure(name string, n int, fn func(i int)) latencyStats {
	ds := make([]time.Duration, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		fn(i)
		ds[i] = time.Since(t0)
	}
	total := time.Since(start)

	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })

	return latencyStats{
		name:       name,
		samples:    n,
		min:        ds[0],
		p50:        pct(ds, 50),
		p90:        pct(ds, 90),
		p95:        pct(ds, 95),
		p99:        pct(ds, 99),
		p100:       pct(ds, 100),
		mean:       sum / time.Duration(n),
		throughput: float64(n) / total.Seconds(),
	}
}

// throughputConcurrent measures aggregate ops/sec across `workers` goroutines.
func throughputConcurrent(workers, perWorker int, fn func()) float64 {
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				fn()
			}
		}()
	}
	wg.Wait()
	return float64(workers*perWorker) / time.Since(start).Seconds()
}

// ---- the report ----

// TestPerfReport prints a resume/README-ready performance table.
// Run: go test ./internal/perf/ -run TestPerfReport -v
func TestPerfReport(t *testing.T) {
	if testing.Short() {
		t.Skip("perf report skipped in -short mode")
	}

	var rows []latencyStats
	var notes []string

	// 1. Sharding: SHA-256 hash-prefix key computation
	rows = append(rows, measure("ShardKey (SHA-256 shard routing)", 200_000, func(i int) {
		_ = storage.ShardKey(fmt.Sprintf("image_%09d.jpg", i))
	}))
	shardTput := throughputConcurrent(8, 200_000, func() {
		_ = storage.ShardKey("image_000123456.jpg")
	})
	notes = append(notes, fmt.Sprintf("ShardKey concurrent throughput (8 goroutines): %s ops/sec", humanCount(shardTput)))

	// 2. Consistent hash ring lookup (100-node cluster, 15,000 vnodes)
	ring := cluster.NewRing(150)
	for i := 0; i < 100; i++ {
		ring.Add(fmt.Sprintf("worker-%d", i))
	}
	rows = append(rows, measure("Ring.Lookup (100 nodes, 15k vnodes)", 200_000, func(i int) {
		_, _ = ring.Lookup(fmt.Sprintf("shard-key-%d", i))
	}))
	ringTput := throughputConcurrent(8, 200_000, func() {
		_, _ = ring.Lookup("some-shard-key")
	})
	notes = append(notes, fmt.Sprintf("Ring.Lookup concurrent throughput (8 goroutines): %s lookups/sec", humanCount(ringTput)))

	// 3. Scheduler task dispatch (in-process PollTasks)
	rows = append(rows, schedulerDispatchStats(t))

	// 4. Metadata index (real SQLite, WAL mode)
	insertStats, queryStats := metadataStats(t)
	rows = append(rows, insertStats, queryStats)

	// 5. Coordinator end-to-end HTTP (task poll over the wire via httptest)
	rows = append(rows, coordinatorHTTPStats(t))

	printHuman(t, rows, notes)
	printMarkdown(t, rows, notes)
}

func schedulerDispatchStats(t *testing.T) latencyStats {
	t.Helper()
	ring := cluster.NewRing(150)
	const workers = 16
	for i := 0; i < workers; i++ {
		ring.Add(fmt.Sprintf("w%d", i))
	}
	s := scheduler.New(ring, 0, discardLog)

	// Submit enough jobs to have a large pending queue (8 x 256 = 2048 tasks).
	const jobs = 8
	for j := 0; j < jobs; j++ {
		s.Submit(scheduler.SubmitJobRequest{Dataset: "train", Algorithm: "resnet"})
	}
	const total = jobs * 256

	workerIDs := make([]string, workers)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("w%d", i)
	}

	return measure("Scheduler.PollTasks (in-process dispatch)", total, func(i int) {
		_, _ = s.PollTasks(workerIDs[i%workers])
	})
}

func metadataStats(t *testing.T) (latencyStats, latencyStats) {
	t.Helper()
	dir := t.TempDir()
	idx, err := metadata.Open(filepath.Join(dir, "perf.db"))
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { idx.Close() })
	ctx := context.Background()

	const inserts = 10_000
	insertStats := measure("Metadata.Insert (SQLite WAL)", inserts, func(i int) {
		fn := fmt.Sprintf("img_%08d.jpg", i)
		_ = idx.Insert(ctx, metadata.DataRecord{
			ID:        fmt.Sprintf("id-%d", i),
			Filename:  fn,
			S3Key:     storage.ObjectKey("train", fn),
			Shard:     storage.ShardKey(fn),
			Dataset:   "train",
			SizeBytes: 4096,
			Labels:    []string{"cat", "animal"},
			Tier:      storage.TierHot,
			IndexedAt: time.Now(),
		})
	})

	// Query latency: fetch a shard manifest (indexed lookup over 10k rows).
	shards := storage.AllShards()
	queryStats := measure("Metadata.GetShardManifest (indexed query)", 5_000, func(i int) {
		_, _ = idx.GetShardManifest(ctx, shards[i%len(shards)], "train")
	})

	return insertStats, queryStats
}

func coordinatorHTTPStats(t *testing.T) latencyStats {
	t.Helper()
	cfg := &config.Config{Coordinator: config.CoordinatorConfig{
		SuspectTimeout: 10 * time.Second,
		DeadTimeout:    20 * time.Second,
		VnodesPerNode:  150,
		TaskMaxRetries: 0,
	}}
	coord := coordinator.New(cfg, discardLog)
	mux := http.NewServeMux()
	coordinator.NewAPI(coord).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Register a worker and queue 8 jobs (2048 tasks).
	post(srv.URL+"/v1/cluster/register", cluster.RegisterRequest{ID: "w1", Address: "x"})
	const jobs = 8
	for j := 0; j < jobs; j++ {
		post(srv.URL+"/v1/jobs", scheduler.SubmitJobRequest{Dataset: "train", Algorithm: "resnet"})
	}
	const total = jobs * 256

	pollURL := srv.URL + "/v1/tasks/poll?worker=w1"
	return measure("Coordinator poll (end-to-end HTTP)", total, func(i int) {
		resp, err := http.Get(pollURL)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func post(url string, v any) {
	b, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// ---- formatting ----

func humanCount(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func printHuman(t *testing.T, rows []latencyStats, notes []string) {
	t.Helper()
	fmt.Println()
	fmt.Println("PERFORMANCE REPORT")
	fmt.Printf("Go %s  |  GOMAXPROCS=%d  |  %s\n", goVersion(), maxProcs(), time.Now().Format("2006-01-02"))
	fmt.Println("Single-node, in-process (no network/MinIO/K8s). Per-op timing includes ~40ns timer overhead.")
	fmt.Println()
	fmt.Printf("%-44s %8s %8s %8s %8s %8s %8s %14s\n",
		"operation", "p50", "p90", "p95", "p99", "p100", "mean", "throughput/s")
	for _, r := range rows {
		fmt.Printf("%-44s %8s %8s %8s %8s %8s %8s %14s\n",
			r.name, r.p50, r.p90, r.p95, r.p99, r.p100, r.mean, humanCount(r.throughput))
	}
	fmt.Println()
	for _, n := range notes {
		fmt.Println("  - " + n)
	}
	fmt.Println()
}

func printMarkdown(t *testing.T, rows []latencyStats, notes []string) {
	t.Helper()
	fmt.Println("--- README markdown (copy below) ---")
	fmt.Println()
	fmt.Println("| Operation | p50 | p90 | p95 | p99 | p100 | Throughput |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, r := range rows {
		fmt.Printf("| %s | %s | %s | %s | %s | %s | %s/s |\n",
			r.name, r.p50, r.p90, r.p95, r.p99, r.p100, humanCount(r.throughput))
	}
	fmt.Println()
	for _, n := range notes {
		fmt.Println("- " + n)
	}
	fmt.Println()
}

func goVersion() string { return runtime.Version() }

func maxProcs() int { return runtime.GOMAXPROCS(0) }

// ---- standard Go benchmarks (go test -bench=. -benchmem) ----

func BenchmarkShardKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = storage.ShardKey("image_000123456.jpg")
	}
}

func BenchmarkRingLookup(b *testing.B) {
	ring := cluster.NewRing(150)
	for i := 0; i < 100; i++ {
		ring.Add(fmt.Sprintf("worker-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ring.Lookup("shard-key")
	}
}

func BenchmarkRingLookupParallel(b *testing.B) {
	ring := cluster.NewRing(150)
	for i := 0; i < 100; i++ {
		ring.Add(fmt.Sprintf("worker-%d", i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = ring.Lookup("shard-key")
		}
	})
}

func BenchmarkSchedulerSubmit(b *testing.B) {
	ring := cluster.NewRing(150)
	ring.Add("w0")
	s := scheduler.New(ring, 0, discardLog)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Submit(scheduler.SubmitJobRequest{
			Dataset:   "train",
			Algorithm: "resnet",
			Shards:    []string{"00", "01", "02", "03"},
		})
	}
}

func BenchmarkMetadataInsert(b *testing.B) {
	dir := b.TempDir()
	idx, err := metadata.Open(filepath.Join(dir, "bench.db"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer idx.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn := fmt.Sprintf("img_%08d.jpg", i)
		idx.Insert(ctx, metadata.DataRecord{
			ID:        fmt.Sprintf("id-%d", i),
			Filename:  fn,
			S3Key:     storage.ObjectKey("train", fn),
			Shard:     storage.ShardKey(fn),
			Dataset:   "train",
			SizeBytes: 4096,
			Tier:      storage.TierHot,
			IndexedAt: time.Now(),
		})
	}
}
