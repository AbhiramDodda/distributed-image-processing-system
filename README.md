# Petabyte-Scale Image Processing Platform

A cloud-native distributed platform for storing petabyte-scale image datasets and running independent algorithms against the same data concurrently. Compute is dispatched to where data lives — data never moves.

## What it does

Multiple researchers submit algorithms that run simultaneously against the same image dataset. The platform handles sharding, scheduling, parallelism, and fault tolerance. Each researcher sees only their own results.

## Architecture

```
  REST clients / CLI
        |
  +----------+       +---------------+       +------------+
  | cmd/server|       | cmd/coordinator|      | cmd/worker |
  |  :8080   |       |    :8090      |       |  :9001+    |
  | Level 1  |       |    Level 2    |       |  Level 2   |
  +----------+       +------+--------+       +------+-----+
       |                    |                       |
  +----+-------+    consistent hash ring    poll for tasks
  | metadata.db|    assigns shard to worker <------+
  | SQLite/WAL |
  +----+-------+
       |
  +----+-------+       +------------------+
  | MinIO / S3 |       | cmd/operator     |
  | 256 shards |       |  K8s operator    |
  +------------+       |  Level 3         |
                       +--------+---------+
                                |
                   creates batch/v1 Jobs
                                |
                       +--------+---------+
                       | Kubernetes       |
                       | GPU worker pods  |
                       +------------------+
```

## Implemented Levels

### Level 1 - Object Storage and Data Layout (complete)

- Hash-prefix sharding: SHA-256 of filename → 2-hex prefix → 256 independent S3 partitions → ~896,000 req/s ceiling
- SQLite metadata index (WAL mode) for shard manifests and label search
- Parallel ingestion pipeline: N workers, bounded queue, SHA-256 checksum, multipart upload for files over 100 MB
- Hot/Warm/Cold/Archive storage tiering based on object age
- HTTP API: stats, shard manifests, label search, cost projection

### Level 2 - Distributed Systems (complete)

- Consistent hash ring with 150 virtual nodes per physical node (~±5% variance)
- Two-stage failure detector: Active → Suspect (10s) → Dead (20s), recovery on any heartbeat
- AP design (CAP theorem): workers keep processing during a partition; duplicate task execution is acceptable and deduplicated by output key
- Job scheduler: submit jobs, one task per shard, ring-based locality preference, automatic rebalancing when a worker dies
- Pull model: workers poll the coordinator for tasks (`GET /v1/tasks/poll?worker=ID`)

### Level 3 - Compute Orchestration (complete)

- K8s operator creates one `batch/v1` Job per task (no k8s.io/client-go dependency — uses the K8s REST API directly)
- Data-locality scheduling: Jobs are placed on nodes that already have the target shard in their NVMe cache (via `preferredDuringSchedulingIgnoredDuringExecution`)
- GPU resource injection: `Config["gpu"]` from the job request maps to `nvidia.com/gpu` resource limits on the pod
- Worker single-task mode: K8s Job pods receive the task assignment via `PETABYTE_TASK_JSON` env var and exit when done
- Ray integration: health check and job submission via the Ray Dashboard REST API for ML workloads
- HPA: `pending_tasks_per_worker` custom metric exposed at `/v1/metrics/pending`, bridged to K8s Custom Metrics API via the Prometheus Adapter
- DaemonSet workers: one pod per node ensures data locality; `CachedShards` in the heartbeat lets the operator know what each node holds

## Prerequisites

- Go 1.22 or later
- MinIO running locally (or AWS S3 credentials)

Start MinIO with Docker:

```sh
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address :9001
```

Create the bucket:

```sh
docker exec <container> mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec <container> mc mb local/petabyte-images
```

## Build

```sh
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker      ./cmd/worker
go build -o bin/server      ./cmd/server
go build -o bin/ingest      ./cmd/ingest
go build -o bin/operator    ./cmd/operator
```

## Testing

Run the full test suite (no external services required):

```sh
go test ./...
```

The suite covers 93 tests across 10 packages and completes in under one second. No Docker or MinIO is needed because:

- Storage tests exercise sharding logic, tier mapping, and the multipart chunk reader (pure functions, no I/O)
- Metadata tests use a real SQLite database in a temp directory
- Coordinator integration tests spin up the full coordinator API in-process via `net/http/httptest`
- K8s watcher and Ray client tests run against fake API servers via `net/http/httptest`
- Scheduler, ring, and membership tests exercise the state machines with real timers and short timeouts

Because the platform's correctness depends on concurrent access to shared state, run the suite under the race detector before any change to the scheduler, ring, or membership packages:

```sh
go test ./... -race
```

The concurrency tests deliberately stress the locks: 16 workers polling 256 tasks simultaneously (asserting no task is ever assigned twice), heartbeats racing the failure detector, and ring churn racing lookups.

To run one package with verbose output:

```sh
go test ./internal/coordinator/... -v
go test ./internal/scheduler/... -v
go test ./internal/cluster/... -v
```

### What the tests cover

| Package | Tests | What is verified |
|---|---|---|
| `internal/storage` | 9 | ShardKey determinism, 2-hex format, all 256 shards reachable, ObjectKey structure, tier → S3 class mapping, multipart chunk reader |
| `internal/cluster` | 21 | Ring empty/single/multi-node, distribution variance (±10%), Remove redistribution, LookupN distinct nodes, concurrent churn under -race; Membership Active→Suspect→Dead transitions, recovery, failure events, concurrent heartbeat/tick |
| `internal/scheduler` | 15 | Submit, poll, start, result, retry, max-retry failure, RebalanceWorker, DrainPending, PendingCount, and concurrent-poll no-double-assignment under -race |
| `internal/metadata` | 12 | Insert/GetShardManifest, SearchByLabel with limit, ShardStats, DatasetStats, LabelCounts, UpdateTier, RecordsByTierAge, durability after reopen |
| `internal/coordinator` | 8 | Full register→submit→poll→complete lifecycle via HTTP, heartbeat metrics, /v1/metrics/pending, /v1/operator/drain |
| `internal/k8s` | 9 | JobName format/determinism, batch/v1 spec structure, GPU resources, node affinity from CachedShards, watcher phase resolution + emit/delete against fake API server |
| `internal/ray` | 7 | Dashboard health check, job submit/get, WaitForJob terminal-state polling and context cancellation |
| `internal/config` | 5 | DefaultConfig sane values, Load with missing file, overrides, malformed YAML |
| `internal/metrics` | 6 | Counter/Gauge/Histogram math, concurrent counter increments under -race, collector snapshot |
| `internal/tiering` | 5 | CostProjection zero/single/ordering/petabyte-scale/linear scaling |

### What requires Docker (not automated yet)

- Ingestion pipeline end-to-end (needs MinIO): use `docker-compose` with the provided `deploy/` manifests
- K8s operator Job creation (needs a cluster): use `kind create cluster` then `kubectl apply -f deploy/`
- Ray job submission (needs Ray Dashboard): use `deploy/ray-cluster.yaml` via KubeRay

## Performance

Benchmarks for the hot paths. All measurements are single-node and in-process
(no network, MinIO, or Kubernetes) so they isolate the platform's own overhead.
Reproduce with:

```sh
go test ./internal/perf/ -run TestPerfReport -v      # latency percentiles + throughput
go test ./internal/perf/ -bench=. -benchmem -run=^$  # ns/op, B/op, allocs/op
```

Environment: 13th Gen Intel Core i7-13620H (16 threads), Go 1.25, Linux.

### Latency percentiles and throughput

| Operation | p50 | p90 | p95 | p99 | Throughput |
|---|---|---|---|---|---|
| SHA-256 hash-prefix shard routing | 448ns | 551ns | 593ns | 773ns | 1.84M/s (single), 23.6M/s (8 goroutines) |
| Consistent-hash ring lookup (100 nodes, 15k vnodes) | 463ns | 512ns | 527ns | 574ns | 1.89M/s (single), 6.16M/s (8 goroutines) |
| Scheduler task dispatch (in-process poll) | 17.6µs | 115.8µs | 162.0µs | 243.3µs | 24.3K/s |
| Metadata insert (SQLite WAL) | 86.9µs | 130.6µs | 151.0µs | 224.4µs | 10.1K/s |
| Metadata shard-manifest query (indexed) | 358.4µs | 472.3µs | 544.9µs | 888.8µs | 2.6K/s |
| Coordinator task-poll, end-to-end HTTP | 86.7µs | 289.6µs | 333.9µs | 453.3µs | 7.2K/s |

p100 (max single sample) is omitted from the table because it is dominated by
occasional GC pauses (e.g. a single 0.5ms outlier among 200k sub-microsecond
ring lookups); the report test prints it for completeness.

### Microbenchmarks (allocations)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `ShardKey` | 207 | 2 | 1 |
| `RingLookup` | 171 | 0 | 0 |
| `RingLookup` (parallel, 16 cores) | 119 | 0 | 0 |
| `SchedulerSubmit` (4-shard job) | 5,378 | 1,857 | 19 |
| `MetadataInsert` | 94,303 | 1,169 | 34 |

The consistent-hash ring performs **zero-allocation** lookups — no garbage is
produced on the task-routing hot path regardless of cluster size.

## Quick Start

Start the Level 1 metadata server:

```sh
bin/server -config configs/server.yaml
```

Start the Level 2 coordinator (port 8090):

```sh
bin/coordinator -config configs/coordinator.yaml
```

Start two workers (different ports):

```sh
bin/worker -config configs/worker.yaml -id worker-1 -port 9001
bin/worker -config configs/worker.yaml -id worker-2 -port 9002
```

## Ingesting Images

Ingest a local directory into the `train` dataset:

```sh
bin/ingest -config configs/server.yaml \
  -dir /path/to/images \
  -dataset train \
  -labels "cat,animal"
```

Progress is printed per worker. Multipart upload activates automatically for files over 100 MB.

## Submitting a Job

```sh
curl -s -X POST http://localhost:8090/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "train",
    "algorithm": "resnet-feature-extractor",
    "config": {"batch_size": "32"}
  }' | jq .
```

Response:

```json
{
  "job_id": "abc-123",
  "total_tasks": 256
}
```

By default a job creates one task per shard (256 tasks for a full dataset). To target specific shards:

```sh
-d '{"dataset": "train", "algorithm": "...", "shards": ["00","01","02"]}'
```

Request GPU resources for a task:

```sh
-d '{"dataset": "train", "algorithm": "...", "config": {"gpu": "1", "memory": "32Gi"}}'
```

## Running in Kubernetes (Level 3)

Apply the manifests in order:

```sh
kubectl apply -f deploy/coordinator.yaml
kubectl apply -f deploy/worker.yaml
kubectl apply -f deploy/ray-cluster.yaml   # requires KubeRay operator
kubectl apply -f deploy/hpa.yaml
```

Start the operator (runs outside the cluster for local dev, or as a Deployment in production):

```sh
bin/operator -config configs/operator.yaml
```

The operator polls `/v1/operator/drain` on the coordinator to get pending tasks and creates one K8s Job per task. Completed Jobs are detected by the watcher and reported back to the coordinator.

## API Reference

### Level 1 Server (:8080)

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness probe |
| GET | `/v1/stats?dataset=train` | Total images, bytes, shards |
| GET | `/v1/shards?dataset=train` | Per-shard distribution |
| GET | `/v1/shards/{shard}/manifest?dataset=train` | Work manifest for one shard |
| GET | `/v1/search?label=cat&dataset=train&limit=100` | Label search |
| GET | `/v1/labels?dataset=train` | Label frequency counts |
| POST | `/v1/tiering/estimate` | Storage cost projection (body: map of tier → bytes) |

### Level 2 Coordinator (:8090)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/cluster/register` | Worker registers with ID and address |
| POST | `/v1/cluster/heartbeat` | Worker heartbeat with current metrics |
| GET | `/v1/cluster/nodes` | All nodes and their states |
| GET | `/v1/cluster/ring` | Ring node count and vnode distribution |
| POST | `/v1/jobs` | Submit a job |
| GET | `/v1/jobs` | List all jobs |
| GET | `/v1/jobs/{id}` | Job status and progress |
| GET | `/v1/tasks/poll?worker=ID` | Worker polls for next task |
| POST | `/v1/tasks/{id}/start` | Worker confirms task started |
| POST | `/v1/tasks/{id}/result` | Worker reports completion or failure |

### Level 3 Operator endpoints (:8090)

| Method | Path | Description |
|---|---|---|
| GET | `/v1/metrics/pending` | `{pending_tasks, active_workers, pending_tasks_per_worker}` |
| POST | `/v1/operator/drain?n=N` | Atomically drain up to N pending tasks for K8s Job creation |

## Sharding

Object keys follow the pattern `{dataset}/{shard}/{filename}`.

```
SHA-256("cat_007842.jpg") → a3f1...  → train/a3/cat_007842.jpg
SHA-256("dog_002341.jpg") → 7fc2...  → train/7f/dog_002341.jpg
```

256 partitions × 3,500 req/s per S3 prefix = ~896,000 req/s ceiling.

## Storage Tiers

| Tier | S3 Class | Approx cost/PB/month |
|---|---|---|
| HOT | STANDARD | $23,000 |
| WARM | STANDARD_IA | $12,500 |
| COLD | GLACIER_IR | $4,000 |
| ARCHIVE | DEEP_ARCHIVE | $990 |

The tiering engine transitions objects based on age (configurable thresholds in `server.yaml`).

## Roadmap

| Level | Description | Status |
|---|---|---|
| 1 | Object storage, sharding, metadata, ingestion, tiering | Complete |
| 2 | Cluster membership, hash ring, job scheduler, failure detection | Complete |
| 3 | Kubernetes operator, K8s Job-per-shard, Ray integration, GPU scheduling, HPA | Complete |
| 4 | Sandboxed algorithm execution (gVisor), resource limits, algorithm registry | Planned |
| 5 | Apache Arrow pipelines, WAL checkpointing, Parquet output, phi accrual FD | Planned |
| 6 | gRPC API, OAuth2/OIDC, per-tenant quotas, Raft coordinator HA | Planned |
