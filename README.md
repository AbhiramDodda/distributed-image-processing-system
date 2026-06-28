# Petabyte-Scale Image Processing Platform

A cloud-native distributed platform for storing petabyte-scale image datasets and running independent algorithms against the same data concurrently. Compute is dispatched to where data lives — data never moves.

## What it does

Multiple researchers submit algorithms that run simultaneously against the same image dataset. The platform handles sharding, scheduling, parallelism, and fault tolerance. Each researcher sees only their own results.

## Architecture

```
  REST clients / CLI
        |
  +-----------+       +------------+       +------------+
  |  cmd/server|       |cmd/coordinator|     | cmd/worker |
  |  :8080    |       |  :8090     |       |  :9001+    |
  | Level 1   |       |  Level 2   |       |  Level 2   |
  +-----------+       +------+-----+       +------+-----+
        |                    |                    |
  +-----+--------+   consistent   poll for tasks  |
  | metadata.db  |   hash ring    <--------------+
  | (SQLite/WAL) |   assigns shard to worker
  +--------------+
        |
  +-----+--------+
  | MinIO / S3   |
  | 256 shards   |
  +--------------+
```

## Implemented Levels

### Level 1 - Object Storage and Data Layout (complete)

- Hash-prefix sharding: SHA-256 of filename -> 2-hex prefix -> 256 independent S3 partitions -> ~896,000 req/s ceiling
- SQLite metadata index (WAL mode) for shard manifests and label search
- Parallel ingestion pipeline: N workers, bounded queue, SHA-256 checksum, multipart upload for files over 100 MB
- Hot/Warm/Cold/Archive storage tiering based on object age
- HTTP API: stats, shard manifests, label search, cost projection

### Level 2 - Distributed Systems (complete)

- Consistent hash ring with 150 virtual nodes per physical node (~+-5% variance)
- Two-stage failure detector: Active -> Suspect (10s) -> Dead (20s), recovery on any heartbeat
- AP design (CAP theorem): workers keep processing during partition; duplicate task execution is acceptable and deduplicated by output key
- Job scheduler: submit jobs, one task per shard, ring-based locality preference, automatic rebalancing when a worker dies
- Pull model: workers poll the coordinator for tasks (`GET /v1/tasks/poll?worker=ID`)

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
```

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

## API Reference

### Level 1 Server (:8080)

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness probe |
| GET | `/v1/stats?dataset=train` | Total images, bytes, shards |
| GET | `/v1/shards?dataset=train` | Per-shard distribution |
| GET | `/v1/shards/{shard}/manifest?dataset=train` | Work manifest for one shard |
| GET | `/v1/search?label=cat&dataset=train&limit=100` | Label search |
| POST | `/v1/tiering/estimate` | Storage cost projection (body: map of tier -> bytes) |

### Level 2 Coordinator (:8090)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/cluster/register` | Worker registers |
| POST | `/v1/cluster/heartbeat` | Worker heartbeat |
| GET | `/v1/cluster/nodes` | All nodes and states |
| GET | `/v1/cluster/ring` | Ring node count and vnode distribution |
| POST | `/v1/jobs` | Submit a job |
| GET | `/v1/jobs` | List all jobs |
| GET | `/v1/jobs/{id}` | Job status and progress |
| GET | `/v1/tasks/poll?worker=ID` | Worker polls for next task |
| POST | `/v1/tasks/{id}/start` | Worker confirms task started |
| POST | `/v1/tasks/{id}/result` | Worker reports completion or failure |

## Sharding

Object keys follow the pattern `{dataset}/{shard}/{filename}`.

```
SHA-256("cat_007842.jpg") -> a3f1...  -> train/a3/cat_007842.jpg
SHA-256("dog_002341.jpg") -> 7fc2...  -> train/7f/dog_002341.jpg
```

256 partitions x 3,500 req/s per S3 prefix = ~896,000 req/s ceiling.

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
| 3 | Kubernetes + Ray compute orchestration, GPU scheduling, auto-scaling | Planned |
| 4 | Sandboxed algorithm execution (gVisor), resource limits, algorithm registry | Planned |
| 5 | Apache Arrow pipelines, WAL checkpointing, Parquet output, phi accrual FD | Planned |
| 6 | gRPC API, OAuth2/OIDC, per-tenant quotas, Raft coordinator HA | Planned |
