# Petabyte-Scale Image Processing Platform

A cloud-native distributed platform for storing petabyte-scale image datasets and running independent algorithms against the same data concurrently. Compute is dispatched to where data lives -- data never moves.

## What it does

Multiple researchers submit algorithms that run simultaneously against the same image dataset. The platform handles sharding, scheduling, parallelism, and fault tolerance. Each researcher sees only their own results.

> **Design rationale:** every significant decision — and the alternatives weighed against it, with pros and cons — is documented in [design.md](design.md).

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

- Hash-prefix sharding: SHA-256 of filename -> 2-hex prefix -> 256 independent S3 partitions -> ~896,000 req/s ceiling
- SQLite metadata index (WAL mode) for shard manifests and label search
- Parallel ingestion pipeline: N workers, bounded queue, SHA-256 checksum, multipart upload for files over 100 MB
- Hot/Warm/Cold/Archive storage tiering based on object age
- HTTP API: stats, shard manifests, label search, cost projection

### Level 2 - Distributed Systems (complete)

- Consistent hash ring with 150 virtual nodes per physical node (~8% per-node load std-dev; ~10-25% peak deviation depending on cluster size)
- Two-stage failure detector: Active -> Suspect (10s) -> Dead (20s), recovery on any heartbeat
- AP design (CAP theorem): workers keep processing during a partition; duplicate task execution is acceptable and deduplicated by output key
- Job scheduler: submit jobs, one task per shard, ring-based locality preference, automatic rebalancing when a worker dies
- Pull model: workers poll the coordinator for tasks (`GET /v1/tasks/poll?worker=ID`)

### Level 3 - Compute Orchestration (complete)

- K8s operator creates one `batch/v1` Job per task (no k8s.io/client-go dependency -- uses the K8s REST API directly)
- Data-locality scheduling: Jobs are placed on nodes that already have the target shard in their NVMe cache (via `preferredDuringSchedulingIgnoredDuringExecution`)
- GPU resource injection: `Config["gpu"]` from the job request maps to `nvidia.com/gpu` resource limits on the pod
- Worker single-task mode: K8s Job pods receive the task assignment via `PETABYTE_TASK_JSON` env var and exit when done
- Ray integration: health check and job submission via the Ray Dashboard REST API for ML workloads
- HPA: `pending_tasks_per_worker` custom metric exposed at `/v1/metrics/pending`, bridged to K8s Custom Metrics API via the Prometheus Adapter
- DaemonSet workers: one pod per node ensures data locality; `CachedShards` in the heartbeat lets the operator know what each node holds

### Level 4 - Sandboxed Algorithm Execution (complete)

- Untrusted user code runs in a gVisor (`runsc`) container with **network mode `none`** -- it can read the mounted input volume and write the output volume, nothing else. The sandbox cannot be disabled by the user's manifest.
- Algorithm package format: a zip containing `Dockerfile`, `main.py`, `requirements.txt`, and `manifest.json` (declares name, version, base image, GPU/memory/timeout, and parallelism mode).
- Registry validates every submission against a per-tenant quota and a base-image allowlist before recording it. `(name, version)` is immutable -- a running job's code can never change under it.
- Content-canonical package digest (hashes sorted file contents, not the zip bytes) so identical code dedupes to one image regardless of how the zip was packed; the digest doubles as the image tag.
- Hard resource limits (`--cpus`, `--memory`, `--gpus`) enforced by the runtime/cgroups; timeout and OOM kills are distinguished so the scheduler can retry vs. fail appropriately.
- `sandbox-runner` sidecar: a single-shot binary that stages a shard into the input volume, runs the algorithm image, collects `result.json`, uploads declared artifacts, and reports back to the coordinator.

### Level 5 - High-Throughput Data Pipelines (complete)

- NVMe-backed object cache: size-bounded LRU over local disk with single-flight on concurrent misses (a stampede of readers triggers one S3 fetch), crash-safe temp-file + atomic-rename writes, and restart adoption of objects already on disk.
- Look-ahead prefetcher: streams shard objects with bounded concurrency (a semaphore caps in-flight fetches at `depth`) while preserving submission order via per-key futures, overlapping S3/NVMe latency with GPU compute (double buffering).
- Write-ahead log for coordinator state: every scheduler mutation is appended (CRC32-framed, fsync'd) and folded into an atomic snapshot by periodic checkpoints. Each record carries a monotonic sequence, so a crash between snapshot-write and log-truncate never double-applies on replay; a torn tail is detected and truncated on recovery. The coordinator restores job/task state on restart -- in-flight jobs survive a crash.
- Phi-accrual failure detector: models the recent heartbeat inter-arrival distribution and emits a continuous suspicion value `phi = -log10(P(late))` instead of a fixed timeout, so a jittery link tolerates longer gaps than a steady one without hand-tuned thresholds.
- Output formats for ML frameworks: TFRecord (masked CRC32C framing, interoperable with `tf.data`), WebDataset tar shards (grouped by sample key, deterministic ordering), Apache Arrow columnar batches (one contiguous buffer per field, with IPC transport), and Parquet result files queryable directly by Athena/Spark/DuckDB.

### Level 6 - Platform API and Multi-Tenancy (complete)

- Per-tenant quota enforcer: a live admission gauge that reserves cpu/memory/gpu/active-job capacity at job-submit time. The check-and-reserve is atomic under one lock across every dimension, so two jobs that each fit alone cannot both slip past a ceiling they jointly exceed; unknown tenants fail closed. Reservations release idempotently, so a double-free never corrupts the count.
- Per-tenant usage ledger: monotonic billing counters (CPU/GPU-seconds, bytes read/written, jobs completed) kept separate from the enforcer -- admission is a rise-and-fall gauge, billing is an ever-growing counter. Charges reserved resource-time (what a tenant denied to others), leaving the price per unit to the billing layer.
- Token-bucket rate limiter: per-tenant buckets with lazy refill (no background goroutine), so a burst of submissions is admitted while the sustained rate is capped and one tenant cannot drain another's allowance.
- Authentication and authorization: local HS256 JWT verification (constant-time signature check, hard-rejects the alg-confusion/"none" attack, exp/nbf with leeway) plus coarse resource:verb RBAC (admin/operator/viewer/worker roles, unknown roles grant nothing).
- gRPC control-plane API: mirrors the HTTP/JSON coordinator API and adds `WatchJob`, a server-streaming RPC that replaces the client poll loop. A unary+stream interceptor chain composes the three pieces above -- authenticate -> authorize (RBAC per method) -> per-tenant rate limit -- before any handler runs. Enabled via `grpc_port` (refuses to start without a `jwt_secret`).
- Raft coordinator HA: wraps the etcd Raft library (leader election, log replication, membership) behind an FSM + transport, retiring the single-coordinator single-point-of-failure. A 3-node cluster elects a leader, replicates committed commands to every FSM, and survives leader failure -- the surviving majority elects a new leader and keeps committing with no split brain. Now driven over a real gRPC transport across separate processes and carrying the exactly-once commit ledger (see [Raft-agreed commit ledger](#raft-agreed-commit-ledger-over-grpc-complete)).

## Beyond the Levels — Differentiating Deep-Dives (in progress)

With the six planned levels complete, the platform is growing a set of harder,
correctness-subtle concurrency problems — the parts that don't reduce to wiring up a
framework. Each one is deliberately paired with an honest account of the gap it does
*not* close.

### Exactly-once result commit via two-phase staging (complete)

A worker never writes to a task's final location directly. It stages its output to a
per-attempt staging key; the coordinator commits by an **idempotent server-side copy**
to the canonical `FinalResultKey(job, shard, range)` and then records the terminal commit
decision. Because the copy is attempt-independent, a task re-run after a crash or a
false-positive failure stages a fresh object, but only the coordinator's single commit
makes any object the visible result. The copy runs **outside** the scheduler lock (a
server-side copy is slow relative to polling) with a done-guard for idempotency under
concurrent duplicate reports.

The commit **decision** is agreed through the **replicated Raft log** (over a real gRPC
transport — see [the commit ledger below](#raft-agreed-commit-ledger-over-grpc-complete))
when a `CommitDecider` is attached: it is agreed exactly once across coordinators (no split-brain),
**fenced by lease generation** (a stale attempt loses to the recorded one), and durable on
a majority — so a failover leader inherits an authoritative record of committed tasks and
never re-dispatches one. Verified end-to-end over real gRPC with leader failover
(`internal/consensus/raftgrpc`, `internal/coordinator/raft_test.go`).

**Side effects made effectively-once (idempotency keys, complete).** Consensus makes the
*output* and the *commit decision* exactly-once, but not arbitrary *side effects*: a crash
*before* the decide re-executes the task, and while its output is unaffected (idempotent
copy), a real algorithm's external side effects would repeat. That window is irreducible —
the fundamental atomic-commit-across-heterogeneous-systems problem (object store + log +
downstream), not something more consensus can fix. So rather than deliver a side effect
*exactly* once, the platform delivers it *at-least*-once under a stable idempotency key and
lets the receiver dedup (at-least-once + idempotent apply = **effectively-once**):

- `scheduler.SideEffectKey` derives a **deterministic idempotency key** per committed unit
  of work from its attempt-, worker-, and generation-independent identity `(job, shard,
  range)` — every retry, rebalance, or failover re-execution derives the *same* key.
- It is propagated into untrusted algorithm code as `PETABYTE_IDEMPOTENCY_KEY` so an
  algorithm can stamp its own downstream writes and have those dedup too.
- For effects the platform mediates, a post-commit `SideEffect` (fired after the decision is
  agreed, before the WAL mark → at-least-once) runs behind an idempotency ledger
  (`internal/effect`); the reference `coordinator.EnableSideEffects` emits an exactly-once
  task-completion event. Tests: `internal/effect`, `internal/scheduler/sideeffect_test.go`,
  `internal/coordinator/effect_test.go`, `internal/sandbox` env propagation.

The only thing that survives re-execution is now *compute* — never a duplicated committed
output or a double-applied keyed side effect.

### Raft-agreed commit ledger over gRPC (complete)

The `internal/consensus` package (etcd Raft: leader election, log replication, PreVote/
CheckQuorum) is wired for genuine **multi-process** HA via a real gRPC transport
(`internal/consensus/raftgrpc`) — three separate coordinator processes replicate `raftpb`
messages over the wire, not just an in-process test network. Each coordinator runs a
`CommitFSM`: a deterministic, fenced ledger of commit decisions (`Apply` keeps the
highest-generation record per task, so replicas converge and stale attempts are rejected).
`ReportResult` closes its terminal commit by proposing to this log and waiting for the entry
to apply (`raftCommitDecider`), re-proposing across a leadership change per Raft's
drop-and-retry contract. The transport is non-blocking by construction: each peer has a
bounded queue drained by its own goroutine, so a slow or dead peer never stalls the Raft run
loop (Raft retransmits what's dropped). Config: `coordinator.raft` (`id`, `port`, `peers`).
Verified with a 3-node localhost gRPC cluster that elects a leader, replicates commits to
every FSM, and **survives leader failover** (`internal/consensus/raftgrpc/transport_test.go`,
`internal/coordinator/raft_test.go`).

### Intra-shard work stealing (complete)

The real imbalance here isn't cross-shard — it's *intra-shard*: one giant shard is a single
task that one worker grinds through alone while others sit idle (the tail-latency problem).
The fix gives each task a half-open range `[RangeStart, RangeEnd)` over the shard's sorted
key list plus a lease. Safety comes from **bounded grants**: `RenewLease` grants only the
next `leaseChunk` items, so a worker's real progress can never exceed `Granted`; the
scheduler therefore steals only the provably-untouched tail `[Granted, RangeEnd)`. A steal
splits that tail into a new sub-task, shrinks the victim's `RangeEnd`, and bumps its lease
generation — the victim learns it was stolen from on its next renewal and winds down
cooperatively at the split point. The invariant `RangeStart ≤ Frontier ≤ Granted ≤ RangeEnd`
holds throughout, and the ranges tile the shard with no gap or overlap. It composes with
exactly-once through range-scoped result keys (a split range writes its own key; an unsplit
whole shard keeps the flat key). The whole loop is worker-driven over a `/v1/tasks/{id}/renew`
endpoint: S3 lists keys lexicographically, so the victim and the thief index into the same
sorted offsets. Verified end-to-end (`TestCoordinator_workStealingOverHTTP`).

**Live demo (`scripts/steal-demo.sh`).** The mechanism is also proven end-to-end against real
MinIO objects. The script concentrates 6,000 images into a single shard (`gen-images -shard`
mines filenames whose hash lands in one shard), sets a small `lease_chunk`, submits a
one-shard job, and starts one busy worker plus one idle worker. A representative run: **53
steals** progressively tiled the shard between the two workers (first steal hands off half the
un-granted tail, `[3200,6000)`; the victim winds down at frontier 400), and **all 6,000 images
were processed exactly once — 3,447 by w0 and 2,553 by w1, summing to 6,000 with no gap and no
double-processing**, which is the whole safety guarantee made observable. A new `lease_chunk`
coordinator config knob (default 1000) tunes steal granularity; the small shards of a uniform
dataset stay unsplittable under the default.

### Runtime concurrency diagnostics (complete)

`internal/diag` is an **opt-in** layer for catching the concurrency bugs ordinary logging
misses. `diag.Mutex` / `diag.RWMutex` are drop-in `sync` replacements that time wait+hold,
track the holder, and maintain a global lock-order graph — an inconsistent acquisition order
(a latent deadlock) is reported the first time it occurs, via a DFS cycle check, *before* it
ever actually hangs. `diag.Assert` logs loudly (never panics) on an invariant violation with
detail, goroutine id, and stack; it's wired into the scheduler's lease-ordering, steal
no-reclaim/contiguity, and generation-monotonicity checks. `GET /debug/diag` returns lock
stats, recent violations, lock-order warnings, and (with `?stacks=1`) a full goroutine dump —
the go-to artifact for a hang. Off by default: one atomic load per lock op, nothing for
asserts. Turn it on with `PETABYTE_DIAG=1`.

**Scope boundary:** this catches *logical* races (invariant assertions) and *deadlocks /
contention* (instrumented locks + goroutine dumps). It does **not** replace `go test -race`
for *data* races — the two are complementary.

### Backpressure & admission control (complete)

Submitting a thousand jobs schedules a quarter-million tasks at once — the scheduler's
working set explodes and tail latency grows without bound. The fix is a bounded
admission layer (`internal/admission`) that **sheds load rather than queuing it**. A global
`MaxInFlight` cap on concurrently-admitted jobs is the backpressure valve: past it, a
submission gets a fast `429` + `Retry-After` instead of joining an unbounded queue. Each
admitted job holds one in-flight slot for its lifetime; the scheduler's job-done hook
releases the slot the instant the job reaches a terminal state. Capacity is partitioned
across tenants by **weight**, so one tenant's burst can't starve another (isolation), and a
job's `Priority` orders dispatch — an idle worker with no local task is handed the
highest-priority pending work.

Why *shed* instead of *queue* — the reasoning is queuing-theoretic, not a hunch:

- **Little's Law (`L = λW`).** With arrival rate `λ` at or above service capacity, an
  unbounded queue's occupancy `L` — and therefore its wait `W` — grows without limit: the
  classic latency collapse of a system that looked healthy at steady state. Bounding
  in-flight work at `L_max` fixes the working set, so `W = L_max / throughput` stays bounded
  *regardless of arrival rate*. Backpressure trades a few rejected submissions for a latency
  ceiling.
- **Erlang-B loss model.** A tenant with a share of `c` slots is an `M/M/c/c` loss system.
  Its blocking probability is the closed form `B(ρ, c) = (ρ^c / c!) / Σ_{k=0..c} ρ^k/k!`,
  where `ρ` is offered load (arrival rate × mean job holding time). So "what fraction of a
  tenant's submissions are shed at load `ρ`?" is a design knob you can compute up front, not
  an emergent surprise under pressure.
- **Sizing `MaxInFlight`.** Set it at the *knee* of the load curve — where the Universal
  Scalability Law's coherency/contention term starts to flatten throughput — not from raw
  CPU/GPU totals. Past the knee, more concurrency buys latency, not work.

**Honest trade-off:** per-tenant hard shares favour *isolation over utilisation* — a quiet
tenant's reserved slots sit idle rather than being lent to a busy neighbour. Work-conserving
weighted fair queuing would reclaim that idle capacity, but only by reintroducing a real
queue (and the unbounded-latency risk it then has to re-bound). The loss-system design is
simpler, predictable, and analyzable in closed form, which is the right default for a
platform whose main multi-tenant job is *not letting one tenant hurt another*.

Config: `max_in_flight_jobs` in `coordinator.yaml` (disabled/unbounded when unset).
Observability: `GET /v1/metrics/admission` (in-flight, lifetime admitted/rejected, per-tenant
load). Verified: `internal/admission` unit tests (fairness, isolation, a concurrent burst
that never exceeds the cap) plus `TestAdmission_shedsThenReadmitsAfterCompletion` end-to-end
over HTTP.

### CLIP image similarity search (complete)

The platform's first *end-to-end algorithm*, not just infrastructure: run CLIP embeddings
over an image corpus, store them columnar as Parquet, and search by nearest neighbour —
find images from a text prompt ("a photo of a dog") or from an example image.

The division of labour is deliberate. **Vector math lives in Go** (`internal/vsearch`): the
corpus loads into one contiguous, L2-normalised `float32` buffer and a query is `N` dot
products with a bounded min-heap for top-k — exact brute-force cosine, `O(N·dim)`. At ≤1M
vectors that's a few hundred MB scanned per query with no index to build, tune, or keep
consistent; an ANN index (HNSW/IVF) only earns its complexity well past that scale. **CLIP
inference lives in Python** (`scripts/clip/`, where torch lives): an offline batch encoder
writes the corpus, and a tiny online sidecar encodes a query text/image on demand (text→image
inherently needs the CLIP text encoder live). The two meet at a **fixed Parquet schema**
(`id`, `dataset`, `vector`) and a **one-line JSON `/encode` contract**, so either side is
swappable — the corpus is queryable from Athena/DuckDB, and the encoder can be any model.

Four query modes, over both a CLI (`cmd/vsearch`) and an HTTP endpoint (`POST /v1/similar`):
`-id` (image→image over a corpus member — needs no encoder, self-excluded), `-vector` (a raw
pre-computed vector), `-text` and `-image` (encoded live by the sidecar). Both surfaces are
opt-in: the endpoint 501s until `server.search.index_key` is set.

**Live demo (`scripts/clip-search-demo.sh`).** Runs the whole pipeline — Python encodes a
corpus to Parquet, Go loads it and answers text/id/vector/HTTP queries. `SYNTHETIC=1` (the
default) uses deterministic concept-clustered stand-in vectors (numpy + pyarrow only, no
torch, no download) to verify the entire Go↔Parquet↔sidecar contract anywhere: a "dog" query
returns dog images at cosine ~0.98 vs ~0.07 cross-concept, and the run asserts the planted
nearest neighbour lands at rank 1. Drop `SYNTHETIC=1` for real open_clip embeddings
(ViT-B/32 `laion2b`, CPU-runnable) over the sample images in `scripts/clip/sample_urls.txt`,
which returns genuinely similar images. Verified: `internal/vsearch` unit tests (k-NN
ordering/tie-break/normalisation-invariance, Parquet round-trip, encoder client) plus the
`/v1/similar` handler tests, all green under `-race`.

**Scored on a real dataset (`scripts/clip-eval-demo.sh`).** Beyond a handful of images, this
downloads a labelled HuggingFace image dataset, embeds it with real CLIP, and measures
text→image retrieval accuracy *through the Go engine* against ground truth — a quantitative
number, not a cherry-picked demo. On the **full 10,000-image CIFAR-10 test set across 10
classes** (1,000 each, random baseline 10%), every class query (`"a photo of a <class>"`)
scored **100% precision@10 and 100% top-1**. Pushing `k` traces the expected curve, computed
by `cmd/vsearch → CLIP sidecar → Go k-NN`: P@100 98.7% → P@500 96.7% → P@1000 86.7% (asking
for exactly the per-class count) → P@2000 49.1% (≈ the 50% ceiling — only 1,000 of any 2,000
results *can* be a given class). Embedding streams in batches (bounded memory, ~1.5 GB for
10k on CPU). `embed_corpus.py --hf-dataset` (any HF image dataset) and `evaluate.py`
(precision@k vs. labels) are the reusable pieces.

### Failure resilience (chaos-tested)

The failure story is only worth as much as its evidence, so `scripts/chaos-demo.sh`
proves it on real datasets: it ingests real images as objects into MinIO (sharded across
all 256 shards), brings up **10 worker nodes**, and runs the same job twice — once with
every worker healthy, once **`kill -9`-ing 3 of the 10 workers mid-flight** (at ~⅓ done).

**At scale — 1.28M real ImageNet objects.** The headline run is the full
`benjamin-paine/imagenet-1k-256x256` train split: **1,281,167 real 256×256 JPEGs (19 GB)**
ingested as objects, then chaos-tested. Killing 3 of 10 workers mid-job:

| Metric | Baseline (healthy) | Chaos (kill 3/10) |
| --- | --- | --- |
| tasks done / total | 256 / 256 | **256 / 256** |
| tasks failed | 0 | **0** |
| tasks **lost** | 0 | **0** |
| tasks rebalanced off dead workers | — | **52** |
| end-to-end job wall time | 284.7 s | 353.2 s (**Δ +68.5 s**) |
| task latency p50 / p95 | 84.9 s / 113.8 s | 85.7 s / 120.9 s |

**Zero tasks lost** despite a third of the fleet dying uncleanly: the phi-accrual detector
(tuned suspect=2s/dead=4s for the demo) marks the silent workers dead, `RebalanceWorker`
requeues their 52 in-flight tasks, and the idempotent `FinalResultKey` makes each re-run
*overwrite* rather than duplicate or drop — every shard completes exactly once. The +68.5 s
tax is the dead-detection window plus whole-shard reprocessing of interrupted tasks, not a
steady-state throughput hit (per-task p50 barely moved).

**Also verified on the 10,000-image CIFAR-10 test set** (same harness, `DATASET=cifar`):
256/256 tasks both runs, 0 failed, 24 rebalanced; latency **2.76 s → 6.69 s** (Δ +3.9 s),
per-task p50 82 → 88 ms. A fast, small correctness check; the ImageNet run is the volume one.

The mechanism is size-independent: content-addressed sharding spreads any corpus across the
256 shards, and the exactly-once commit + rebalance hold per task regardless of total size.
Observed live via `/v1/metrics/tasks` (state counts, `rebalances`, latency percentiles) and
`/v1/cluster/nodes` (per-worker Active→Suspect→Dead).

#### Test environment

All numbers above were measured on a **single laptop**, not a cluster:

| | |
| --- | --- |
| CPU | 13th Gen Intel Core i7-13620H — 10 cores / 16 threads, up to 4.9 GHz |
| RAM | 13.3 GiB, **no swap** |
| Disk | 1 TB KIOXIA NVMe SSD (single local disk; MinIO data + metadata DB all here) |
| GPU | RTX 4060 Laptop present but **unused** — see below |
| OS / runtime | Arch Linux (kernel 6.19), Go 1.26; MinIO single-node, filesystem backend |

**No GPU is involved.** The coordinator, workers, MinIO, and the scan job are pure
CPU + local-NVMe I/O; the discrete GPU sits idle (even the separate CLIP eval runs on CPU).

**Peak memory during the 1.28M-object run is small and flat** — coordinator **~21 MB**,
each worker **~20 MB** (~200 MB for all 10), MinIO **~1.0 GB** (it dominates, holding and
serving the 1.28M-object bucket): **~1.3 GB total for the whole platform**. The job's memory
is bounded by shard/lease size, not corpus size, so it does not grow with object count.
RAM is the real ceiling on this box, which is exactly why the dumper is memory-bounded: the
old `datasets`-streaming path OOM-killed this same 13.3 GiB machine at ~430k images, whereas
`dump_parquet_images.py` holds one ~1,000-row batch and stays flat across all 1.28M.

#### Reproduce it

Requires the local demo harness at `~/petabyte-demo` (MinIO + `mc` binaries) and a Python
venv with `datasets`, `pyarrow`, `huggingface_hub`, `pillow`.

```bash
# 1. Dump the full ImageNet train split to real JPEGs on disk (memory-bounded, resumable,
#    ~7 min, 19 GB). Do NOT use dump_dataset.py --stream at this size — it OOMs at ~430k.
python scripts/clip/dump_parquet_images.py \
    --hf-dataset benjamin-paine/imagenet-1k-256x256 --split train \
    --subdirs 256 --name-prefix imagenet --out ~/petabyte-demo/imagenet-images

# 2. Ingest into MinIO + run baseline & chaos (first run ingests ~1.28M objects, ~1.5 h).
DATASET=imagenet INGEST_DIR=~/petabyte-demo/imagenet-images WORKERS=10 KILL=3 \
    scripts/chaos-demo.sh

# 3. Re-run the job/chaos phase later WITHOUT re-ingesting (objects persist in minio-data):
DATASET=imagenet SKIP_INGEST=1 WORKERS=10 KILL=3 scripts/chaos-demo.sh

# The small CIFAR-10 variant (minutes end-to-end):
python scripts/clip/dump_dataset.py --hf-dataset uoft-cs/cifar10 --split test \
    --out ~/petabyte-demo/cifar-images
DATASET=cifar INGEST_DIR=~/petabyte-demo/cifar-images scripts/chaos-demo.sh
```

Env knobs: `WORKERS` (10), `KILL` (3), `CONCURRENCY` (8 — parallel tasks per worker),
`LEASE_CHUNK` (100000 — set high to grant each shard whole on a uniform corpus; drop to
~1000 to deliberately exercise work-stealing on skewed shards), `SKIP_INGEST`, `INGEST_DIR`.

#### Why this is *not* industry-standard production code

This is a portfolio/learning system that demonstrates the mechanisms honestly; it is **not**
something you would run a real petabyte fleet on. Concretely:

- **Single-box, not a real cluster.** "10 workers" are 10 processes on one machine against a
  single local MinIO on the same disk; there is no real network, no multi-node MinIO, no rack
  or AZ fault domains. Real distributed failures (partitions, slow disks, GC pauses, clock
  skew) are only approximated.
- **The 1.28M run is ~19 GB, not a petabyte.** It proves the *mechanism* scales in object
  count (1.28M objects / 256 shards / 52 rebalanced tasks), not that the system moves petabytes.
  Byte throughput here is bounded by one local disk, not a distributed store.
- **Metadata is SQLite.** A single-writer embedded DB is a hard scaling ceiling — the ingest
  already hit `SQLITE_BUSY` under 16-way write contention (worked around with a 30 s
  `busy_timeout`). Production would use a real distributed metadata store.
- **Exactly-once has a known gap.** The result-copy and done-mark aren't a single atomic
  replicated log entry, so a coordinator crash in a narrow window can re-run a committed task
  (safe because commits are idempotent, but not truly exactly-once). See *Future Plans*.
- **Tuned-for-demo timeouts.** suspect=2 s / dead=4 s make failures visible in seconds; real
  deployments need far more conservative detection to avoid false-positive failovers under load.
- **Work-stealing is naive.** It fragments large uniform shards into quadratic re-listing
  unless `lease_chunk` is hand-tuned (hence the knob above) — a production scheduler would size
  leases adaptively rather than relying on an operator to pick the right constant.
- **No security/multitenancy/ops.** Dev-only static credentials, no auth/encryption/quotas, no
  real deployment, autoscaling, upgrade, or backup story.

The value here is the *correctness reasoning* — content-addressed sharding, phi-accrual failure
detection, idempotent commits, work-stealing — demonstrated with real data and real numbers,
not a claim of production readiness.

## Future Plans

- **Close the exactly-once gap.** Fold the result copy and the done-mark into one replicated
  Raft log entry so a coordinator crash can't re-run a committed task.
- **Deeper observability.** Causal event tracing across the coordinator/worker boundary, and a
  `-race` stress/chaos harness that actively provokes races and deadlocks rather than waiting
  for them to appear.

## Prerequisites

- Go 1.26 or later (required by the Level 6 Raft dependency; earlier levels build on 1.22+)
- MinIO running locally (or AWS S3 credentials)

**Option A — MinIO via Docker:**

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

**Option B — MinIO as a standalone binary (no Docker, no root):**

MinIO and its client `mc` are single static Go binaries. This is the simplest path
on a dev laptop and is the setup used for the 10 GB walkthrough below.

```sh
mkdir -p ~/petabyte-demo/bin ~/petabyte-demo/minio-data
curl -sSLf -o ~/petabyte-demo/bin/minio https://dl.min.io/server/minio/release/linux-amd64/minio
curl -sSLf -o ~/petabyte-demo/bin/mc    https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x ~/petabyte-demo/bin/minio ~/petabyte-demo/bin/mc

# start the server (leave running in another shell)
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ~/petabyte-demo/bin/minio server ~/petabyte-demo/minio-data \
  --address :9000 --console-address :9001

# create the bucket (the storage client does NOT auto-create it)
~/petabyte-demo/bin/mc alias set local http://localhost:9000 minioadmin minioadmin
~/petabyte-demo/bin/mc mb local/petabyte-images
```

> **Port note:** the MinIO console binds `:9001`. Do **not** run a worker on 9001 — use
> 9101+ (see the walkthrough). `configs/worker.yaml` now defaults to 9101 to avoid the collision.

## Build

```sh
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
go build -o bin/server ./cmd/server
go build -o bin/ingest ./cmd/ingest
go build -o bin/operator ./cmd/operator
go build -o bin/sandbox-runner ./cmd/sandbox-runner
go build -o bin/gen-images ./cmd/gen-images   # synthetic dataset generator (see walkthrough)
```

## Testing

Run the full test suite (no external services required):

```sh
go test ./...
```

The suite covers 200+ tests across 22 packages and completes in a few seconds. No Docker or MinIO is needed because:

- Storage tests exercise sharding logic, tier mapping, and the multipart chunk reader (pure functions, no I/O)
- Metadata and registry tests use a real SQLite database in a temp directory
- Coordinator integration tests spin up the full coordinator API in-process via `net/http/httptest`
- K8s watcher and Ray client tests run against fake API servers via `net/http/httptest`
- Sandbox tests inject an in-memory object store and a fake container runtime, so the full task flow (stage -> run -> collect -> upload) runs with no Docker
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
| `internal/storage` | 9 | ShardKey determinism, 2-hex format, all 256 shards reachable, ObjectKey structure, tier -> S3 class mapping, multipart chunk reader |
| `internal/cluster` | 21 | Ring empty/single/multi-node, distribution variance (+/-10%), Remove redistribution, LookupN distinct nodes, concurrent churn under -race; Membership Active->Suspect->Dead transitions, recovery, failure events, concurrent heartbeat/tick |
| `internal/scheduler` | 33 | Submit, poll, start, result, retry, max-retry failure, RebalanceWorker, DrainPending, PendingCount, concurrent-poll no-double-assignment under -race; two-phase commit (dup/concurrent/failure), work-stealing range tiling + lease-generation handoff, and diag invariant checks |
| `internal/metadata` | 12 | Insert/GetShardManifest, SearchByLabel with limit, ShardStats, DatasetStats, LabelCounts, UpdateTier, RecordsByTierAge, durability after reopen |
| `internal/coordinator` | 11 | Full register->submit->poll->complete lifecycle via HTTP, heartbeat metrics, /v1/metrics/pending, /v1/operator/drain, and end-to-end work-stealing steal over the /renew endpoint |
| `internal/diag` | 7 | Invariant assert record/no-op-when-disabled, bounded violation ring, instrumented Mutex/RWMutex acquisition accounting, and lock-order cycle detection (inconsistent order flagged, consistent order clean) |
| `internal/admission` | 6 | Fail-closed on unknown tenant, weighted capacity partitioning, per-tenant isolation below the global cap, idempotent release, admitted/rejected accounting, and a concurrent burst that never exceeds the cap or a tenant's share |
| `internal/k8s` | 9 | JobName format/determinism, batch/v1 spec structure, GPU resources, node affinity from CachedShards, watcher phase resolution + emit/delete against fake API server |
| `internal/ray` | 7 | Dashboard health check, job submit/get, WaitForJob terminal-state polling and context cancellation |
| `internal/config` | 5 | DefaultConfig sane values, Load with missing file, overrides, malformed YAML |
| `internal/metrics` | 6 | Counter/Gauge/Histogram math, concurrent counter increments under -race, collector snapshot |
| `internal/tiering` | 5 | CostProjection zero/single/ordering/petabyte-scale/linear scaling |
| `internal/sandbox` | 17 | Package parse (required files, bad/invalid manifest, traversal, zip-bomb, content-canonical digest), buildRunArgs isolation flags (runsc + network none + ro mount), LimitsFromManifest, result collection + artifact-traversal guard, end-to-end Runner via fakes |
| `internal/registry` | 7 | Register/Get round-trip, version immutability, List/ListVersions, quota breaches, base-image allowlist |

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

Environment: 13th Gen Intel Core i7-13620H (16 threads), Go 1.26, Linux.

### Latency percentiles and throughput

| Operation | p50 | p90 | p95 | p99 | Throughput |
|---|---|---|---|---|---|
| SHA-256 hash-prefix shard routing | 448ns | 551ns | 593ns | 773ns | 1.84M/s (single), 23.6M/s (8 goroutines) |
| Consistent-hash ring lookup (100 nodes, 15k vnodes) | 463ns | 512ns | 527ns | 574ns | 1.89M/s (single), 6.16M/s (8 goroutines) |
| Scheduler task dispatch (in-process poll) | 17.6us | 115.8us | 162.0us | 243.3us | 24.3K/s |
| Metadata insert (SQLite WAL) | 86.9us | 130.6us | 151.0us | 224.4us | 10.1K/s |
| Metadata shard-manifest query (indexed) | 358.4us | 472.3us | 544.9us | 888.8us | 2.6K/s |
| Coordinator task-poll, end-to-end HTTP | 86.7us | 289.6us | 333.9us | 453.3us | 7.2K/s |

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

The consistent-hash ring performs **zero-allocation** lookups -- no garbage is
produced on the task-routing hot path regardless of cluster size.

### End-to-end local run (10 GB, 44,000 images)

Unlike the microbenchmarks above, this is a full-stack run through MinIO, the ingestion
pipeline, the coordinator, and three workers on a single laptop -- the first real
integration validation of the data plane (see the [walkthrough](#end-to-end-walkthrough-on-a-10-gb-dataset)).
Everything (object store, coordinator, workers) shares one machine, so these numbers
measure the platform's own overhead against local NVMe, not a real cluster.

| Stage | Result |
|---|---|
| Dataset | 44,000 synthetic JPEGs, 10.36 GB (~235 KB each) |
| Ingest (16-worker pipeline → MinIO) | 44,000 uploaded, **0 failed**, 2m03s → **356 img/s (~84 MB/s)** |
| Shard balance (SHA-256 routing, 256 shards) | min 130 / max 212 / **avg 171.9** images per shard (ideal 171.9) |
| Distributed scan (3 workers, 256 tasks) | **8.88 s → 1.17 GB/s aggregate read**, load-balanced 85 / 86 / 85 tasks |

The scan uses the Level 2 `runAlgorithm` placeholder (list shard → read every object →
report bytes), so 1.17 GB/s is read-and-dispatch throughput, not real inference (the
gVisor-sandboxed algorithm path is Level 4 and needs Kubernetes). Load balance across
workers reflects the consistent-hash ring assigning shards; the near-uniform shard fill
reflects SHA-256 prefix distribution.

**Bug found by this run:** the metadata index opened SQLite with `mattn/go-sqlite3`-style
DSN params (`?_journal_mode=WAL&_busy_timeout=5000`), which the actual driver
(`modernc.org/sqlite`) silently ignores -- so WAL was off and `busy_timeout` was 0, and
the 16-worker ingest hit instant `SQLITE_BUSY` (only 4 of the first 50 index rows
survived; objects still uploaded fine). Fixed to the driver's `?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)`
form; the 44,000-insert run then completed with zero failures.

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
bin/worker -config configs/worker.yaml -id worker-1 -port 9101
bin/worker -config configs/worker.yaml -id worker-2 -port 9102
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

## End-to-End Walkthrough on a 10 GB Dataset

A full local run — MinIO, ingestion, coordinator, workers, and a job — validated on
44,000 synthetic images (10.36 GB). No Docker, no cloud, no GPU. Assumes MinIO is
running via **Option B** above and the binaries are built into `bin/`.

The pieces talk over HTTP; compute is pulled to where the data lives:

```
  bin/ingest ──uploads──► MinIO (S3)  ◄──reads shards── bin/worker (xN)
       │                     ▲                              │
   metadata.db          holds train/<shard>/<file>   polls for tasks
   (SQLite)                  │                              ▼
                       bin/coordinator :8090 ──1 task per shard──►
```

Run every command below with your working directory set to `~/petabyte-demo` so the
generated `metadata.db` and `coordinator-wal/` stay out of the repo. Let `$P` point at
the repo checkout:

```sh
export P=/path/to/petabyte-platform
cd ~/petabyte-demo
```

**1. Generate ~10 GB of synthetic images.** Any folder of `.jpg`/`.png` works; for a
self-contained test, use the bundled generator. It writes random-noise JPEGs (noise
compresses poorly, so each is a realistic ~235 KB) named `<class>_<n>.jpg`, which the
hash-prefix sharding spreads across all 256 shards. ~44,000 files ≈ 10 GB:

```sh
go build -o bin/gen-images ./cmd/gen-images
bin/gen-images -out ~/petabyte-demo/images -count 44000 -dim 512
# -> done: 44000 images in ~2m (≈330 img/s)
```

(Or skip this and point `-dir` at any real image directory you already have.)

**2. Ingest into the `train` dataset** (16-worker pipeline: SHA-256 checksum → 2-hex
shard prefix → S3 upload → SQLite index row):

```sh
$P/bin/ingest -config $P/configs/server.yaml \
  -dir ~/petabyte-demo/images -dataset train -labels "synthetic,bench"
# -> processed=44000 failed=0 bytes=10364550448 elapsed=2m03s (356 img/s)
```

**3. Start the coordinator** (hash ring + scheduler, port 8090):

```sh
$P/bin/coordinator -config $P/configs/coordinator.yaml &
```

**4. Start workers on ports 9101+** (NOT 9001 — MinIO's console owns it). Copy
`configs/worker.yaml` to `worker-demo.yaml`, set `port: 9101` and `poll_interval: 100ms`
for a snappy local demo, then:

```sh
for p in 9101 9102 9103; do
  $P/bin/worker -config ~/petabyte-demo/worker-demo.yaml -id worker-$p -port $p &
done
curl -s localhost:8090/v1/cluster/nodes   # all three Active
curl -s localhost:8090/v1/cluster/ring    # 150 vnodes each
```

**5. Inspect the sharded dataset** (optional — start the Level 1 server on :8080):

```sh
$P/bin/server -config $P/configs/server.yaml &
curl -s "localhost:8080/v1/stats?dataset=train"    # 44000 images, 256 shards, 10.36 GB
curl -s "localhost:8080/v1/shards?dataset=train"   # per-shard Count (near-uniform)
```

**6. Submit a job and watch it drain.** One task per shard; workers poll, read every
object in their shard, and report back:

```sh
JOB=$(curl -s -X POST localhost:8090/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"train","algorithm":"shard-scan"}' | jq -r .job_id)

# poll until done_tasks == 256
curl -s localhost:8090/v1/jobs/$JOB | jq '{status, done_tasks, failed_tasks}'
# -> {"status":"completed","done_tasks":256,"failed_tasks":0}  (~9s for all 10 GB)
```

**7. Tear down** and reclaim disk:

```sh
pkill -f 'bin/worker'; pkill -f 'bin/coordinator'; pkill -f 'bin/server'
rm -rf ~/petabyte-demo/images ~/petabyte-demo/minio-data \
       ~/petabyte-demo/metadata.db* ~/petabyte-demo/coordinator-wal
```

See [Performance → End-to-end local run](#end-to-end-local-run-10-gb-44000-images) for the
measured numbers.

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
| GET | `/v1/search?label=cat&dataset=train&limit=100` | Label search (metadata) |
| GET | `/v1/labels?dataset=train` | Label frequency counts |
| POST | `/v1/tiering/estimate` | Storage cost projection (body: map of tier -> bytes) |
| POST | `/v1/similar` | CLIP vector similarity (body: `k` + one of `id`/`vector`/`text`/`image_b64`); `501` until `server.search.index_key` is set |

### Level 4 Algorithm Registry (:8080)

| Method | Path | Description |
|---|---|---|
| GET | `/v1/algorithms` | List all registered algorithm versions |
| POST | `/v1/algorithms` | Submit an algorithm package (zip body); validates against quota and registers it. `X-Tenant` header sets the owner |
| POST | `/v1/algorithms/validate` | Dry-run: parse and validate a package without registering |
| GET | `/v1/algorithms/{name}` | List all versions of one algorithm |
| GET | `/v1/algorithms/{name}/{version}` | Get one algorithm's metadata |

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
| GET | `/v1/metrics/tasks` | Task state counts, `rebalances` (failure-driven requeues), and task-latency percentiles (p50/p95/p99/max/mean over StartedAt→FinishedAt) |
| GET | `/v1/metrics/admission` | Backpressure load: in-flight, lifetime admitted/rejected, per-tenant (`enabled:false` when unconfigured) |
| POST | `/v1/operator/drain?n=N` | Atomically drain up to N pending tasks for K8s Job creation |

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
| 3 | Kubernetes operator, K8s Job-per-shard, Ray integration, GPU scheduling, HPA | Complete |
| 4 | Sandboxed algorithm execution (gVisor), resource limits, algorithm registry | Complete |
| 5 | Apache Arrow pipelines, WAL checkpointing, Parquet output, phi accrual FD | Complete |
| 6 | Per-tenant quotas + ledger, token-bucket limiter, JWT auth + RBAC, gRPC API + WatchJob, Raft HA | Complete |
| + | Exactly-once two-phase staging commit (Raft-agreed, fenced commit decision) | Complete |
| + | Multi-process Raft HA over a gRPC transport + replicated commit ledger | Complete |
| + | Intra-shard work stealing (bounded-grant lease handoff) | Complete (live MinIO demo: `scripts/steal-demo.sh`) |
| + | Runtime concurrency diagnostics (`internal/diag`, `/debug/diag`) | Complete |
| + | Backpressure & admission control (`internal/admission`, load-shedding + weighted shares) | Complete |
| + | CLIP image similarity search (`internal/vsearch`, exact cosine k-NN + `/v1/similar`) | Complete (live demo: `scripts/clip-search-demo.sh`) |
| + | End-to-end CLIP/LAION similarity-search demo | Planned |
| + | Causal event tracing + `-race` chaos harness | Planned |
