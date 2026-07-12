# Design Decisions

This document records the significant design decisions behind the platform and,
for each, the alternatives that were considered with their trade-offs. The goal
is not to argue that every choice is optimal for all situations but to make the
reasoning explicit: what problem the decision solves, what it costs, and under
what conditions a different choice would win.

Decisions are grouped by concern. Each entry follows the same shape: **Context**
(the forces at play), **Alternatives** (with pros/cons), and **Decision** (what
was chosen and why).

## Table of Contents

- [1. Data layout](#1-data-layout)
  - [1.1 Hash-prefix sharding](#11-hash-prefix-sharding-into-256-partitions)
  - [1.2 Object storage as the substrate](#12-object-storage-s3minio-as-the-substrate)
  - [1.3 Metadata in SQLite](#13-metadata-index-in-sqlite)
- [2. Distribution & scheduling](#2-distribution--scheduling)
  - [2.1 Pull-based work distribution](#21-pull-based-work-distribution)
  - [2.2 AP over CP](#22-ap-over-cp-with-dedup-by-output-key)
  - [2.3 Consistent hashing with virtual nodes](#23-consistent-hashing-with-virtual-nodes)
  - [2.4 Failure detection](#24-failure-detection-two-stage--phi-accrual)
- [3. Correctness under concurrency](#3-correctness-under-concurrency)
  - [3.1 Exactly-once via two-phase staging](#31-exactly-once-via-two-phase-staging-commit)
  - [3.2 Work stealing via bounded-grant leases](#32-work-stealing-via-bounded-grant-leases)
  - [3.3 Backpressure by load shedding](#33-backpressure-by-load-shedding)
  - [3.4 Runtime concurrency diagnostics](#34-runtime-concurrency-diagnostics)
- [4. Durability & availability](#4-durability--availability)
  - [4.1 WAL + checkpoint for coordinator state](#41-wal--checkpoint-for-coordinator-state)
  - [4.2 Raft for coordinator HA](#42-raft-for-coordinator-ha)
- [5. Compute & isolation](#5-compute--isolation)
  - [5.1 gVisor sandbox with no network](#51-gvisor-sandbox-with-no-network)
  - [5.2 Kubernetes via the raw REST API](#52-kubernetes-via-the-raw-rest-api)
- [6. Platform surface](#6-platform-surface)
  - [6.1 HTTP/JSON and gRPC side by side](#61-httpjson-and-grpc-side-by-side)
  - [6.2 Multiple ML output formats](#62-multiple-ml-output-formats)
- [7. Cross-cutting](#7-cross-cutting)
  - [7.1 Go as the implementation language](#71-go-as-the-implementation-language)
  - [7.2 Monorepo with internal packages](#72-monorepo-with-internal-packages)

---

## 1. Data layout

### 1.1 Hash-prefix sharding into 256 partitions

**Context.** The platform stores petabyte-scale image datasets and must spread
them so that (a) no single storage prefix becomes a hotspot, (b) work can be
partitioned across workers without coordination, and (c) a filename maps to its
location by pure computation, with no lookup. S3 throttles per key *prefix*
(~3,500 writes / 5,500 reads per second per prefix), so the layout directly
determines the throughput ceiling.

Object keys are `{dataset}/{shard}/{filename}` where `shard` is the first two hex
characters of `SHA-256(filename)` — 256 partitions.

**Alternatives.**

- **Range/lexicographic partitioning (e.g. by filename prefix `a*`, `b*`).**
  - Pros: range scans are cheap; human-readable; supports ordered iteration.
  - Cons: pathological skew — real filenames cluster (`IMG_0001`, `cat_*`), so a
    few ranges get most of the data and become hotspots. Rebalancing requires
    moving objects.
- **Directory/date-based partitioning (`year/month/day/`).**
  - Pros: natural for time-series ingestion; easy lifecycle rules.
  - Cons: today's partition is always the write hotspot; unbalanced historically;
    unrelated to read access patterns.
- **More shards (e.g. 4,096 = 3 hex chars) or fewer (16 = 1 hex char).**
  - Pros of more: higher aggregate S3 ceiling, finer work granularity.
  - Cons of more: more list operations, more metadata rows, diminishing returns
    past the point where shard count ≫ worker count.
  - Fewer: fewer partitions to manage, but a lower ceiling and coarse balancing.
- **Consistent hashing of individual objects (not a fixed prefix count).**
  - Pros: rebalances smoothly as capacity changes.
  - Cons: object → location is no longer a pure function of the name; needs a ring
    lookup per object; overkill when the storage layer (S3) already handles
    physical placement.

**Decision.** SHA-256 hash-prefix into a **fixed 256 shards**. SHA-256 gives a
near-uniform distribution independent of naming conventions (validated at 10 GB:
min 130 / max 212 / avg 172 objects per shard). 256 is a deliberate sweet spot:
`256 × 3,500 req/s ≈ 896,000 req/s` ceiling, which is far above any single
cluster's needs, while keeping shard count small enough that list/manifest
operations stay cheap and every shard still holds meaningful work. The mapping is
a pure function — `shard = SHA256(name)[:2]` — so any component computes a
location with zero coordination. The cost we accept: no range scans and no
in-place rebalancing of the shard count (changing 256 would rehash everything),
which is fine because the shard count is a design constant, not an operational
knob.

### 1.2 Object storage (S3/MinIO) as the substrate

**Context.** The data plane must hold petabytes durably and cheaply, be reachable
by many workers concurrently, and support tiering to colder storage as data ages.

**Alternatives.**

- **HDFS / a distributed filesystem.**
  - Pros: data-locality scheduling is first-class; high sequential throughput.
  - Cons: heavy to operate (NameNode HA, DataNodes); couples storage to compute
    nodes; poor fit for cloud object semantics and lifecycle tiering.
- **Local disk on worker nodes (shared-nothing).**
  - Pros: fastest possible reads; no network for data.
  - Cons: durability and rebalancing become the platform's problem; losing a node
    loses its shards unless replicated by hand.
- **A database/blob store (e.g. images as BLOBs in Postgres).**
  - Pros: transactional metadata + data together.
  - Cons: databases are the wrong tool for petabytes of large binary objects;
    cost and operational load explode.

**Decision.** S3-compatible object storage (AWS S3 in production, MinIO for local
dev). It gives durability, effectively unbounded capacity, native storage-class
tiering (STANDARD → IA → GLACIER → DEEP_ARCHIVE), and a uniform API across cloud
and laptop. The trade-off is that data locality is not automatic — compute is not
physically on the bytes — which the platform addresses at the scheduling layer
(ring-based locality preference, NVMe caching, prefetch) rather than in storage.
Using the same S3 API for MinIO means the local walkthrough exercises the real
code paths, not a mock.

### 1.3 Metadata index in SQLite

**Context.** Ingestion needs a queryable index: which objects are in a shard,
their labels, sizes, tiers, checksums. This is small relative to the data
(kilobytes per object) but is written concurrently by a many-worker ingest
pipeline and read by the API.

**Alternatives.**

- **PostgreSQL / a networked RDBMS.**
  - Pros: real concurrency, rich queries, horizontal read scaling.
  - Cons: an extra service to deploy, secure, and back up; overkill for an index
    that is a strict subset of what S3 already lists.
- **An embedded KV store (BoltDB/Badger).**
  - Pros: no SQL engine; fast point lookups.
  - Cons: label search and aggregate stats (counts by tier/label) become
    hand-rolled scans; loses ad-hoc query flexibility.
- **No index; list from S3 on demand.**
  - Pros: zero extra state; S3 is the single source of truth.
  - Cons: label search and per-shard stats become full-bucket scans; far too slow
    and expensive at petabyte scale.

**Decision.** Embedded **SQLite in WAL mode**. It is a single file, needs no
service, and gives real SQL (indexed shard manifests, label search, tier
aggregates) that fits the platform's read patterns. WAL mode plus a `busy_timeout`
lets the concurrent ingest pipeline write without serializing on a global lock.
The known cost: SQLite is single-node, so at true multi-coordinator scale the
index would need to move to a networked DB — an acceptable later migration, since
the schema and queries carry over. (This layer also surfaced a real bug: the
`modernc.org/sqlite` driver ignores `mattn`-style DSN pragmas, so WAL was
silently off until the DSN was corrected — see the README.)

---

## 2. Distribution & scheduling

### 2.1 Pull-based work distribution

**Context.** Tasks (one per shard, or sub-ranges after stealing) must reach
workers. Workers vary in speed and number, and can die.

**Alternatives.**

- **Push / central dispatch (coordinator assigns tasks to specific workers).**
  - Pros: coordinator has global placement control; can optimize locality
    directly.
  - Cons: the coordinator must track each worker's live capacity and in-flight
    work; a slow worker that was pushed a task creates head-of-line blocking;
    backpressure is awkward (who slows down whom?).
- **Message queue (workers consume from Kafka/SQS).**
  - Pros: durable buffering, mature at-least-once delivery, natural fan-out.
  - Cons: another heavy dependency; the queue becomes the coordination point;
    range-based work stealing and lease renewal don't map cleanly onto a queue.

**Decision.** **Pull model** — workers poll `GET /v1/tasks/poll?worker=ID`. A busy
worker simply doesn't poll, so it never gets over-assigned; a fast worker pulls
more; a dead worker just stops pulling and its in-flight tasks are rebalanced by
the failure detector. This makes the system naturally self-balancing and makes
backpressure a property of the *admission* side rather than the dispatch side.
The cost is polling latency (bounded by the poll interval) and that the
coordinator has slightly less direct control over placement — recovered by
letting `poll` express a ring-locality preference and by work stealing filling
idle capacity.

### 2.2 AP over CP, with dedup by output key

**Context.** During a network partition or a false-positive failure detection, the
platform must choose (CAP) between refusing to make progress (CP) and continuing
with the risk of duplicate task execution (AP).

**Alternatives.**

- **CP (linearizable task ownership; a task runs at most once).**
  - Pros: no duplicate work; simpler reasoning about side effects.
  - Cons: a partition stalls progress; requires consensus on the hot path of every
    task assignment, adding latency and a liveness dependency on a quorum.

**Decision.** **AP**: workers keep processing during a partition, and duplicate
execution is *tolerated and made harmless* rather than prevented. The key insight
is that the workload is (mostly) idempotent computation over immutable inputs, so
running a shard twice produces the same bytes. Duplicates are deduplicated at the
**output key**: two runs of the same task write to the same canonical key, so the
last writer wins and there is exactly one visible result. This buys availability
and throughput at the cost of occasional wasted recompute — a good trade when
compute is cheap relative to coordination and inputs are immutable. Where true
side effects exist, this is upgraded by the two-phase commit ([3.1](#31-exactly-once-via-two-phase-staging-commit))
and, ultimately, Raft ([4.2](#42-raft-for-coordinator-ha)).

### 2.3 Consistent hashing with virtual nodes

**Context.** Shards must map to workers for locality preference, and the mapping
must survive workers joining and leaving with minimal reshuffling.

**Alternatives.**

- **Modulo-N hashing (`worker = hash(shard) % N`).**
  - Pros: trivial, zero state.
  - Cons: changing N remaps almost every shard — catastrophic churn when a worker
    joins/leaves.
- **Rendezvous (highest-random-weight) hashing.**
  - Pros: minimal disruption, no ring to maintain, naturally balanced.
  - Cons: `O(N)` per lookup (score every node); balancing without vnodes can be
    lumpy for small N.
- **Centralized assignment table (coordinator stores shard → worker).**
  - Pros: full control, arbitrary policies.
  - Cons: state to persist and replicate; the coordinator becomes authoritative
    for placement, adding coupling.

**Decision.** **Consistent hashing with 150 virtual nodes per physical node.**
Adding or removing a worker remaps only `~1/N` of shards (not all of them), and
vnodes smooth the distribution to roughly ±5% variance. Lookups are `O(log V)`
via a sorted ring. Rendezvous hashing was the closest contender and would also be
defensible; the ring was chosen because vnode density gives predictable balance
and the same structure serves both locality preference and rebalancing. It is a
*preference*, not a hard assignment — a worker may still pull a non-local shard —
so the ring never blocks progress.

### 2.4 Failure detection: two-stage + phi-accrual

**Context.** A worker that stops heartbeating must be declared dead so its tasks
are rebalanced — but too-eager declaration on a jittery network causes false
positives (and duplicate work), while too-slow declaration wastes capacity.

**Alternatives.**

- **Single fixed timeout (dead after N seconds of silence).**
  - Pros: dead simple.
  - Cons: one threshold can't fit both a steady LAN and a jittery link; picking it
    is a guess; no notion of "suspected but not yet dead."
- **Heartbeat count only (dead after K missed beats).**
  - Pros: simple, somewhat adaptive to interval.
  - Cons: still a hard threshold; ignores the *distribution* of arrival times.

**Decision.** A **two-stage detector** (Active → Suspect → Dead, with recovery on
any heartbeat) for coarse state, plus an optional **phi-accrual** detector that
models the recent heartbeat inter-arrival distribution and emits a continuous
suspicion value `φ = -log10(P(late))` instead of a fixed timeout. This lets a
link that is normally jittery tolerate longer gaps than a normally-steady one,
without hand-tuned per-link thresholds — the threshold is expressed once, in units
of confidence, not seconds. The cost is more state per node (a sliding window of
arrival times) and slightly more complex tuning semantics, justified because
false-positive failure detection directly causes duplicate execution and
rebalancing churn.

---

## 3. Correctness under concurrency

These are the platform's hardest problems: each is a place where a naive
implementation is subtly wrong under concurrency.

### 3.1 Exactly-once via two-phase staging commit

**Context.** A worker computes a result and must publish it. If it writes directly
to the final key and then reports done, a crash or a duplicate (from AP execution)
can leave a half-written or racing object, and consumers can observe partial
state.

**Alternatives.**

- **At-least-once with a direct write.**
  - Pros: simplest; one write.
  - Cons: a consumer can read a partially written object; two concurrent runs race
    on the final key with no defined winner boundary.
- **Client-side dedup (workers coordinate who writes).**
  - Pros: avoids duplicate writes.
  - Cons: reintroduces coordination on the hot path — the very thing AP avoids.
- **Transactional outbox / DB transaction around the write.**
  - Pros: atomic result + state.
  - Cons: object stores aren't transactional with your DB; needs a DB in the write
    path.

**Decision.** **Two-phase staging commit, with the commit decision agreed through
Raft.** The worker writes to a *staging* key (invisible to consumers); the
coordinator commits by an **idempotent server-side copy** to the canonical
`FinalResultKey`, and then records the terminal "this task is committed" decision.
Because the copy is attempt-independent and the final key is fixed, a re-run
stages a fresh object but only the single commit makes any object visible — the
copy is idempotent in the destination, so duplicates collapse to one result. The
copy runs *outside* the scheduler lock (a server-side copy is slow relative to
polling) with a done-guard for idempotency.

The commit *decision* itself is not a bare local write: when a `CommitDecider` is
attached (`internal/scheduler/commit.go`), it is proposed as an entry in the
replicated **Raft** log (`internal/consensus`, over a real gRPC transport —
[4.2](#42-raft-for-coordinator-ha)). The decision is therefore **agreed exactly
once across coordinators** (no split-brain), **fenced by lease generation** (a
stale/zombie attempt with an older generation loses to the recorded one), and
**durable on a majority**, so a failover leader inherits an authoritative record
of which tasks are committed and never re-dispatches one. Recovery is
deterministic: committed-ness is a majority fact, not a guess reconstructed from a
single node's WAL.

**Honest limitation (documented, not hidden):** this makes the *output* and the
*commit decision* exactly-once, but not arbitrary *side effects*. The
copy-then-decide ordering leaves an irreducible window — a crash *before* the
decide re-executes the task. Its output is unaffected (the copy is idempotent in
the destination), but a real algorithm's external side effects would repeat. That
residual window cannot be closed by more consensus: it is the fundamental
atomic-commit-across-heterogeneous-systems problem (the object store and the log
are two systems, and no single action spans both). The only way to make side
effects themselves exactly-once is to pass **idempotency keys** into the
downstream system so *it* dedups — which is exactly what the deterministic,
attempt-independent `FinalResultKey` does for the one side effect the platform
controls (the write).

### 3.2 Work stealing via bounded-grant leases

**Context.** One shard is one task. A giant shard is a single task that one worker
grinds through alone while others idle — an *intra-shard* tail-latency problem
(not cross-shard imbalance, which the ring already handles). We want another
worker to help finish a big shard, without ever processing an item twice.

**Alternatives.**

- **Do nothing (one shard = one task, always).**
  - Pros: trivial; no stealing machinery.
  - Cons: the slowest shard bounds job completion; a skewed dataset has terrible
    tail latency.
- **Static pre-split (divide each shard into K sub-tasks up front).**
  - Pros: simple, parallel from the start.
  - Cons: picking K is a guess; over-splitting adds overhead on small shards,
    under-splitting doesn't help the big ones; no adaptation to actual worker
    speed.
- **Central queue of fine-grained sub-tasks.**
  - Pros: naturally load-balances.
  - Cons: coordinator must enumerate and track every sub-task; huge state for
    billions of items; loses the "compute where the data is" property.
- **Steal based on the victim's reported progress (frontier).**
  - Pros: intuitive.
  - Cons: *unsafe* — the victim may have processed past its last report, so
    stealing `[frontier, end)` can double-process the in-flight gap.

**Decision.** **Bounded-grant leases.** Each task owns a half-open range
`[RangeStart, RangeEnd)` over the shard's sorted key list plus a lease. The safety
mechanism is that `RenewLease` grants only the next `leaseChunk` items, so a
worker's real progress can never exceed `Granted`. The scheduler therefore steals
only the provably-untouched tail `[Granted, RangeEnd)` — never the region the
worker might be inside. A steal splits that tail, shrinks the victim's `RangeEnd`,
and bumps its lease generation; the victim learns on its next renewal and stops
cooperatively at the split. The invariant `RangeStart ≤ Frontier ≤ Granted ≤
RangeEnd` holds throughout, and ranges tile the shard with no gap or overlap. S3
lists keys lexicographically, so the victim and thief index into the *same* sorted
offsets with no shared state beyond the range integers. It composes with
[3.1](#31-exactly-once-via-two-phase-staging-commit) through range-scoped result
keys.

The cost: workers must renew leases (extra round-trips proportional to
work/`leaseChunk`), and `leaseChunk` is a tunable that trades renewal overhead
against steal granularity. This is a deliberate trade of a little chatter for a
provable no-double-processing guarantee.

### 3.3 Backpressure by load shedding

**Context.** Submitting a thousand jobs schedules a quarter-million tasks at once.
Without admission control the scheduler's working set explodes and tail latency
grows without bound. Multi-tenancy adds a second requirement: one tenant's burst
must not starve another.

**Alternatives.**

- **Unbounded queue (accept everything, queue internally).**
  - Pros: never rejects a submission; simple to implement.
  - Cons: the classic latency collapse. By Little's Law (`L = λW`), if arrival
    rate `λ` meets or exceeds service capacity, queue occupancy `L` and wait `W`
    grow without limit. The system looks healthy at steady state and falls over
    under a burst.
- **Work-conserving weighted fair queuing (WFQ).**
  - Pros: highest utilization — idle capacity is lent to whoever has work;
    fairness under contention.
  - Cons: needs a real async queue (reintroducing the unbounded-latency risk it
    must then re-bound); more complex; fairness accounting on the hot path.
- **Rate limiting only (token bucket on submissions).**
  - Pros: caps arrival rate; simple.
  - Cons: bounds *rate*, not *concurrency* — a sustained in-limit rate can still
    accumulate unbounded in-flight work if jobs are long; doesn't model capacity.

**Decision.** **Load shedding with weighted per-tenant shares.** A global
`MaxInFlight` cap on concurrently-admitted jobs is the backpressure valve: past
it, a submission is shed with `429 + Retry-After` rather than queued. This bounds
the working set — and therefore `W` — *regardless* of arrival rate. Capacity is
partitioned across tenants by weight (isolation), and each tenant is then an
independent `M/M/c/c` **Erlang-B loss system** whose blocking probability is a
closed-form function of offered load and share — a design knob computable up
front, not an emergent surprise. A held admission ticket is released the instant
the job reaches a terminal state (via the scheduler's job-done hook). Priority
orders dispatch: an idle worker with no local task takes the highest-priority
pending work.

**Honest trade-off (documented):** hard per-tenant shares favor *isolation over
utilization* — a quiet tenant's reserved slots sit idle rather than being lent to
a busy neighbor. WFQ would reclaim that idle capacity, but the loss-system design
is simpler, predictable, and analyzable in closed form, which is the right default
for a platform whose primary multi-tenant job is *not letting one tenant hurt
another*. If utilization became the dominant concern, WFQ is the documented next
step.

### 3.4 Runtime concurrency diagnostics

**Context.** Data races, deadlocks, and logical invariant violations are the bugs
that ordinary logging misses and that are hardest to reproduce. We want to catch
them during real runs, not just in a lab.

**Alternatives.**

- **`go test -race` only.**
  - Pros: the gold standard for *data* races; no production cost.
  - Cons: only catches races that actually execute under test; adds heavy overhead
    (unsuitable for production); says nothing about *logical* races (double-commit,
    stale generation) or deadlocks that don't happen to trigger in tests.
- **pprof / execution traces.**
  - Pros: excellent for profiling and post-hoc analysis.
  - Cons: not designed to assert invariants or predict lock-order deadlocks before
    they hang.
- **An external APM/tracing vendor.**
  - Pros: rich dashboards, distributed traces.
  - Cons: heavyweight dependency; not focused on the specific concurrency
    invariants of this scheduler.

**Decision.** A small, **opt-in** `internal/diag` layer that is complementary to
`-race`, not a replacement. `diag.Mutex`/`diag.RWMutex` are drop-in `sync`
replacements that time wait/hold and maintain a global lock-order graph — an
inconsistent acquisition order (latent deadlock) is reported the first time it
occurs, via a DFS cycle check, *before* it can hang. `diag.Assert` logs loudly
(never panics) on invariant violations (lease ordering, steal no-reclaim,
generation monotonicity), and `/debug/diag` exposes lock stats, violations, and a
goroutine dump. Off by default: one atomic load per lock op, nothing for asserts.
The explicit scope boundary — it catches *logical* races and *deadlocks*, not
*data* races — is stated so it is never mistaken for a substitute for `-race`.

---

## 4. Durability & availability

### 4.1 WAL + checkpoint for coordinator state

**Context.** The coordinator holds job/task state in memory. A restart must not
lose in-flight jobs.

**Alternatives.**

- **No persistence (rebuild from workers on restart).**
  - Pros: nothing to write.
  - Cons: in-flight state is genuinely lost; requires every worker to re-report,
    and some state (which tasks existed) can't be reconstructed.
- **Write state to the metadata DB on every change.**
  - Pros: durable, queryable.
  - Cons: a DB round-trip on every scheduler mutation is slow on the hot path;
    couples the scheduler to the DB.
- **Snapshot-only (periodic full dumps).**
  - Pros: simple.
  - Cons: loses everything since the last snapshot on a crash.

**Decision.** A **write-ahead log plus periodic checkpoints**. Every scheduler
mutation is appended (CRC32-framed, fsync'd) with a monotonic sequence number, and
a checkpoint folds the log into an atomic snapshot. On restart the coordinator
replays snapshot + tail. The sequence number makes replay idempotent (a crash
between snapshot-write and log-truncate never double-applies), and a torn tail is
detected and truncated. This is the standard durable-state-machine pattern: fast
appends on the hot path, bounded replay time via checkpoints, crash safety via
framing and sequencing. The cost is fsync latency per mutation and checkpoint I/O,
both acceptable for coordinator-rate (not data-rate) writes.

### 4.2 Raft for coordinator HA

**Context.** A single coordinator is a single point of failure. HA requires
replicating its state machine across nodes with agreed ordering.

**Alternatives.**

- **No HA (accept the SPOF, rely on fast restart + WAL).**
  - Pros: far simpler; the WAL already gives durability.
  - Cons: a coordinator outage stalls the whole control plane until restart.
- **External coordination service (etcd/ZooKeeper as the source of truth).**
  - Pros: mature, offloads consensus.
  - Cons: another cluster to run; the coordinator's rich state doesn't map cleanly
    onto a KV watch API; still need an FSM on top.
- **Gossip / eventually-consistent replication.**
  - Pros: highly available, partition-tolerant.
  - Cons: no total order — exactly the property needed to make copy-and-mark
    atomic; conflicts on task state would need bespoke resolution.

**Decision.** **Raft** (via the etcd `raft` library) behind an FSM + a real gRPC
transport (`internal/consensus` + `internal/consensus/raftgrpc`). A 3-node cluster
of separate processes elects a leader, replicates committed commands to every FSM
over the wire, and survives leader failure with no split brain. Raft is chosen
over an external service because the coordinator's state is a genuine replicated
state machine, not a set of KV pairs, and the same log is the natural home for the
exactly-once **commit ledger** ([3.1](#31-exactly-once-via-two-phase-staging-commit)):
the terminal commit decision is a Raft entry (`CommitFSM`), so it is agreed once
and fenced by generation. The transport drops rather than blocks (per-peer bounded
queues), honouring Raft's retransmit contract so a slow peer never stalls the run
loop. The cost is real: consensus latency on writes and the operational weight of a
quorum. Because of that, HA is a distinct, opt-in layer (Level 6), not baked into
the single-node path, so the platform runs single-coordinator when HA isn't needed.

---

## 5. Compute & isolation

### 5.1 gVisor sandbox with no network

**Context.** Users submit arbitrary algorithm code that runs against the data.
Untrusted code must not read other tenants' data, exfiltrate over the network, or
escape to the host.

**Alternatives.**

- **Plain Docker/OCI containers.**
  - Pros: ubiquitous, fast.
  - Cons: shares the host kernel; a kernel exploit escapes the container — too weak
    for genuinely untrusted code.
- **Firecracker / full microVMs.**
  - Pros: strongest isolation (a real VM boundary).
  - Cons: heavier per-task startup and resource cost; more moving parts to
    orchestrate for short-lived shard jobs.
- **seccomp/AppArmor profiles on a normal container.**
  - Pros: lightweight hardening.
  - Cons: still the host kernel's syscall surface; profiles are easy to get subtly
    wrong.
- **No sandbox (trust the code).**
  - Pros: zero overhead.
  - Cons: unacceptable for a multi-tenant platform running arbitrary code.

**Decision.** **gVisor (`runsc`) with network mode `none`.** gVisor interposes a
user-space kernel between the workload and the host, shrinking the host syscall
attack surface dramatically while staying far lighter than a microVM — the right
point on the security/overhead curve for short-lived, CPU/GPU-bound shard jobs.
Network `none` means the code can read the mounted input volume and write the
output volume and *nothing else*; the sandbox cannot be disabled by the user's
manifest. Hard resource limits (`--cpus`, `--memory`, `--gpus`) are enforced by
the runtime/cgroups, and timeout vs. OOM kills are distinguished so the scheduler
retries vs. fails appropriately. If a workload ever needed a stronger boundary
than gVisor, Firecracker is the documented escalation.

### 5.2 Kubernetes via the raw REST API

**Context.** The operator creates one `batch/v1` Job per task, with data-locality
node affinity and GPU resources.

**Alternatives.**

- **`k8s.io/client-go` + controller-runtime.**
  - Pros: the standard, feature-rich, typed clients, informers, leader election.
  - Cons: a very large dependency tree with tight version coupling to the cluster;
    heavy for the narrow set of objects this operator actually touches.

**Decision.** Talk to the **Kubernetes REST API directly** (no `client-go`). The
operator manipulates a small, well-known set of objects (Jobs, a watch), so the
raw API keeps the dependency surface tiny and avoids the notorious `client-go`
version-pinning pain. The trade-off is giving up informers/typed helpers and
writing a bit more marshaling by hand — worthwhile for a focused operator, and
revisited if the operator's scope grew to many resource types.

---

## 6. Platform surface

### 6.1 HTTP/JSON and gRPC side by side

**Context.** The control plane needs both easy human/`curl` access and an
efficient, streaming, strongly-typed API for programmatic clients.

**Alternatives.**

- **HTTP/JSON only.**
  - Pros: universal, trivial to debug, no codegen.
  - Cons: no server-streaming (clients must poll for job progress); no schema
    contract; more per-request overhead.
- **gRPC only.**
  - Pros: typed, efficient, streaming, generated clients.
  - Cons: not `curl`-able; harder to debug; forces codegen on every consumer.

**Decision.** **Both.** HTTP/JSON is the default, always-on surface (health,
submit, poll, metrics) — easy to operate and demo. gRPC is an opt-in Level 6
control-plane API that mirrors the HTTP surface and *adds* `WatchJob`, a
server-streaming RPC that replaces the client poll loop, all behind an
authenticate → authorize (RBAC) → rate-limit interceptor chain. gRPC refuses to
start without a JWT secret (fail-closed). The cost is maintaining two surfaces,
mitigated by having the scheduler satisfy both and keeping the gRPC set a mirror
plus streaming.

### 6.2 Multiple ML output formats

**Context.** Results feed ML frameworks and query engines with different native
formats.

**Alternatives.**

- **One format (e.g. JSON or a single binary format).**
  - Pros: least code; one path to test.
  - Cons: forces every consumer to convert; loses framework-native performance
    (e.g. `tf.data` wants TFRecord, Spark/Athena want Parquet).

**Decision.** Emit **TFRecord** (masked CRC32C framing for `tf.data`),
**WebDataset** tar shards, **Apache Arrow** columnar batches, and **Parquet**
result files. Each targets a real consumer and is produced with correct framing
so it interoperates without a conversion step. The cost is more encoders to
maintain and test; justified because "queryable directly by Athena/Spark/DuckDB"
and "streams into `tf.data`" are concrete requirements, not hypotheticals.

---

## 7. Cross-cutting

### 7.1 Go as the implementation language

**Context.** The platform is concurrency-heavy (schedulers, pollers, failure
detectors, pipelines) and must be operationally simple to deploy.

**Alternatives.**

- **Rust.**
  - Pros: no GC pauses, strongest memory-safety guarantees, top-tier performance.
  - Cons: slower to write; async ecosystem heavier; concurrency ergonomics steeper
    for a system that is mostly coordination, not raw compute.
- **JVM (Java/Kotlin/Scala).**
  - Pros: mature distributed-systems libraries; strong tooling.
  - Cons: GC tuning at scale; heavier runtime and memory footprint; fatter deploy
    artifacts.
- **Python.**
  - Pros: fastest to prototype; native to the ML ecosystem.
  - Cons: the GIL and interpreter overhead make it a poor fit for the concurrent
    control plane (it's used for the *out-of-repo* CLIP/GPU job instead).

**Decision.** **Go.** Goroutines and channels fit a coordination-heavy system;
static binaries make deployment trivial (single artifact, no runtime); the race
detector and pprof are first-class; and GC pauses are sub-millisecond at this
workload (visible only as rare p100 outliers in the benchmarks). The trade-off vs.
Rust is accepting a GC and slightly less compile-time safety in exchange for much
faster iteration — the right call for a control plane where clarity and
concurrency ergonomics matter more than shaving microseconds. (The ring lookup is
still zero-allocation on the hot path, so the GC rarely has anything to do there.)

### 7.2 Monorepo with internal packages

**Context.** The codebase spans storage, scheduling, cluster membership, sandbox,
consensus, and several binaries.

**Alternatives.**

- **Multiple repos / modules (one per component).**
  - Pros: independent versioning; enforced boundaries.
  - Cons: cross-cutting changes span repos; version-skew friction; heavy for a
    single team/project.
- **A flat package layout (everything importable).**
  - Pros: simplest.
  - Cons: no boundary between public API and implementation detail; anything can
    depend on anything.

**Decision.** A **single module with `internal/` packages** and thin `cmd/`
binaries. `internal/` enforces that implementation packages can't be imported
outside the module, keeping the public surface honest, while the monorepo makes
cross-cutting changes (like the module-path rename, or threading a new field
through scheduler → coordinator → worker) a single atomic change. Binaries in
`cmd/` stay thin — wiring only — so the logic lives in testable packages. The cost
is that all components version together; acceptable and desirable for a cohesive
platform developed as one unit.

---

## Recurring principles

A few themes run through the decisions above:

- **Fail closed.** Unknown tenants get no quota and no admission; gRPC refuses to
  serve without auth; the sandbox can't be disabled by user input.
- **Make the hot path lock-free or lock-light.** Zero-allocation ring lookups;
  the two-phase commit copies outside the scheduler lock; diagnostics cost one
  atomic load when off.
- **Prefer provable safety to hopeful safety.** Work stealing steals only the
  provably-untouched tail; result commit is idempotent in the destination key.
- **Document the limitation, don't hide it.** Effectively- (not truly-) exactly-
  once; isolation-over-utilization in admission; AP's duplicate-execution window.
  Each ships with its honest boundary and the concrete next step to close it.
