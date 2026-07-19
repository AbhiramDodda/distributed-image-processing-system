# Strict Correctness Audit — Petabyte-Scale Image Processing Platform

**Auditor role:** adversarial verifier. Goal — try to *break* every falsifiable claim
in `README.md`, hunt for races/deadlocks/logical bugs down to the runtime level, and
record exactly what was run, why, and what it showed.

**Date:** 2026-07-19 · **Machine:** i7-13620H (16 threads), 13.3 GiB RAM, Arch Linux
kernel 6.19, **Go 1.26.0** · Repo at commit `04e51b1` (clean tree).

**Bottom line:** the platform is unusually honest and unusually well-tested. Every
*mechanism* claim I could exercise — work-stealing exactly-once tiling, chaos failure
resilience, JWT alg-confusion defense, race-freedom — **held under adversarial load,
including tests I wrote myself.** The defects found are **not** correctness bugs in the
distributed core; they are **one quantitatively wrong performance claim** (ring
variance) and a handful of **documentation drift** items. Details and severities below.

---

## 1. Method & why each technique was used

| Technique | Why (what class of bug it catches) | Result |
|---|---|---|
| `go build ./...` / `go vet ./...` | Compile + suspicious-construct baseline | Clean |
| `go test -count=1 ./...` | Functional correctness, defeat cache | 293 tests / 22 pkgs green, 3.54 s |
| `go test -race -count=1 ./...` | **Data races** (happens-before violations on shared state) | Green |
| `go test -race -count=30` on the 7 concurrency pkgs | **Flaky/rare races** that a single pass misses (scheduler locks, membership, raft) | Green |
| `PETABYTE_DIAG=1 -race -count=10` | Activates the project's **runtime invariant asserts + lock-order cycle detection** — catches *logical* races and *latent deadlocks* the race detector cannot | No assert fired, no lock-order warning |
| **Author-independent adversarial property test** (I wrote it) | The authored tests check steal / rebalance / renew / report *separately*. I interleave all four randomly and assert the global tiling invariant after **every** mutation — this is the real "no double-processing / no gap" safety net | Held over 40 seeds × 3000 ops + 24 concurrent goroutines |
| **Live end-to-end `steal-demo.sh`** against real MinIO | Proves the exactly-once work-stealing claim on the *production HTTP path*, not in-memory | 6000 images processed **exactly once**, 54 steals |
| **Live end-to-end `chaos-demo.sh`** (kill 3/10 workers) | Proves the "zero tasks lost" failure-resilience headline live | 256/256 both runs, 0 lost, 6 rebalanced |
| Source audit of `auth/jwt.go`, `scheduler/*.go`, `worker.go`, `metadata/index.go` | Claims that no test can prove (constant-time compare, alg rejection, lease-bound honoring) | See §3 |
| Empirical **ring variance measurement** (I wrote it) | The one quantitative distribution claim ("±5%") | **Refuted — see F1** |
| `go test ./internal/perf -run TestPerfReport` | Reproduce the published latency/throughput table | Reproduces; platform is *faster* than published |

All commands were run from the repo root. Temporary test files I added
(`zz_adversarial_test.go`, `zz_variance_test.go`) were **removed after use**; their full
source is archived in the analysis scratch dir and reproduced in §6 so the results can be
re-derived. The working tree is left clean.

---

## 2. Claims VERIFIED

### 2.1 Test suite scale & race-freedom
- README: *"200+ tests across 22 packages … completes in a few seconds."*
  **Verified:** 293 `Test*` functions across exactly 22 test-bearing packages; plain
  `-count=1` suite = **3.54 s**.
- README: *"run the suite under the race detector … 16 workers polling 256 tasks
  (no task assigned twice), heartbeats racing the detector, ring churn racing lookups."*
  **Verified:** full suite green under `-race`; the 7 concurrency-critical packages
  (`scheduler`, `cluster`, `coordinator`, `consensus`, `consensus/raftgrpc`, `admission`,
  `diag`, `effect`) stayed green at **`-count=30`** under `-race`. **Zero data races.**

### 2.2 Runtime diagnostics (`PETABYTE_DIAG=1`) find nothing wrong
- README: *"an inconsistent acquisition order (a latent deadlock) is reported … a DFS
  cycle check, before it ever actually hangs."*
  **Verified working and clean:** with diagnostics on and `-race`, no invariant assert
  fired and no lock-order cycle was reported across the whole concurrency suite. The
  scheduler's `scheduler.mu → admission.mu` order (documented at `scheduler.go:60`) is
  acyclic as claimed.

### 2.3 Work-stealing exactly-once / no-gap / no-overlap  ← the subtlest claim
This is the crown-jewel correctness claim (`RangeStart ≤ Frontier ≤ Granted ≤ RangeEnd`,
"steal only the provably-untouched tail `[Granted, RangeEnd)`", "ranges tile the shard").

- **Source audit:** the safety argument is airtight *and* the real worker honors it.
  `scheduler.grantLocked` is the only place `Granted` advances; `stealLocked` splits at
  `Granted + tail/2` and asserts `stolen.RangeStart ≥ victim.Granted` and contiguity;
  crucially, `worker.go:224-247` shows the worker loop **stops at `bound` (= `Granted`)**
  and winds down when `Stolen` — so the "no processing past the leased bound" premise the
  whole proof rests on is actually implemented, not just assumed.
- **My independent adversarial test:** randomized interleaving of poll/steal, renew (with
  realistic frontier advancement), dead-worker rebalance, and result-report, asserting the
  **global tiling invariant after every single mutation** — 40 seeds × 3000 ops, plus a
  24-goroutine concurrent variant, under `-race` + `PETABYTE_DIAG=1`. **The invariant was
  never violated.** (The authored tests check these operations in isolation; this checks
  their cross-product.)
- **Live proof (`steal-demo.sh`, real MinIO):** one shard = 6000 objects, `lease_chunk=200`.
  Result: **6000 images processed exactly once — 3426 by w0 + 2574 by w1 = 6000**, 54
  steals, first steal handed off `[3200,6000)`, victim wound down at frontier 400.
  README says a representative run does "~53 steals / 3447+2553" — reproduced in kind.

### 2.4 Failure resilience — "zero tasks lost"
- **Live proof (`chaos-demo.sh`, real MinIO, default synthetic 16000-object corpus):**
  10 workers, `kill -9` 3 of them (w7/w8/w9) mid-flight.
  ```
  baseline:  done=256/256  failed=0
  chaos:     done=256/256  failed=0   tasks_lost=0   rebalanced=6
  latency:   2795 ms → 5186 ms (Δ +2391 ms from detection + reprocessing)
  ```
  **Zero tasks lost**, phi-accrual detector marked the 3 dead, `RebalanceWorker` requeued
  their tasks, idempotent `FinalResultKey` made re-runs overwrite. Matches the mechanism
  described for the 1.28M ImageNet headline (which I did not re-run — see §5).

### 2.5 JWT auth — alg-confusion defense (`internal/auth/jwt.go`)
- README: *"constant-time signature check, hard-rejects the alg-confusion/'none' attack,
  exp/nbf with leeway."*
  **Verified by source audit:** `Parse` rejects any `alg != "HS256"` (line 94) *before*
  verifying, uses `hmac.Equal` (constant-time, line 103), verifies the signature **before**
  unmarshalling/trusting claims, and applies `exp`/`nbf` with `leeway`. The "none" and
  algorithm-swap attacks are both closed. No weakness found.

### 2.6 Fixed bugs are actually fixed; performance reproduces
- **SQLite DSN bug** (README "Bug found by this run"): `metadata/index.go:45` uses the
  correct `modernc.org/sqlite` form `?_pragma=journal_mode(WAL)&_pragma=busy_timeout(...)`.
  Fixed as claimed.
- **Perf table** regenerates cleanly; on this machine the platform is **faster** than the
  committed numbers (e.g. ShardKey p50 **231 ns measured vs 448 ns published**), so the
  published latencies are conservative, not inflated.
- No `TODO`/`FIXME`/`panic(` in non-test production code.

---

## 3. FINDINGS (defects & discrepancies)

### F1 — **`~±5%` ring variance claim is quantitatively false**  · Severity: **Medium** (accuracy)
`ring.go:20` and `README.md:53` both claim *"150 vnodes … distribution variance ~±5%."*
I measured the actual key→node peak deviation with 150 vnodes (200k keys/run):

| nodes | measured peak deviation from ideal |
|---|---|
| 3  | **9.3 %** |
| 5  | 12.0 % |
| 10 | 10.4 % |
| 20 | 20.0 % |
| 50 | 24.1 % |

Even at **3 nodes** — the most favorable count, and the only one the authored test
`TestRing_distribution_variance` actually uses — deviation is ~9 %, already ~2× the
claim. This matches consistent-hashing theory: per-node load has coefficient of variation
≈ `1/√150 ≈ 8 %`, so peak deviation across a real fleet is 10–25 %, not 5 %.

Note the authored test hides this: it asserts only `±10%` **and** pins the node count to 3
(`ring_test.go:59-60`). At ≥5 nodes that same assertion would fail. **Recommendation:**
restate the claim as "≈8 % per-node standard deviation; ~10–25 % peak depending on cluster
size," or raise the vnode count if ±5 % is actually required (≈600 vnodes/node gets peak
near ±5 %).

### F2 — **`busy_timeout` value is inconsistent across the README**  · Severity: **Low** (doc)
Code uses `busy_timeout(30000)` (30 s). README limitations section (line 396) agrees
("30 s"). But the Performance "Bug found" note (line 601) states the fix was
`busy_timeout(5000)`. The `5000` is wrong relative to the shipped code and to the README's
own other mention. **Fix:** change line 601 to `30000`.

### F3 — **Go version drift in the perf environment line**  · Severity: **Low** (doc)
`README.md:546` says the benchmarks were taken on **"Go 1.25"**, but the prerequisites
(line 422), the test-environment table (line 340), `go.mod` (`go 1.26`), and the actual
toolchain are all **Go 1.26**. Cosmetic but it undercuts reproducibility precision.

### F4 — **Default `configs/worker.yaml` ships the port the README warns against**  · Severity: **Low** (footgun)
`configs/worker.yaml:13` sets `port: 9001`. The README itself (lines 462-464) warns
"Do **not** run a worker on 9001 … The default `configs/worker.yaml` uses 9001, which
collides" with the MinIO console. The doc acknowledges it, but the default config still
ships the colliding value — a copy-paste of the Quick Start (`-port 9001`, line 621) will
collide out of the box. **Fix:** default the config to 9101 (or drop the console off 9001).

### F5 — **Worker computes an unused, stale `FinalResultKey`**  · Severity: **Very Low** (code smell)
`worker.go:268` builds `OutputKey: FinalResultKey(jobID, shard, {RangeEnd, Split})` from
the **assignment-time** range and embeds it in the staged JSON body. After a mid-flight
steal the task's real range shrinks, so this embedded key can be stale — but it is never
used to commit: the scheduler recomputes the authoritative `finalKey` from current state
(`scheduler.go:289`), and the key actually returned to the coordinator is the task-scoped
`StagingResultKey`. Harmless today, but it invites a future reader to believe the worker
chooses the final key. **Fix:** drop the field or compute it from live state.

### F6 — **Exactly-once residual gap** (already disclosed) · Severity: **Informational**
The README honestly states (lines 397-399, Future Plans) that the result-copy and done-mark
are not one atomic replicated log entry, so a coordinator crash in a narrow window can
re-run a committed task (safe only because commits are idempotent), and that platform side
effects are *at-least-once + idempotent-key* ("effectively-once"), not truly exactly-once.
My source audit confirms this is an accurate self-description, not an overclaim: between the
Phase-1 unlock and Phase-4 lock in `ReportResult`, two concurrent duplicate reports can both
run `Commit`/`Decide`/`Apply`, and safety rests entirely on those three being idempotent —
which they are. **No action; noted for completeness.**

---

## 4. What was NOT reproduced (scope honesty)
- **1.28M-object ImageNet chaos run** (~1.5 h ingest): not re-run. The *mechanism* is
  identical to the 16k chaos run I did verify live, and the README is explicit that this
  proves object-count scaling, not petabyte byte-throughput.
- **CLIP eval accuracy numbers** (P@k on CIFAR-10): needs a Torch/HuggingFace download and
  GPU-less CPU embedding; not run. The Go-side `internal/vsearch` k-NN tests pass under
  `-race`.
- **K8s operator / Ray / gVisor sandbox live paths:** need a cluster + Docker + `runsc`;
  the README already marks these "not automated." Their unit tests (fake API servers,
  in-memory store, fake runtime) pass.
- **Multi-process Raft over a real network:** only the in-process + localhost-gRPC
  `raftgrpc`/`raft_test.go` paths were exercised (they pass at `-count=30 -race`).

---

## 5. Verdict
No data races, no deadlocks, no lock-order cycles, and no work-stealing safety violation
could be provoked — including by an adversarial property test and two live end-to-end
demos against real MinIO. The distributed correctness core is **sound and honestly
documented**. The actionable defects are **one inaccurate quantitative claim (F1, ring
variance)** and **four low-severity documentation/config nits (F2–F5)**. Fixing F1's wording
and F4's default config would remove the only two items a careful reader could call
misleading.

---

## 6. Reproduction — exact commands & the two tests I wrote

```sh
# Baseline
go build ./... && go vet ./...
go test -count=1 ./...                       # 293 tests / 22 pkgs
go test -race -count=1 ./...
go test -race -count=30 ./internal/scheduler/... ./internal/cluster/... \
  ./internal/coordinator/... ./internal/consensus/... ./internal/admission/... \
  ./internal/diag/... ./internal/effect/...
PETABYTE_DIAG=1 go test -race -count=10 ./internal/scheduler/... \
  ./internal/coordinator/... ./internal/cluster/... ./internal/admission/... ./internal/diag/...

# Perf table
go test ./internal/perf/ -run TestPerfReport -v

# Live demos (need ~/petabyte-demo MinIO binaries; start MinIO on :9000 first)
bash scripts/steal-demo.sh          # -> 6000 processed exactly once, ~54 steals
bash scripts/chaos-demo.sh          # -> 256/256 both runs, tasks_lost=0
```

### 6.1 `internal/cluster/zz_variance_test.go` (measures F1)
```go
package cluster

import ("fmt"; "testing")

func TestZZ_MeasureVariance(t *testing.T) {
	for _, n := range []int{3, 5, 10, 20, 50} {
		r := NewRing(150)
		for i := 0; i < n; i++ { r.Add(fmt.Sprintf("node-%d", i)) }
		const samples = 200000
		hits := map[string]int{}
		for i := 0; i < samples; i++ {
			id, _ := r.Lookup(fmt.Sprintf("key-%d", i)); hits[id]++
		}
		expected := float64(samples) / float64(n)
		var maxDevPct float64
		for _, h := range hits {
			dev := (float64(h) - expected) / expected * 100
			if dev < 0 { dev = -dev }
			if dev > maxDevPct { maxDevPct = dev }
		}
		t.Logf("nodes=%2d  max deviation from ideal = %.1f%%", n, maxDevPct)
	}
}
```

### 6.2 `internal/scheduler/zz_adversarial_test.go` (steal/rebalance/renew/report tiling fuzz)
Randomized single-thread run (40 seeds × 3000 ops) asserting the global tiling invariant
after every mutation, plus a 24-goroutine concurrent variant checked under `-race`. Full
source archived alongside this audit in the analysis scratch directory. Key invariant
asserted after *every* op:

> For all tasks of the job (any status), sorted by `RangeStart`, the ranges are contiguous
> from 0, cover exactly `[0,total)` with no gap/overlap, and each task's `Frontier`/`Granted`
> lie within `[RangeStart, RangeEnd]`.

Both tests were deleted after the run; the tree is clean.
```
