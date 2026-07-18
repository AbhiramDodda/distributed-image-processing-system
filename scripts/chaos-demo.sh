#!/usr/bin/env bash
#
# chaos-demo.sh — worker-failure chaos test with before/after latency.
#
# Brings up a coordinator + 10 worker nodes against real MinIO, then runs the
# SAME job twice:
#   1. baseline — all 10 workers healthy, measure end-to-end latency.
#   2. chaos    — kill -9 a few workers mid-flight, measure latency + loss.
#
# It answers three questions with real numbers:
#   * how many tasks/jobs are LOST to the failures?  (expected: 0 — the phi-accrual
#     detector marks a silent worker dead, the coordinator requeues its in-flight
#     tasks, and the idempotent result key means a re-run overwrites, never
#     duplicates or drops)
#   * how many tasks were REBALANCED off the dead workers?  (/v1/metrics/tasks)
#   * what's the LATENCY difference with vs without failures?  (detection window +
#     whole-shard reprocessing of the interrupted tasks)
#
# Health + latency come from /v1/cluster/nodes and the new /v1/metrics/tasks.
# Requires the local demo harness at ~/petabyte-demo (MinIO + mc binaries).
set -euo pipefail

# --- knobs -------------------------------------------------------------------
DEMO="${DEMO:-$HOME/petabyte-demo}"
COUNT="${COUNT:-16000}"        # images spread uniformly across all 256 shards
DIM="${DIM:-64}"
WORKERS="${WORKERS:-10}"
KILL="${KILL:-3}"              # workers to kill mid-chaos-run
CONCURRENCY="${CONCURRENCY:-8}"    # parallel tasks per worker (I/O parallelism)
# lease_chunk bounds work-stealing granularity. For a UNIFORM corpus (all 256
# shards ~equal) stealing has no straggler to help and just fragments big shards
# into micro-tasks that each re-list their shard -> quadratic listing. Set it
# above the largest shard so every shard is granted whole and nothing splits.
# (Drop it back to ~1000 to *exercise* stealing on a deliberately skewed corpus.)
LEASE_CHUNK="${LEASE_CHUNK:-100000}"
# A dedicated, uniform dataset so every shard-task is comparable -- keeps the
# latency numbers clean and isolated from other demos' data.
DATASET="${DATASET:-chaos}"
# SKIP_INGEST=1 reuses whatever is already in MinIO for $DATASET and jumps straight
# to the job/chaos phase -- lets you re-run the test without re-ingesting a huge
# corpus (a 1.28M-object ingest takes ~1.5h; the objects persist in minio-data).
SKIP_INGEST="${SKIP_INGEST:-}"
COORD_URL="http://localhost:8090"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$DEMO/chaos-run"
mkdir -p "$RUN"
BIN="$RUN/bin"
MC="$DEMO/bin/mc"; MINIO="$DEMO/bin/minio"

declare -A wpid                # worker id -> pid
pids=()
cleanup() {
  for p in "${wpid[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT
say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
now_ms() { date +%s%3N; }
jget() { grep -oP "\"$2\":\s*\K[0-9.]+" <<<"$1" | head -1; }   # numeric field
jstr() { grep -oP "\"$2\":\"\K[^\"]+" <<<"$1" | head -1; }      # string field

# --- build -------------------------------------------------------------------
say "building binaries"
for b in gen-images ingest coordinator worker; do go build -o "$BIN/$b" "$REPO/cmd/$b"; done

# --- MinIO -------------------------------------------------------------------
if ! curl -sf -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null; then
  say "starting MinIO"
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    "$MINIO" server "$DEMO/minio-data" --address :9000 --console-address :9001 >"$RUN/minio.log" 2>&1 &
  pids+=($!)
  for _ in $(seq 1 50); do curl -sf -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null && break; sleep 0.2; done
fi
"$MC" alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
"$MC" mb --ignore-existing local/petabyte-images >/dev/null 2>&1

# --- generate + ingest a dataset across all shards --------------------------
cat >"$RUN/server.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
metadata: {db_path: "$RUN/metadata.db"}
server: {metadata_db_path: "$RUN/metadata.db"}
ingestion: {workers: 16, batch_size: 100, max_file_size_mb: 500}
logging: {level: warn, format: text}
YAML
# Corpus: ingest a real image directory (INGEST_DIR, e.g. dumped CIFAR-10) if given,
# otherwise generate a synthetic set. Either way the filenames shard across all 256.
have=0
if [ -z "$SKIP_INGEST" ]; then
  have=$("$MC" ls --recursive "local/petabyte-images/$DATASET/" 2>/dev/null | wc -l)
fi
if [ -n "$SKIP_INGEST" ]; then
  say "SKIP_INGEST set -- reusing existing '$DATASET' objects in MinIO"
elif [ "${INGEST_DIR:-}" != "" ]; then
  want=$(find "$INGEST_DIR" -type f | wc -l)
  if [ "$have" -lt "$want" ]; then
    say "ingesting $want real images from $INGEST_DIR into dataset '$DATASET'"
    "$BIN/ingest" -config "$RUN/server.yaml" -dir "$INGEST_DIR" -dataset "$DATASET"
  fi
elif [ "$have" -lt "$COUNT" ]; then
  say "generating + ingesting $COUNT synthetic images across all 256 shards"
  IMG_DIR="$RUN/images"; rm -rf "$IMG_DIR"
  "$BIN/gen-images" -out "$IMG_DIR" -count "$COUNT" -dim "$DIM"
  "$BIN/ingest" -config "$RUN/server.yaml" -dir "$IMG_DIR" -dataset "$DATASET"
fi
INGESTED=$("$MC" ls --recursive "local/petabyte-images/$DATASET/" | wc -l)
echo "corpus: $INGESTED objects in MinIO (dataset '$DATASET')"

# --- coordinator with a fast failure detector -------------------------------
# Short suspect/dead timeouts so a killed worker is detected and its tasks
# requeued within a few seconds rather than the 20s production default.
rm -rf "$RUN/coordinator-wal"
cat >"$RUN/coordinator.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
coordinator:
  host: 0.0.0.0
  port: 8090
  suspect_timeout: 2s
  dead_timeout: 4s
  heartbeat_interval: 1s
  dispatch_interval: 100ms
  task_max_retries: 3
  lease_chunk: $LEASE_CHUNK
  wal_dir: "$RUN/coordinator-wal"
metrics: {enabled: false}
logging: {level: info, format: text}
YAML
say "starting coordinator (suspect=2s dead=4s)"
"$BIN/coordinator" -config "$RUN/coordinator.yaml" >"$RUN/coordinator.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 50); do curl -sf -o /dev/null "$COORD_URL/v1/metrics/pending" 2>/dev/null && break; sleep 0.2; done

# --- start N workers ---------------------------------------------------------
cat >"$RUN/worker.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
worker: {coordinator_url: "$COORD_URL", host: 0.0.0.0, port: 0, concurrency: $CONCURRENCY, poll_interval: 100ms, heartbeat_interval: 1s}
logging: {level: warn, format: text}
YAML
say "starting $WORKERS workers"
for i in $(seq 0 $((WORKERS-1))); do
  "$BIN/worker" -config "$RUN/worker.yaml" -id "w$i" -port $((9200+i)) >"$RUN/w$i.log" 2>&1 &
  wpid[w$i]=$!
done
# wait until all N have registered as active
for _ in $(seq 1 50); do
  a=$(jget "$(curl -sf "$COORD_URL/v1/metrics/pending")" active_workers || echo 0)
  [ "${a:-0}" -ge "$WORKERS" ] && break; sleep 0.2
done
echo "active workers: $(jget "$(curl -sf "$COORD_URL/v1/metrics/pending")" active_workers)"

# submit a whole-dataset job and return its id
submit_job() {
  local resp; resp=$(curl -sf -X POST "$COORD_URL/v1/jobs" -H 'Content-Type: application/json' \
    -d "{\"dataset\":\"$DATASET\",\"algorithm\":\"scan\"}")
  jstr "$resp" job_id
}
# poll until done_tasks+failed_tasks == total_tasks; echo "done failed total"
wait_job() {
  local id="$1" resp
  for _ in $(seq 1 9000); do
    resp=$(curl -sf "$COORD_URL/v1/jobs/$id" || true)
    local st; st=$(jstr "$resp" status)
    if [ "$st" = "completed" ] || [ "$st" = "failed" ]; then
      echo "$(jget "$resp" done_tasks) $(jget "$resp" failed_tasks) $(jget "$resp" total_tasks)"; return
    fi
    sleep 0.1
  done
  echo "TIMEOUT"
}

# =============================================================================
# RUN 1 — baseline (no failures)
# =============================================================================
say "RUN 1: baseline — all $WORKERS workers healthy"
JOB1=$(submit_job)
t0=$(now_ms); read d1 f1 tot1 <<<"$(wait_job "$JOB1")"; base_ms=$(( $(now_ms) - t0 ))
M1=$(curl -sf "$COORD_URL/v1/metrics/tasks")
echo "baseline: done=$d1 failed=$f1 total=$tot1  latency=${base_ms}ms"

# =============================================================================
# RUN 2 — chaos (kill $KILL workers mid-flight)
# =============================================================================
say "RUN 2: chaos — kill $KILL workers after the job is ~1/3 done"
JOB2=$(submit_job)
t0=$(now_ms)
threshold=$(( tot1 / 3 ))
killed=""
for _ in $(seq 1 9000); do
  resp=$(curl -sf "$COORD_URL/v1/jobs/$JOB2" || true)
  done=$(jget "$resp" done_tasks); done=${done:-0}
  st=$(jstr "$resp" status)
  if [ -z "$killed" ] && [ "$done" -ge "$threshold" ]; then
    for i in $(seq $((WORKERS-KILL)) $((WORKERS-1))); do
      say "  killing w$i (pid ${wpid[w$i]}) at done=$done/$tot1"
      kill -9 "${wpid[w$i]}" 2>/dev/null || true; unset "wpid[w$i]"
      killed="$killed w$i"
    done
  fi
  if [ "$st" = "completed" ] || [ "$st" = "failed" ]; then break; fi
  sleep 0.1
done
chaos_ms=$(( $(now_ms) - t0 ))
read d2 f2 tot2 <<<"$(wait_job "$JOB2")"
M2=$(curl -sf "$COORD_URL/v1/metrics/tasks")
NODES=$(curl -sf "$COORD_URL/v1/cluster/nodes")
dead=$(grep -o '"Dead"' <<<"$NODES" | wc -l)
active_after=$(jget "$(curl -sf "$COORD_URL/v1/metrics/pending")" active_workers)
rebal=$(jget "$M2" rebalances)
lost=$(( tot2 - d2 ))

# =============================================================================
# report
# =============================================================================
echo
echo "======================================================================"
echo "  CHAOS TEST RESULT — $WORKERS workers, killed $KILL mid-flight"
echo "======================================================================"
echo "  corpus: $INGESTED objects, $tot1 tasks (256 shards; large shards may split under work-stealing)"
echo "  ---- worker health (after chaos) ----------------------------------"
echo "  killed:$killed    active_now=$active_after    marked_dead=$dead"
echo "  ---- job loss -----------------------------------------------------"
echo "  baseline:  done=$d1/$tot1  failed=$f1"
echo "  chaos:     done=$d2/$tot2  failed=$f2   tasks_lost=$lost   rebalanced=$rebal"
if [ "$d2" -eq "$tot2" ] && [ "$f2" -eq 0 ]; then
  echo "  => ZERO tasks lost: every shard completed exactly once despite $KILL deaths"
else
  echo "  => LOSS DETECTED (investigate): done=$d2 failed=$f2 total=$tot2"
fi
echo "  ---- latency (end-to-end job wall time) ---------------------------"
echo "  without failures: ${base_ms} ms"
echo "  with failures:    ${chaos_ms} ms   (Δ +$(( chaos_ms - base_ms )) ms from detection + reprocessing)"
echo "  task latency p50/p95/max (baseline):  $(jget "$M1" latency_p50_ms) / $(jget "$M1" latency_p95_ms) / $(jget "$M1" latency_max_ms) ms"
echo "  task latency p50/p95/max (post-chaos): $(jget "$M2" latency_p50_ms) / $(jget "$M2" latency_p95_ms) / $(jget "$M2" latency_max_ms) ms"
echo "======================================================================"

# Fail the script unless chaos completed with zero loss and a real rebalance.
[ "$d2" -eq "$tot2" ] && [ "$f2" -eq 0 ] && [ "${rebal:-0}" -gt 0 ]
