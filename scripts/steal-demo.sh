#!/usr/bin/env bash
#
# steal-demo.sh — live end-to-end proof of intra-shard work stealing.
#
# One whole shard is a single task. A giant shard is therefore one worker
# grinding alone (the intra-shard tail). This demo forces that situation and
# shows a second, idle worker steal the busy worker's *un-granted* tail over
# real HTTP against real MinIO objects — the production path the unit and
# over-HTTP tests exercise in-memory.
#
# It works by concentrating thousands of images into ONE shard (gen-images
# -shard mines filenames whose hash lands in the target shard) and setting a
# small lease_chunk so the shard becomes splittable after the first renewal,
# leaving a large tail an idle worker can reclaim.
#
# Requires the local demo harness at ~/petabyte-demo (MinIO + mc binaries); see
# the "Local demo harness" note. Everything else is built from this repo.
#
# Usage: scripts/steal-demo.sh
set -euo pipefail

# --- knobs -------------------------------------------------------------------
DEMO="${DEMO:-$HOME/petabyte-demo}"
SHARD="${SHARD:-7a}"            # every image is mined into this one shard
COUNT="${COUNT:-6000}"         # images in the shard (bigger => wider steal window)
DIM="${DIM:-64}"               # small images keep disk + generation cheap
LEASE_CHUNK="${LEASE_CHUNK:-200}"
DATASET="train"
COORD_URL="http://localhost:8090"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$DEMO/steal-run"          # scratch: configs, logs, WAL, metadata for this run
mkdir -p "$RUN"

MC="$DEMO/bin/mc"
MINIO="$DEMO/bin/minio"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# Build binaries once and run them directly. Running `go run` instead would fork
# a compiled child the trap can't reach, orphaning the coordinator/workers.
BIN="$RUN/bin"
say "building binaries"
go build -o "$BIN/gen-images" "$REPO/cmd/gen-images"
go build -o "$BIN/ingest" "$REPO/cmd/ingest"
go build -o "$BIN/coordinator" "$REPO/cmd/coordinator"
go build -o "$BIN/worker" "$REPO/cmd/worker"

# --- 1. MinIO ----------------------------------------------------------------
if ! curl -sf -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null; then
  say "starting MinIO"
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    "$MINIO" server "$DEMO/minio-data" --address :9000 --console-address :9001 \
    >"$RUN/minio.log" 2>&1 &
  pids+=($!)
  for _ in $(seq 1 50); do
    curl -sf -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null && break
    sleep 0.2
  done
fi
"$MC" alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
"$MC" mb --ignore-existing local/petabyte-images >/dev/null 2>&1

# --- 2. generate a single hot shard -----------------------------------------
IMG_DIR="$RUN/images"
rm -rf "$IMG_DIR"
say "generating $COUNT images, all in shard $SHARD"
"$BIN/gen-images" -out "$IMG_DIR" -count "$COUNT" -dim "$DIM" -shard "$SHARD"

# --- 3. ingest into MinIO ----------------------------------------------------
cat >"$RUN/server.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
metadata: {db_path: "$RUN/metadata.db"}
server: {metadata_db_path: "$RUN/metadata.db"}
ingestion: {workers: 16, batch_size: 100, max_file_size_mb: 500}
logging: {level: warn, format: text}
YAML
say "ingesting into shard $SHARD"
"$BIN/ingest" -config "$RUN/server.yaml" -dir "$IMG_DIR" -dataset "$DATASET"
INGESTED=$("$MC" ls --recursive "local/petabyte-images/$DATASET/$SHARD/" | wc -l)
echo "shard $SHARD now holds $INGESTED objects in MinIO"

# --- 4. coordinator with a small lease_chunk --------------------------------
rm -rf "$RUN/coordinator-wal"
cat >"$RUN/coordinator.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
coordinator:
  host: 0.0.0.0
  port: 8090
  dispatch_interval: 200ms
  lease_chunk: $LEASE_CHUNK
  wal_dir: "$RUN/coordinator-wal"
metrics: {enabled: false}
logging: {level: info, format: text}
YAML
say "starting coordinator (lease_chunk=$LEASE_CHUNK)"
"$BIN/coordinator" -config "$RUN/coordinator.yaml" >"$RUN/coordinator.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 50); do
  curl -sf -o /dev/null "$COORD_URL/v1/metrics/pending" 2>/dev/null && break
  sleep 0.2
done

# --- 5. worker config --------------------------------------------------------
cat >"$RUN/worker.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
worker: {coordinator_url: "$COORD_URL", host: 0.0.0.0, port: 9101, concurrency: 1, poll_interval: 100ms, heartbeat_interval: 2s}
logging: {level: info, format: text}
YAML

# --- 6. submit a single-shard job -------------------------------------------
say "submitting a job for shard $SHARD only (one task, one shard)"
curl -sf -X POST "$COORD_URL/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d "{\"dataset\":\"$DATASET\",\"algorithm\":\"resnet\",\"shards\":[\"$SHARD\"]}" | tee "$RUN/submit.json"; echo
JOB=$(grep -oP '"job_id":"\K[^"]+' "$RUN/submit.json")

# --- 7. w0 takes the shard; give it a head start to claim + first-renew ------
say "starting w0 (grabs the whole shard and starts grinding)"
"$BIN/worker" -config "$RUN/worker.yaml" -id w0 -port 9101 >"$RUN/w0.log" 2>&1 &
pids+=($!)
# Wait until w0 has claimed the task and reported the shard size (first renew),
# which is what makes the shard splittable.
for _ in $(seq 1 50); do
  grep -q 'task started' "$RUN/w0.log" 2>/dev/null && break
  sleep 0.1
done
sleep 0.3

# --- 8. w1 is idle -> its poll steals w0's un-granted tail -------------------
say "starting w1 (idle: no pending queue, so its poll must STEAL w0's tail)"
"$BIN/worker" -config "$RUN/worker.yaml" -id w1 -port 9102 >"$RUN/w1.log" 2>&1 &
pids+=($!)

# --- 9. wait for the whole (now-split) shard to finish ----------------------
say "waiting for the job to complete (both workers draining the split shard)"
STATUS=""
for _ in $(seq 1 600); do
  STATUS=$(curl -sf "$COORD_URL/v1/jobs/$JOB" | grep -oP '"status":"\K[^"]+' || true)
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then break; fi
  sleep 0.2
done

# --- 10. report --------------------------------------------------------------
steals=$(grep -c 'stole task tail' "$RUN/coordinator.log" || true)
w0img=$(grep 'task done' "$RUN/w0.log" | grep -oP 'images=\K[0-9]+' | awk '{s+=$1} END{print s+0}')
w1img=$(grep 'task done' "$RUN/w1.log" | grep -oP 'images=\K[0-9]+' | awk '{s+=$1} END{print s+0}')
total=$((w0img + w1img))

echo
echo "======================================================================"
echo "  WORK-STEALING DEMO RESULT"
echo "======================================================================"
echo "  one shard ($SHARD) = one task = $INGESTED objects, lease_chunk=$LEASE_CHUNK"
echo "  job status:                 $STATUS"
echo "  steals (coordinator):       $steals"
echo "  images processed by w0:     $w0img"
echo "  images processed by w1:     $w1img"
echo "  ---------------------------------"
echo "  total images processed:     $total  (expected $INGESTED)"
if [ "$total" -eq "$INGESTED" ]; then
  echo "  => every image processed EXACTLY ONCE: no gap, no double-processing"
else
  echo "  => MISMATCH: coverage is not exactly-once (investigate)"
fi
echo "----------------------------------------------------------------------"
echo "  first steal (whole un-granted tail handed off):"
grep 'stole task tail' "$RUN/coordinator.log" | head -1 | sed 's/^/    /'
echo "  a victim winding down after losing its tail:"
grep 'lease tail stolen' "$RUN/w0.log" "$RUN/w1.log" | head -1 | sed 's/^/    /'
echo "======================================================================"

# Fail the script (non-zero exit) unless a steal happened AND coverage is exact.
[ "$steals" -gt 0 ] && [ "$total" -eq "$INGESTED" ]
