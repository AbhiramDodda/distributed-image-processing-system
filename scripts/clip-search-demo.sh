#!/usr/bin/env bash
#
# clip-search-demo.sh — end-to-end CLIP image similarity search.
#
# Runs the whole pipeline: a Python encoder turns an image corpus into a Parquet
# file of embeddings, the Go vsearch engine loads it and answers nearest-neighbour
# queries by text ("a photo of a dog"), by an example corpus image (-id), and by a
# raw vector, over both the CLI (cmd/vsearch) and the HTTP endpoint (/v1/similar).
#
# Two modes:
#   SYNTHETIC=1 (default here)  deterministic no-torch embeddings — verifies the
#                               entire Go<->Parquet<->sidecar plumbing with only
#                               numpy+pyarrow, no GPU and no download.
#   SYNTHETIC=0                 real open_clip embeddings over the images in
#                               scripts/clip/sample_urls.txt (installs torch; CPU ok).
#
# Usage:  scripts/clip-search-demo.sh            # synthetic plumbing proof
#         SYNTHETIC=0 scripts/clip-search-demo.sh # real CLIP, meaningful results
set -euo pipefail

# --- knobs -------------------------------------------------------------------
DEMO="${DEMO:-$HOME/petabyte-demo}"
SYNTHETIC="${SYNTHETIC:-1}"
COUNT="${COUNT:-400}"          # synthetic corpus size
DIM="${DIM:-512}"              # vector dim (synthetic; real mode uses the model's 512)
PORT="${PORT:-8600}"           # CLIP encode sidecar
SRV_PORT="${SRV_PORT:-8085}"   # cmd/server for the /v1/similar demo

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIP="$REPO/scripts/clip"
RUN="$DEMO/clip-run"           # scratch: venv, parquet, logs, configs
mkdir -p "$RUN"
BIN="$RUN/bin"
PARQUET="$RUN/embeddings.parquet"

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT
say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

syn_flag=""; [ "$SYNTHETIC" = "1" ] && syn_flag="--synthetic"

# --- 1. build the Go binaries (run directly; go run would orphan children) ---
say "building cmd/vsearch and cmd/server"
go build -o "$BIN/vsearch" "$REPO/cmd/vsearch"
go build -o "$BIN/server" "$REPO/cmd/server"

# --- 2. python env -----------------------------------------------------------
VENV="$RUN/venv"
if [ ! -d "$VENV" ]; then
  say "creating python venv"
  python3 -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
say "installing python deps ($([ "$SYNTHETIC" = 1 ] && echo 'numpy+pyarrow only' || echo 'full: torch+open_clip'))"
if [ "$SYNTHETIC" = "1" ]; then
  pip install -q --disable-pip-version-check numpy pyarrow
else
  pip install -q --disable-pip-version-check -r "$CLIP/requirements.txt"
fi

# --- 3. embed the corpus -> Parquet -----------------------------------------
say "encoding corpus -> $PARQUET"
( cd "$CLIP" && python embed_corpus.py $syn_flag --count "$COUNT" --dim "$DIM" \
    --urls "$CLIP/sample_urls.txt" --out "$PARQUET" )

# --- 4. start the CLIP encode sidecar ---------------------------------------
say "starting CLIP encode sidecar on :$PORT"
( cd "$CLIP" && python encode_server.py $syn_flag --port "$PORT" --dim "$DIM" ) \
  >"$RUN/sidecar.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 100); do
  grep -q 'ready on' "$RUN/sidecar.log" 2>/dev/null && break
  sleep 0.1
done
ENC="http://127.0.0.1:$PORT"

# --- 5. CLI queries ----------------------------------------------------------
# A synthetic corpus cycles concepts (dog, cat, car, ...) as ids like dog_00000.jpg,
# so a "dog" query must rank a dog_* id first. Real mode returns genuine matches.
say "CLI: text query  -> 'a photo of a dog'"
"$BIN/vsearch" -index "$PARQUET" -encoder "$ENC" -k 5 -text "a photo of a dog" | tee "$RUN/q_text.out"

say "CLI: image->image -> most similar corpus images to dog_00000.jpg"
FIRST_DOG=$("$BIN/vsearch" -index "$PARQUET" -encoder "$ENC" -k 1 -text "a photo of a dog" | awk 'NR==1{print $3}')
"$BIN/vsearch" -index "$PARQUET" -encoder "$ENC" -k 5 -id "$FIRST_DOG" | tee "$RUN/q_id.out"

say "CLI: raw vector query (encoder-free path)"
"$BIN/vsearch" -index "$PARQUET" -k 3 -vector "$(python -c 'import json;print(json.dumps([1.0]+[0.0]*('"$DIM"'-1)))')" | tee "$RUN/q_vec.out"

# --- 6. HTTP endpoint (/v1/similar) -----------------------------------------
cat >"$RUN/server.yaml" <<YAML
storage: {endpoint: "http://localhost:9000", region: us-east-1, bucket: petabyte-images, access_key_id: minioadmin, secret_access_key: minioadmin, use_path_style: true}
metadata: {db_path: "$RUN/metadata.db"}
server:
  host: 127.0.0.1
  port: $SRV_PORT
  metadata_db_path: "$RUN/metadata.db"
  registry_db_path: "$RUN/registry.db"
  search:
    index_key: "$PARQUET"
    encoder_url: "$ENC"
logging: {level: warn, format: text}
YAML
say "starting cmd/server for /v1/similar on :$SRV_PORT"
"$BIN/server" -config "$RUN/server.yaml" >"$RUN/server.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 100); do
  curl -sf -o /dev/null "http://127.0.0.1:$SRV_PORT/healthz" 2>/dev/null && break
  sleep 0.1
done
say "HTTP: POST /v1/similar  text='a photo of a cat'"
curl -sf -X POST "http://127.0.0.1:$SRV_PORT/v1/similar" \
  -H 'Content-Type: application/json' \
  -d '{"text":"a photo of a cat","k":3}' | tee "$RUN/q_http.out"; echo

# --- 7. report + assert (synthetic: planted nearest neighbour at rank 1) -----
top_text=$(awk 'NR==1{print $3}' "$RUN/q_text.out")
top_id=$(awk 'NR==1{print $3}' "$RUN/q_id.out")
echo
echo "======================================================================"
echo "  CLIP SIMILARITY SEARCH DEMO RESULT   (synthetic=$SYNTHETIC)"
echo "======================================================================"
echo "  corpus:                 $(python -c "import pyarrow.parquet as pq;print(pq.read_table('$PARQUET').num_rows)") vectors, dim=$DIM"
echo "  text 'dog' -> rank 1:   $top_text"
echo "  id  '$FIRST_DOG' -> rank 1: $top_id"
echo "  HTTP /v1/similar cat:   $(python -c 'import json,sys;d=json.load(open("'"$RUN/q_http.out"'"));print(d["results"][0]["id"] if d.get("results") else "(none)")')"
echo "======================================================================"

if [ "$SYNTHETIC" = "1" ]; then
  # Concept clustering must hold: a dog query ranks a dog image first, and an
  # image->image query off a dog stays within dogs.
  case "$top_text" in dog_*) ;; *) echo "FAIL: text 'dog' returned $top_text (want dog_*)"; exit 1;; esac
  case "$top_id" in dog_*) ;; *) echo "FAIL: id query returned $top_id (want dog_*)"; exit 1;; esac
  echo "  => synthetic plumbing verified: concept clustering holds end-to-end"
fi
