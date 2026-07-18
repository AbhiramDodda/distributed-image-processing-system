#!/usr/bin/env bash
#
# clip-eval-demo.sh — real CLIP similarity search over a labelled dataset, scored.
#
# Downloads a real HuggingFace image dataset, embeds it with CLIP (open_clip
# ViT-B/32 laion2b), and measures text->image retrieval accuracy through the Go
# engine (cmd/vsearch -> CLIP sidecar -> Go k-NN) against ground-truth labels —
# a real, quantitative number, not a hand-picked handful of images.
#
# Unlike clip-search-demo.sh (which has a SYNTHETIC plumbing mode), this is
# real-only: it needs torch + open_clip + datasets, and network to the HF Hub.
# CPU is fine (~1 min to embed a couple thousand small images).
#
# Usage:  scripts/clip-eval-demo.sh
#         HF_DATASET=uoft-cs/cifar10 LIMIT=2000 K=10 scripts/clip-eval-demo.sh
set -euo pipefail

# --- knobs -------------------------------------------------------------------
DEMO="${DEMO:-$HOME/petabyte-demo}"
HF_DATASET="${HF_DATASET:-uoft-cs/cifar10}"
HF_SPLIT="${HF_SPLIT:-test}"
HF_CONFIG="${HF_CONFIG:-}"     # optional dataset config/subset
LIMIT="${LIMIT:-2000}"         # cap images (0 = whole split)
K="${K:-10}"                   # precision@K
PORT="${PORT:-8600}"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIP="$REPO/scripts/clip"
RUN="$DEMO/clip-run"
mkdir -p "$RUN"
BIN="$RUN/bin"
PARQUET="$RUN/dataset.parquet"

pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# --- 1. build the Go query binary -------------------------------------------
say "building cmd/vsearch"
go build -o "$BIN/vsearch" "$REPO/cmd/vsearch"

# --- 2. python env + deps ----------------------------------------------------
VENV="$RUN/venv"
[ -d "$VENV" ] || python3 -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
say "installing python deps (torch + open_clip + datasets)"
pip install -q --disable-pip-version-check -r "$CLIP/requirements.txt"

# --- 3. download + embed the dataset ----------------------------------------
say "embedding $HF_DATASET:$HF_SPLIT (limit=$LIMIT) with CLIP -> $PARQUET"
cfg_flag=""; [ -n "$HF_CONFIG" ] && cfg_flag="--hf-config $HF_CONFIG"
( cd "$CLIP" && python embed_corpus.py --hf-dataset "$HF_DATASET" --hf-split "$HF_SPLIT" $cfg_flag \
    --limit "$LIMIT" --dataset "${HF_DATASET##*/}" --out "$PARQUET" )

# --- 4. start the CLIP encode sidecar ---------------------------------------
say "starting CLIP encode sidecar on :$PORT"
( cd "$CLIP" && python encode_server.py --port "$PORT" --dim 512 ) >"$RUN/eval-sidecar.log" 2>&1 &
pids+=($!)
until grep -q 'ready on' "$RUN/eval-sidecar.log" 2>/dev/null; do sleep 0.5; done

# --- 5. score retrieval through the Go engine -------------------------------
say "measuring text->image retrieval accuracy through cmd/vsearch (precision@$K)"
python "$CLIP/evaluate.py" --index "$PARQUET" --bin "$BIN/vsearch" \
  --encoder "http://127.0.0.1:$PORT" -k "$K"
