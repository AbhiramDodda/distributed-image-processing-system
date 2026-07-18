#!/usr/bin/env python3
"""Encode an image corpus into a Parquet file of CLIP embeddings for the Go vsearch
engine (internal/vsearch). The schema is exactly what ReadEmbeddingsParquet expects:

    id      : string   -- the search-result identity (filename / object key / url)
    dataset : string   -- logical dataset name
    vector  : list<float32>  -- the L2-normalised embedding (dim inferred by Go)

Real mode (default): download the images listed in --urls, run open_clip's image
encoder, normalise, and write the rows. CPU is fine for a few hundred images.

Synthetic mode (--synthetic): emit deterministic concept-clustered vectors with no
torch and no download, so the pipeline is verifiable anywhere. See synthetic.py.

    # plumbing, no deps beyond numpy+pyarrow:
    python embed_corpus.py --synthetic --count 400 --out embeddings.parquet
    # real embeddings:
    python embed_corpus.py --urls sample_urls.txt --out embeddings.parquet
"""

import argparse
import io
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(path, ids, dataset, vectors):
    # Build the vector column straight from a contiguous (N, dim) float32 buffer via
    # ListArray offsets -- avoids materialising N python lists (cheap and low-memory
    # even for large corpora). Go's reader recovers dim from the per-row width.
    arr = np.ascontiguousarray(np.stack(vectors), dtype=np.float32)
    n, dim = arr.shape
    flat = pa.array(arr.reshape(-1))
    offsets = pa.array(np.arange(0, (n + 1) * dim, dim, dtype=np.int32))
    table = pa.table({
        "id": pa.array(ids, pa.string()),
        "dataset": pa.array([dataset] * n, pa.string()),
        "vector": pa.ListArray.from_arrays(offsets, flat),
    })
    pq.write_table(table, path)
    print(f"wrote {n} embeddings (dim={dim}) -> {path}")


def run_synthetic(args):
    from synthetic import CONCEPTS, synthetic_vector
    ids, vectors = [], []
    for i in range(args.count):
        concept = CONCEPTS[i % len(CONCEPTS)]
        item_id = f"{concept}_{i:05d}.jpg"
        ids.append(item_id)
        vectors.append(synthetic_vector(concept, item_id, args.dim))
    write_parquet(args.out, ids, args.dataset, vectors)


def load_clip(args):
    import open_clip
    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained)
    model.eval()
    return model, preprocess


def encode_stream(model, preprocess, items, batch):
    """Encode (id, PIL.Image) pairs with the CLIP image encoder, L2-normalised, in
    batches. Only `batch` preprocessed image tensors are ever resident at once -- the
    tensors (3x224x224 floats) are the memory cost, not the tiny output vectors -- so
    this streams cleanly over datasets far larger than RAM. Returns (ids, vectors)."""
    import torch
    ids, vectors = [], []
    buf_ids, buf_t = [], []

    def flush():
        if not buf_t:
            return
        with torch.no_grad():
            feats = model.encode_image(torch.stack(buf_t))
            feats = feats / feats.norm(dim=-1, keepdim=True)
            vectors.extend(feats.cpu().numpy().astype(np.float32))
        ids.extend(buf_ids)
        buf_ids.clear()
        buf_t.clear()

    for item_id, im in items:
        buf_ids.append(item_id)
        buf_t.append(preprocess(im.convert("RGB")))
        if len(buf_t) >= batch:
            flush()
            if len(ids) % 1000 < batch:
                print(f"  encoded {len(ids)} images...", file=sys.stderr, flush=True)
    flush()
    return ids, vectors


def run_hf(args):
    """Embed a real labelled image dataset from the HuggingFace Hub. The class label
    becomes the id prefix (e.g. dog_00042.jpg), so text queries can be scored for
    retrieval accuracy against ground truth. See evaluate.py."""
    from datasets import load_dataset
    model, preprocess = load_clip(args)

    ds = load_dataset(args.hf_dataset, args.hf_config, split=args.hf_split)
    if args.limit and args.limit < ds.num_rows:
        ds = ds.shuffle(seed=args.seed).select(range(args.limit))
    img_col = "image" if "image" in ds.column_names else "img"
    names = ds.features[args.hf_label_col].names
    print(f"embedding {ds.num_rows} images from {args.hf_dataset}:{args.hf_split} "
          f"across {len(names)} classes ({', '.join(names)})")

    # Stream (id, image) pairs so HF decodes one example at a time -- never the whole
    # split into memory at once (the 10k-image OOM was building the full tensor list).
    def items():
        for i, ex in enumerate(ds):
            cls = names[ex[args.hf_label_col]].replace(" ", "_")
            yield f"{cls}_{i:05d}.jpg", ex[img_col]

    ids, vectors = encode_stream(model, preprocess, items(), args.batch)
    write_parquet(args.out, ids, args.dataset, vectors)


def run_real(args):
    import requests
    from PIL import Image

    model, preprocess = load_clip(args)

    if not os.path.exists(args.urls):
        sys.exit(f"--urls file not found: {args.urls} (or use --synthetic)")
    # Each line is "url" or "id<space>url": an optional leading id gives the result a
    # readable label (CLIP only ever sees pixels, so the id never affects retrieval).
    entries = []
    with open(args.urls) as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            parts = ln.split(None, 1)
            if len(parts) == 2:
                entries.append((parts[0], parts[1]))
            else:
                entries.append((parts[0].rsplit("/", 1)[-1] or parts[0], parts[0]))

    # Some hosts reject a generic library User-Agent (403), so identify the client.
    headers = {"User-Agent": "petabyte-platform-clip-demo/1.0 (https://github.com/AbhiramDodda/distributed-image-processing-system)"}

    def items():
        for item_id, url in entries:
            try:
                resp = requests.get(url, timeout=20, headers=headers)
                resp.raise_for_status()
                yield item_id, Image.open(io.BytesIO(resp.content))
            except Exception as e:  # skip a dead URL rather than abort the whole run
                print(f"skip {url}: {e}", file=sys.stderr)

    ids, vectors = encode_stream(model, preprocess, items(), args.batch)
    if not ids:
        sys.exit("no images downloaded; check --urls / network")
    write_parquet(args.out, ids, args.dataset, vectors)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="embeddings.parquet")
    p.add_argument("--dataset", default="sample")
    p.add_argument("--synthetic", action="store_true", help="deterministic no-torch embeddings")
    p.add_argument("--count", type=int, default=400, help="synthetic corpus size")
    p.add_argument("--dim", type=int, default=512, help="synthetic vector dimension (real mode uses the model's)")
    p.add_argument("--urls", default="sample_urls.txt", help="real mode: newline-delimited image URLs")
    p.add_argument("--hf-dataset", default="", help="embed a HuggingFace image dataset (e.g. uoft-cs/cifar10)")
    p.add_argument("--hf-config", default=None, help="dataset config/subset name")
    p.add_argument("--hf-split", default="test", help="dataset split")
    p.add_argument("--hf-label-col", default="label", help="ground-truth label column")
    p.add_argument("--limit", type=int, default=0, help="cap the number of images (0 = all)")
    p.add_argument("--seed", type=int, default=42, help="shuffle seed when --limit subsamples")
    p.add_argument("--model", default="ViT-B-32")
    p.add_argument("--pretrained", default="laion2b_s34b_b79k")
    p.add_argument("--batch", type=int, default=32)
    args = p.parse_args()

    if args.synthetic:
        run_synthetic(args)
    elif args.hf_dataset:
        run_hf(args)
    else:
        run_real(args)


if __name__ == "__main__":
    main()
