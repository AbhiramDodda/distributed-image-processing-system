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
    table = pa.table({
        "id": pa.array(ids, pa.string()),
        "dataset": pa.array([dataset] * len(ids), pa.string()),
        # Variable list of float32 -- Go's reader recovers dim from the row width.
        "vector": pa.array([v.tolist() for v in vectors], pa.list_(pa.float32())),
    })
    pq.write_table(table, path)
    print(f"wrote {len(ids)} embeddings (dim={len(vectors[0])}) -> {path}")


def run_synthetic(args):
    from synthetic import CONCEPTS, synthetic_vector
    ids, vectors = [], []
    for i in range(args.count):
        concept = CONCEPTS[i % len(CONCEPTS)]
        item_id = f"{concept}_{i:05d}.jpg"
        ids.append(item_id)
        vectors.append(synthetic_vector(concept, item_id, args.dim))
    write_parquet(args.out, ids, args.dataset, vectors)


def run_real(args):
    import requests
    import torch
    import open_clip
    from PIL import Image

    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained)
    model.eval()

    if not os.path.exists(args.urls):
        sys.exit(f"--urls file not found: {args.urls} (or use --synthetic)")
    with open(args.urls) as f:
        urls = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

    ids, tensors = [], []
    for url in urls:
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            tensors.append(preprocess(img))
            ids.append(url.rsplit("/", 1)[-1] or url)
        except Exception as e:  # skip a dead URL rather than abort the whole run
            print(f"skip {url}: {e}", file=sys.stderr)
    if not tensors:
        sys.exit("no images downloaded; check --urls / network")

    vectors = []
    with torch.no_grad():
        for start in range(0, len(tensors), args.batch):
            batch = torch.stack(tensors[start:start + args.batch])
            feats = model.encode_image(batch)
            feats = feats / feats.norm(dim=-1, keepdim=True)
            vectors.extend(feats.cpu().numpy().astype(np.float32))
    write_parquet(args.out, ids, args.dataset, vectors)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="embeddings.parquet")
    p.add_argument("--dataset", default="sample")
    p.add_argument("--synthetic", action="store_true", help="deterministic no-torch embeddings")
    p.add_argument("--count", type=int, default=400, help="synthetic corpus size")
    p.add_argument("--dim", type=int, default=512, help="synthetic vector dimension (real mode uses the model's)")
    p.add_argument("--urls", default="sample_urls.txt", help="real mode: newline-delimited image URLs")
    p.add_argument("--model", default="ViT-B-32")
    p.add_argument("--pretrained", default="laion2b_s34b_b79k")
    p.add_argument("--batch", type=int, default=32)
    args = p.parse_args()

    if args.synthetic:
        run_synthetic(args)
    else:
        run_real(args)


if __name__ == "__main__":
    main()
