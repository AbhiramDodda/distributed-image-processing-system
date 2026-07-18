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


def load_clip(args):
    import open_clip
    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained)
    model.eval()
    return model, preprocess


def encode_images(model, preprocess, images, batch):
    """Run the CLIP image encoder over PIL images, L2-normalised, in batches."""
    import torch
    tensors = [preprocess(im.convert("RGB")) for im in images]
    vectors = []
    with torch.no_grad():
        for start in range(0, len(tensors), batch):
            feats = model.encode_image(torch.stack(tensors[start:start + batch]))
            feats = feats / feats.norm(dim=-1, keepdim=True)
            vectors.extend(feats.cpu().numpy().astype(np.float32))
    return vectors


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

    images, ids = [], []
    for i, ex in enumerate(ds):
        cls = names[ex[args.hf_label_col]].replace(" ", "_")
        images.append(ex[img_col])
        ids.append(f"{cls}_{i:05d}.jpg")
    print(f"embedding {len(images)} images from {args.hf_dataset}:{args.hf_split} "
          f"across {len(names)} classes ({', '.join(names)})")
    vectors = encode_images(model, preprocess, images, args.batch)
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

    images, ids = [], []
    for item_id, url in entries:
        try:
            resp = requests.get(url, timeout=20, headers=headers)
            resp.raise_for_status()
            images.append(Image.open(io.BytesIO(resp.content)))
            ids.append(item_id)
        except Exception as e:  # skip a dead URL rather than abort the whole run
            print(f"skip {url}: {e}", file=sys.stderr)
    if not images:
        sys.exit("no images downloaded; check --urls / network")

    vectors = encode_images(model, preprocess, images, args.batch)
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
