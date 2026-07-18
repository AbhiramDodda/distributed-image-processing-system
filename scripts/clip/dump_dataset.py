#!/usr/bin/env python3
"""Dump a HuggingFace image dataset to real image files on disk, for ingestion into
the platform (MinIO) as objects. Unlike embed_corpus.py (which emits CLIP vectors),
this writes the actual images so the distributed job pipeline processes real data.

Filenames are "<class>_<n>.png"; the platform's content-addressed sharding hashes
each filename across its 256 shards, so a whole dataset spreads evenly.

    python dump_dataset.py --hf-dataset uoft-cs/cifar10 --split test --out ./cifar-images
"""

import argparse
import os
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hf-dataset", default="uoft-cs/cifar10")
    p.add_argument("--hf-config", default=None)
    p.add_argument("--split", default="test")
    p.add_argument("--label-col", default="label")
    p.add_argument("--out", required=True)
    p.add_argument("--limit", type=int, default=0, help="cap the count (0 = all)")
    p.add_argument("--format", default="png")
    args = p.parse_args()

    from datasets import load_dataset

    ds = load_dataset(args.hf_dataset, args.hf_config, split=args.split)
    if args.limit and args.limit < ds.num_rows:
        ds = ds.select(range(args.limit))
    img_col = "image" if "image" in ds.column_names else "img"
    names = ds.features[args.label_col].names

    os.makedirs(args.out, exist_ok=True)
    for i, ex in enumerate(ds):
        cls = names[ex[args.label_col]].replace(" ", "_")
        ex[img_col].convert("RGB").save(os.path.join(args.out, f"{cls}_{i:05d}.{args.format}"))
        if (i + 1) % 2000 == 0:
            print(f"  wrote {i + 1} images...", file=sys.stderr, flush=True)
    print(f"wrote {ds.num_rows} images to {args.out}")


if __name__ == "__main__":
    main()
