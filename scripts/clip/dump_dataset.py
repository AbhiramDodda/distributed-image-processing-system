#!/usr/bin/env python3
"""Dump a HuggingFace image dataset to real image files on disk, for ingestion into
the platform (MinIO) as objects. Unlike embed_corpus.py (which emits CLIP vectors),
this writes the actual images so the distributed job pipeline processes real data.

Filenames are "<class>_<n>.<fmt>" (or "<prefix>_<n>.<fmt>" with --name-prefix); the
platform's content-addressed sharding hashes each filename across its 256 shards, so
a whole dataset spreads evenly. --stream avoids staging the full dataset locally, and
--subdirs spreads the files so no single directory holds millions of entries.

    # small labelled set
    python dump_dataset.py --hf-dataset uoft-cs/cifar10 --split test --out ./cifar-images

WARNING: `--stream` uses HuggingFace `datasets` streaming, whose internal buffers grow
without bound and OOM a 13GB box at ~430k rows. For large parquet-backed sets (e.g. the
1.43M ImageNet set) use `dump_parquet_images.py` instead -- it processes one shard at a
time with flat memory. This script is fine for small/medium splits like CIFAR-10.
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
    p.add_argument("--format", default="png", help="png or jpg")
    p.add_argument("--quality", type=int, default=90, help="jpeg quality")
    p.add_argument("--stream", action="store_true", help="stream examples instead of downloading the whole split")
    p.add_argument("--subdirs", type=int, default=0, help="spread files across N subdirs (0 = flat)")
    p.add_argument("--name-prefix", default="", help="if set, name files <prefix>_<n> and ignore the label")
    args = p.parse_args()

    from datasets import load_dataset

    ds = load_dataset(args.hf_dataset, args.hf_config, split=args.split, streaming=args.stream)
    names = None
    if not args.name_prefix:
        feats = ds.features
        if feats and args.label_col in feats and hasattr(feats[args.label_col], "names"):
            names = feats[args.label_col].names

    os.makedirs(args.out, exist_ok=True)
    if args.subdirs:
        for d in range(args.subdirs):
            os.makedirs(os.path.join(args.out, f"d{d:03d}"), exist_ok=True)

    def img_of(ex):
        return ex["image"] if "image" in ex else ex["img"]

    written = 0
    for i, ex in enumerate(ds):
        if args.limit and i >= args.limit:
            break
        if args.name_prefix:
            stem = f"{args.name_prefix}_{i:07d}"
        else:
            cls = (names[ex[args.label_col]] if names else str(ex[args.label_col])).replace(" ", "_").replace(",", "")
            stem = f"{cls}_{i:07d}"
        sub = f"d{i % args.subdirs:03d}" if args.subdirs else ""
        path = os.path.join(args.out, sub, f"{stem}.{args.format}")
        im = img_of(ex).convert("RGB")
        if args.format.lower() in ("jpg", "jpeg"):
            im.save(path, quality=args.quality)
        else:
            im.save(path)
        written += 1
        if written % 20000 == 0:
            print(f"  wrote {written} images...", file=sys.stderr, flush=True)
    print(f"wrote {written} images to {args.out}")


if __name__ == "__main__":
    main()
