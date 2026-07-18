#!/usr/bin/env python3
"""Dump a HuggingFace *parquet-backed* image dataset to real JPEG files on disk,
one shard at a time, with bounded memory and per-shard resume.

Why this exists: `dump_dataset.py --stream` uses `datasets` streaming, whose internal
buffers grow without bound over a 1.43M-row set and OOM the box (~430k rows in on a
13GB machine). This script instead downloads ONE parquet shard, iterates it in small
record batches writing each image, then deletes the shard before moving to the next —
so resident memory stays ~one batch regardless of dataset size, and a crash/kill
resumes at the next unfinished shard (a `.shard_NN.done` marker is written per shard).

The `image` column of these HF Image datasets is a struct<bytes, path> whose `bytes`
are already-encoded JPEG, so we write them verbatim (no decode/re-encode). Files are
named `<prefix>_<shard>_<row>.jpg` and spread across `--subdirs` dirs so the platform's
content-addressed sharding fans them evenly across its 256 shards.

    python dump_parquet_images.py \\
        --hf-dataset benjamin-paine/imagenet-1k-256x256 --split train \\
        --subdirs 256 --name-prefix imagenet --out ~/petabyte-demo/imagenet-images
"""

import argparse
import gc
import os
import sys


def main():
	p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
	p.add_argument("--hf-dataset", default="benjamin-paine/imagenet-1k-256x256")
	p.add_argument("--split", default="train")
	p.add_argument("--out", required=True)
	p.add_argument("--subdirs", type=int, default=256, help="spread files across N subdirs")
	p.add_argument("--name-prefix", default="imagenet")
	p.add_argument("--batch-size", type=int, default=1000, help="parquet rows read per batch")
	p.add_argument("--limit-shards", type=int, default=0, help="cap number of shards (0 = all)")
	p.add_argument("--keep-parquet", action="store_true", help="do not delete each shard after processing")
	args = p.parse_args()

	from huggingface_hub import HfApi, hf_hub_download
	import pyarrow.parquet as pq

	api = HfApi()
	files = api.list_repo_files(args.hf_dataset, repo_type="dataset")
	shards = sorted(f for f in files if f.endswith(".parquet") and f"/{args.split}-" in f"/{f}")
	if args.limit_shards:
		shards = shards[: args.limit_shards]
	if not shards:
		print(f"no parquet shards for split {args.split!r}", file=sys.stderr)
		sys.exit(1)
	print(f"{len(shards)} parquet shards for split {args.split}", flush=True)

	os.makedirs(args.out, exist_ok=True)
	for d in range(args.subdirs):
		os.makedirs(os.path.join(args.out, f"d{d:03d}"), exist_ok=True)
	scratch = os.path.join(args.out, "_parquet")
	os.makedirs(scratch, exist_ok=True)
	# Resume markers live OUTSIDE the image tree so a recursive `find`/ingest of
	# --out sees only real images, not bookkeeping files.
	markers = args.out.rstrip("/") + ".markers"
	os.makedirs(markers, exist_ok=True)

	total = 0
	gidx = 0
	for si, remote in enumerate(shards):
		marker = os.path.join(markers, f"shard_{si:02d}.done")
		if os.path.exists(marker):
			with open(marker) as fh:
				gidx += int(fh.read().strip() or 0)
			print(f"[shard {si:02d}] already done, skipping", flush=True)
			continue

		print(f"[shard {si:02d}] downloading {remote} ...", flush=True)
		local = hf_hub_download(args.hf_dataset, remote, repo_type="dataset", local_dir=scratch)
		pf = pq.ParquetFile(local)
		wrote = 0
		for batch in pf.iter_batches(batch_size=args.batch_size, columns=["image"]):
			col = batch.column(0).to_pylist()
			for cell in col:
				data = cell["bytes"]
				sub = f"d{gidx % args.subdirs:03d}"
				name = f"{args.name_prefix}_{si:02d}_{wrote:07d}.jpg"
				with open(os.path.join(args.out, sub, name), "wb") as out:
					out.write(data)
				wrote += 1
				gidx += 1
			del col
		del pf
		if not args.keep_parquet:
			os.remove(local)
		gc.collect()
		with open(marker, "w") as fh:
			fh.write(str(wrote))
		total += wrote
		print(f"[shard {si:02d}] wrote {wrote} images (running total {gidx})", flush=True)

	# best-effort scratch cleanup
	try:
		os.rmdir(scratch)
	except OSError:
		pass
	print(f"done: {gidx} images across {len(shards)} shards -> {args.out}", flush=True)


if __name__ == "__main__":
	main()
