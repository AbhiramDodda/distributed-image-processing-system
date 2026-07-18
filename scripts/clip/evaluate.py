#!/usr/bin/env python3
"""Measure text->image retrieval accuracy of the vsearch system against ground truth.

The corpus must be a Parquet file whose ids are "<class>_<n>.jpg" (as written by
embed_corpus.py --hf-dataset). For each class this runs a real text query
("a photo of a <class>") through the **Go engine** (cmd/vsearch -> CLIP sidecar ->
Go k-NN) and scores the returned neighbours against the class label:

    precision@k  = fraction of the top-k results whose true class == the query class
    top-1        = 1 if the rank-1 result is the query class

So it exercises the whole pipeline end to end and reports a real, quantitative
number (random-baseline precision = 1 / num_classes).

    python evaluate.py --index cifar.parquet --bin ./bin/vsearch --encoder http://127.0.0.1:8600 -k 10
"""

import argparse
import collections
import subprocess
import sys

import pyarrow.parquet as pq


def class_of(item_id):
    return item_id.rsplit("_", 1)[0]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--index", required=True)
    p.add_argument("--bin", default="./bin/vsearch", help="path to the compiled cmd/vsearch")
    p.add_argument("--encoder", default="http://127.0.0.1:8600")
    p.add_argument("-k", type=int, default=10)
    p.add_argument("--template", default="a photo of a {}")
    args = p.parse_args()

    ids = pq.read_table(args.index, columns=["id"]).column("id").to_pylist()
    counts = collections.Counter(class_of(i) for i in ids)
    classes = sorted(counts)

    print(f"corpus: {len(ids)} images, {len(classes)} classes "
          f"(random-baseline precision = {1/len(classes):.1%})\n")
    print(f"{'class':14} {'count':>5}  {'P@'+str(args.k):>6}  top-1  prompt")
    print("-" * 60)

    prec_sum, top1_hits = 0.0, 0
    for cls in classes:
        prompt = args.template.format(cls.replace("_", " "))
        out = subprocess.run(
            [args.bin, "-index", args.index, "-encoder", args.encoder, "-k", str(args.k), "-text", prompt],
            capture_output=True, text=True,
        )
        if out.returncode != 0:
            sys.exit(f"query failed for {cls!r}: {out.stderr.strip()}")
        got = [ln.split()[2] for ln in out.stdout.splitlines() if ln.strip()]
        hits = sum(1 for r in got if class_of(r) == cls)
        precision = hits / len(got) if got else 0.0
        top1 = bool(got) and class_of(got[0]) == cls
        prec_sum += precision
        top1_hits += top1
        print(f"{cls:14} {counts[cls]:>5}  {precision:>6.1%}  {'yes' if top1 else 'no ':>5}  {prompt}")

    n = len(classes)
    print("-" * 60)
    print(f"mean precision@{args.k}: {prec_sum/n:.1%}   |   top-1 accuracy: {top1_hits}/{n} = {top1_hits/n:.1%}")


if __name__ == "__main__":
    main()
