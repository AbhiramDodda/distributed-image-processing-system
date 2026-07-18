#!/usr/bin/env python3
"""CLIP query-encoder sidecar for the Go vsearch engine.

Loads the model once and serves POST /encode:

    request : {"text": "a photo of a dog"}  or  {"image_b64": "<base64 image>"}
    response: {"vector": [<float32>, ...]}   (L2-normalised)

Text runs the CLIP text encoder (text->image search); image_b64 runs the image
encoder (novel image->image). The Go side (internal/vsearch.HTTPEncoder) speaks this
exact contract, so either language can be swapped.

Synthetic mode (--synthetic): no torch, deterministic concept-clustered vectors that
match embed_corpus.py --synthetic, so the wire contract is exercisable anywhere.

    python encode_server.py --synthetic --port 8600     # plumbing
    python encode_server.py --port 8600                 # real CLIP
"""

import argparse
import base64
import io
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np


def build_encoder(args):
    """Return encode(req_dict) -> list[float]. Loads the model once, up front."""
    if args.synthetic:
        from synthetic import synthetic_vector, concept_for_text
        def encode(req):
            if req.get("text"):
                concept = concept_for_text(req["text"])
                # a fresh query point near the concept centre (id keyed on the text)
                return synthetic_vector(concept, "query:" + req["text"], args.dim).tolist()
            if req.get("image_b64"):
                # No concept is recoverable from raw bytes offline; hash them to a
                # stable vector. This exercises the image wire path (real mode gives
                # meaningful results); prefer text / -id queries for synthetic asserts.
                raw = base64.b64decode(req["image_b64"])
                import hashlib
                seed = int.from_bytes(hashlib.sha256(raw).digest()[:4], "big")
                rng = np.random.RandomState(seed)
                v = rng.standard_normal(args.dim)
                return (v / (np.linalg.norm(v) + 1e-12)).astype(np.float32).tolist()
            raise ValueError("request has neither text nor image_b64")
        return encode

    import torch
    import open_clip
    from PIL import Image
    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained)
    model.eval()
    tokenizer = open_clip.get_tokenizer(args.model)

    def encode(req):
        with torch.no_grad():
            if req.get("text"):
                feats = model.encode_text(tokenizer([req["text"]]))
            elif req.get("image_b64"):
                img = Image.open(io.BytesIO(base64.b64decode(req["image_b64"]))).convert("RGB")
                feats = model.encode_image(preprocess(img).unsqueeze(0))
            else:
                raise ValueError("request has neither text nor image_b64")
            feats = feats / feats.norm(dim=-1, keepdim=True)
            return feats[0].cpu().numpy().astype(np.float32).tolist()
    return encode


def make_handler(encode):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/encode":
                self.send_error(404, "use POST /encode")
                return
            length = int(self.headers.get("Content-Length", 0))
            try:
                req = json.loads(self.rfile.read(length) or b"{}")
                vector = encode(req)
            except Exception as e:
                self.send_error(400, str(e))
                return
            payload = json.dumps({"vector": vector}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):  # keep the demo output clean
            pass
    return Handler


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=8600)
    p.add_argument("--synthetic", action="store_true", help="deterministic no-torch encoder")
    p.add_argument("--dim", type=int, default=512, help="synthetic vector dimension (must match the corpus)")
    p.add_argument("--model", default="ViT-B-32")
    p.add_argument("--pretrained", default="laion2b_s34b_b79k")
    args = p.parse_args()

    encode = build_encoder(args)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(encode))
    # Printed so a launcher can wait for readiness before sending queries.
    print(f"clip encode sidecar ready on http://127.0.0.1:{args.port} (synthetic={args.synthetic})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
