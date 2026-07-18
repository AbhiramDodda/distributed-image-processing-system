"""Deterministic stand-in embeddings so the whole search pipeline can be exercised
without torch, a GPU, or a download.

Vectors that share a *concept* (e.g. "dog") cluster together, so a synthetic text
query for "a photo of a dog" ranks the synthetic dog images first -- enough to prove
the Go <-> Parquet <-> sidecar plumbing end to end. Both the corpus encoder
(embed_corpus.py) and the query sidecar (encode_server.py) import this module so the
two sides agree on the space. Drop --synthetic to get real CLIP embeddings instead.
"""

import hashlib

import numpy as np

# A small fixed vocabulary the synthetic corpus and queries agree on.
CONCEPTS = ["dog", "cat", "car", "tree", "house", "flower", "boat", "bird"]


def _seed(s: str) -> int:
    # sha256 (not Python's salted hash()) so seeds are stable across processes/runs.
    return int.from_bytes(hashlib.sha256(s.encode()).digest()[:4], "big")


def concept_vector(concept: str, dim: int) -> np.ndarray:
    """A fixed unit direction for a concept -- the cluster centre."""
    rng = np.random.RandomState(_seed("concept:" + concept))
    v = rng.standard_normal(dim)
    return v / (np.linalg.norm(v) + 1e-12)


def synthetic_vector(concept: str, item_id: str, dim: int, jitter: float = 0.15) -> np.ndarray:
    """A per-item vector near its concept centre: same concept -> nearby vectors.

    The jitter is scaled by 1/sqrt(dim) so its magnitude stays ~`jitter` regardless
    of dimension; otherwise a raw N(0,1) perturbation grows like sqrt(dim) and would
    swamp the unit-norm concept direction, washing the clusters out.
    """
    base = concept_vector(concept, dim)
    rng = np.random.RandomState(_seed("item:" + item_id))
    v = base + (jitter / np.sqrt(dim)) * rng.standard_normal(dim)
    return (v / (np.linalg.norm(v) + 1e-12)).astype(np.float32)


def concept_for_text(text: str) -> str:
    """Map a free-text query to a known concept by substring match."""
    t = text.lower()
    for c in CONCEPTS:
        if c in t:
            return c
    return t.strip() or "unknown"


def concept_for_id(item_id: str) -> str:
    """Recover the concept from a synthetic id like 'dog_00001.jpg'."""
    base = item_id.rsplit("/", 1)[-1]
    return base.split("_", 1)[0].split(".", 1)[0]
