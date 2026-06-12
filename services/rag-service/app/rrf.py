"""
Reciprocal Rank Fusion (RRF).

Merges ranked lists from vector and BM25 retrieval into a single
consensus ranking.

Formula:  RRF(d) = Σ (1 / (k + rank_i(d)))
  where k=60 (standard), rank_i(d) is the rank of document d in list i.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

RRF_K = int(os.getenv("RRF_K", "60"))
VECTOR_WEIGHT = float(os.getenv("VECTOR_WEIGHT", "0.6"))
BM25_WEIGHT = float(os.getenv("BM25_WEIGHT", "0.4"))


def fuse(
    vector_results: List[Dict[str, Any]],
    bm25_results: List[Dict[str, Any]],
    top_k: int = 30,
) -> List[Dict[str, Any]]:
    """
    Merge two ranked lists using weighted RRF.

    Each document is identified by ticket_id.
    """
    scores: Dict[str, float] = {}
    doc_map: Dict[str, Dict[str, Any]] = {}

    # Vector list contribution
    for rank, doc in enumerate(vector_results, start=1):
        tid = doc.get("ticket_id", "")
        if not tid:
            continue
        rrf_score = 1.0 / (RRF_K + rank)
        scores[tid] = scores.get(tid, 0.0) + VECTOR_WEIGHT * rrf_score
        doc_map[tid] = doc

    # BM25 list contribution
    for rank, doc in enumerate(bm25_results, start=1):
        tid = doc.get("ticket_id", "")
        if not tid:
            continue
        rrf_score = 1.0 / (RRF_K + rank)
        scores[tid] = scores.get(tid, 0.0) + BM25_WEIGHT * rrf_score
        if tid not in doc_map:
            doc_map[tid] = doc

    # Sort by fused score, descending
    sorted_ids = sorted(scores.keys(), key=lambda tid: scores[tid], reverse=True)

    fused = []
    for tid in sorted_ids[:top_k]:
        doc = doc_map[tid]
        doc["rrf_score"] = round(scores[tid], 6)
        fused.append(doc)

    logger.debug("RRF fused: vector=%d + bm25=%d → %d candidates",
                 len(vector_results), len(bm25_results), len(fused))
    return fused
