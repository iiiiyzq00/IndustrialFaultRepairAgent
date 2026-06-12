"""
BM25 sparse retrieval index.

Maintains an in-memory BM25 index over all documents in ChromaDB.
Re-indexed on startup and after each upsert.

Uses jieba for Chinese tokenization + rank-bm25 for scoring.
"""

from __future__ import annotations

import os
import logging
import threading
from typing import Any, Dict, List, Optional

import jieba
from rank_bm25 import BM25Okapi

logger = logging.getLogger(__name__)

# ─── Globals ──────────────────────────────────────────────────

_lock = threading.Lock()
_corpus: List[List[str]] = []         # tokenized documents
_doc_metadata: List[Dict[str, Any]] = []  # metadata per doc
_bm25: Optional[BM25Okapi] = None


def _tokenize(text: str) -> List[str]:
    """Tokenize Chinese + English text using jieba."""
    # jieba cuts Chinese words; we also split on whitespace for English tokens
    tokens = []
    for word in jieba.cut(text):
        word = word.strip()
        if word and len(word) > 1:
            tokens.append(word.lower())
    # Also add English words split by whitespace
    for token in text.split():
        token = token.strip().lower()
        if token and len(token) > 1 and token not in tokens:
            tokens.append(token)
    return tokens


def build_index(documents: List[Dict[str, Any]]) -> None:
    """
    Build/replace the BM25 index from a list of documents.
    Each doc must have: ticket_id, content, metadata.
    """
    global _corpus, _doc_metadata, _bm25

    with _lock:
        _corpus = []
        _doc_metadata = []

        for doc in documents:
            content = doc.get("content", "")
            tokens = _tokenize(content)
            if tokens:
                _corpus.append(tokens)
                _doc_metadata.append({
                    "ticket_id": doc.get("ticket_id", doc.get("chroma_id", "")),
                    "metadata": doc.get("metadata", {}),
                    "content": content,
                })

        _bm25 = BM25Okapi(_corpus) if _corpus else None
        logger.info("BM25 index built: %d documents", len(_corpus))


def add_document(doc: Dict[str, Any]) -> None:
    """Add a single document to the running BM25 index."""
    global _bm25

    content = doc.get("content", "")
    tokens = _tokenize(content)
    if not tokens:
        return

    with _lock:
        _corpus.append(tokens)
        _doc_metadata.append({
            "ticket_id": doc.get("ticket_id", ""),
            "metadata": doc.get("metadata", {}),
            "content": content,
        })
        # Rebuild BM25 (simpler than incremental; fine for < 10K docs)
        _bm25 = BM25Okapi(_corpus)

    logger.debug("BM25 index updated: %d documents", len(_corpus))


def search(query: str, top_k: int = 30, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    BM25 sparse retrieval.
    Returns top_k documents sorted by BM25 score (descending).
    """
    with _lock:
        if _bm25 is None or not _corpus:
            logger.warning("BM25 index is empty")
            return []

        query_tokens = _tokenize(query)
        if not query_tokens:
            return []

        scores = _bm25.get_scores(query_tokens)

        # Pair (index, score) sorted by score desc
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

        results = []
        for idx, score in ranked[:top_k * 3]:  # oversample then filter
            if score <= 0:
                continue
            meta = _doc_metadata[idx]
            # Apply filters
            if not _passes_filter(meta.get("metadata", {}), filters):
                continue
            results.append({
                "ticket_id": meta["ticket_id"],
                "content": meta["content"],
                "metadata": meta["metadata"],
                "bm25_score": float(score),
            })
            if len(results) >= top_k:
                break

    logger.debug("BM25 search returned %d candidates", len(results))
    return results


def _passes_filter(metadata: Dict[str, Any], filters: Optional[Dict[str, Any]]) -> bool:
    """Check whether a document passes the metadata filters.

    Important: if a metadata field is missing from the document, the filter
    is NOT applied (missing ≠ reject).  This is deliberate — seed data and
    early flywheel documents donʼt have every field, and we donʼt want to
    throw away otherwise-relevant results.
    """
    if not filters:
        return True
    if "fault_category" in filters:
        if metadata.get("fault_category") != filters["fault_category"]:
            return False
    if "min_confidence" in filters:
        if "confidence" in metadata and metadata["confidence"] < filters["min_confidence"]:
            return False
    if filters.get("successful_only"):
        if "fix_success" in metadata and not metadata["fix_success"]:
            return False
    if "line_profile" in filters:
        if "line_profile" in metadata and metadata["line_profile"] != filters["line_profile"]:
            return False
    return True
