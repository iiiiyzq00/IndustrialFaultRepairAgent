"""
RAG Service — Hybrid retrieval pipeline.

Endpoints:
  POST /api/v1/rag/retrieve  — hybrid search (vector + BM25 + RRF + 2-stage rerank)
  POST /api/v1/rag/upsert    — add/update a document
  POST /api/v1/rag/seed      — bulk seed documents (cold start)
  GET  /health                — health + collection stats
"""

from __future__ import annotations

import os
import sys
import uuid
import time
import json
import logging
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

# Path for common module
_PARENT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from common.auth import setup_api_key_auth  # noqa: E402
from common.metrics import setup_metrics  # noqa: E402

from .schemas import (  # noqa: E402
    RAGRetrieveRequest, RAGRetrieveResponse, RAGDocument,
    RAGUpsertRequest, RAGUpsertResponse, RAGSeedRequest,
)
from . import chroma_client  # noqa: E402
from . import bm25_index  # noqa: E402
from . import rrf  # noqa: E402
from . import reranker  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [rag] %(levelname)s %(message)s",
)
logger = logging.getLogger("rag-service")

SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8200"))
SEED_FILE = os.getenv("SEED_FILE", "")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Industrial Fault Repair — RAG Pipeline",
    version="1.0.0",
)
setup_api_key_auth(app)
setup_metrics(app)


@app.on_event("startup")
async def startup():
    """Load seed data and warm up BM25 index on startup."""
    # Load from seed file if configured
    if SEED_FILE and os.path.isfile(SEED_FILE):
        logger.info("Loading seed tickets from %s", SEED_FILE)
        try:
            with open(SEED_FILE, "r") as f:
                seed_data = json.load(f)
            for ticket in seed_data:
                try:
                    req = RAGUpsertRequest(**ticket)
                    chroma_client.upsert_document(req.ticket_id, req.content, req.metadata)
                except Exception as e:
                    logger.warning("Failed to seed ticket %s: %s", ticket.get("ticket_id", "?"), e)
            logger.info("Seeded %d tickets", len(seed_data))
        except Exception as e:
            logger.error("Seed loading failed: %s", e)

    # Warm BM25 index from ChromaDB
    await _rebuild_bm25()


async def _rebuild_bm25():
    """Re-index BM25 from all ChromaDB documents."""
    try:
        coll = chroma_client._get_collection()
        all_docs = coll.get(include=["documents", "metadatas"])
        docs = []
        if all_docs["ids"]:
            for i, doc_id in enumerate(all_docs["ids"]):
                docs.append({
                    "ticket_id": doc_id,
                    "content": all_docs["documents"][i] if all_docs["documents"] else "",
                    "metadata": all_docs["metadatas"][i] if all_docs["metadatas"] else {},
                })
        bm25_index.build_index(docs)
    except Exception as e:
        logger.warning("BM25 warm-up failed (will retry on first query): %s", e)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "total_documents": chroma_client.get_total_count(),
    }


# ---------------------------------------------------------------------------
# Retrieve
# ---------------------------------------------------------------------------

@app.post("/api/v1/rag/retrieve", response_model=RAGRetrieveResponse)
async def retrieve(req: RAGRetrieveRequest):
    """
    Hybrid retrieval pipeline:
      1. Vector search (ChromaDB cosine) → top-30
      2. BM25 sparse search → top-30
      3. RRF fusion (k=60, weights: vector=0.6, BM25=0.4) → merged top-30
      4. BGE-Reranker-v2-m3 coarse rerank → top-10
      5. DeepSeek-V3 fine rerank → top-3
    """
    t0 = time.monotonic()
    stats: Dict[str, Any] = {}

    # ── Step 1: Vector search ──
    t1 = time.monotonic()
    vector_results = chroma_client.vector_search(
        req.query, top_k=30, filters=req.filters
    )
    stats["vector_candidates"] = len(vector_results)
    stats["vector_ms"] = int((time.monotonic() - t1) * 1000)

    # ── Step 2: BM25 search ──
    t2 = time.monotonic()
    bm25_results = bm25_index.search(
        req.query, top_k=30, filters=req.filters
    )
    stats["bm25_candidates"] = len(bm25_results)
    stats["bm25_ms"] = int((time.monotonic() - t2) * 1000)

    # ── Step 3: RRF fusion ──
    t3 = time.monotonic()
    fused = rrf.fuse(vector_results, bm25_results, top_k=30)
    stats["rrf_merged"] = len(fused)
    stats["rrf_ms"] = int((time.monotonic() - t3) * 1000)

    # ── Step 4 & 5: Two-stage rerank ──
    t4 = time.monotonic()
    reranked = await reranker.rerank_pipeline(req.query, fused)
    stats["reranker_coarse_top"] = min(10, len(fused))
    stats["llm_reranked_final"] = len(reranked)
    stats["rerank_ms"] = int((time.monotonic() - t4) * 1000)

    # ── Build response ──
    documents = []
    for doc in reranked:
        meta = doc.get("metadata", {})
        documents.append(RAGDocument(
            ticket_id=doc.get("ticket_id", ""),
            relevance_score=doc.get("relevance_score", doc.get("rerank_score", 0.5)),
            phenomenon_summary=meta.get("phenomenon_summary", doc.get("content", "")[:300]),
            root_cause_summary=meta.get("root_cause_summary", ""),
            fix_steps=meta.get("fix_steps", []),
            fault_category=meta.get("fault_category", ""),
            severity=meta.get("severity", ""),
            confidence=meta.get("confidence", 0.0),
            match_reason=doc.get("match_reason", ""),
        ))

    stats["total_latency_ms"] = int((time.monotonic() - t0) * 1000)

    logger.info("Retrieve complete: query[:50], candidates=%d/%d/%d/%d, latency=%dms",
                stats["vector_candidates"], stats["bm25_candidates"],
                stats["rrf_merged"], stats["llm_reranked_final"],
                stats["total_latency_ms"])

    return RAGRetrieveResponse(
        query_id=str(uuid.uuid4()),
        documents=documents,
        retrieval_stats=stats,
    )


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

@app.post("/api/v1/rag/upsert", response_model=RAGUpsertResponse)
async def upsert(req: RAGUpsertRequest):
    """Add or update a document in the RAG corpus."""
    chroma_id = chroma_client.upsert_document(req.ticket_id, req.content, req.metadata)

    # Update BM25
    bm25_index.add_document({
        "ticket_id": req.ticket_id,
        "content": req.content,
        "metadata": req.metadata,
    })

    return RAGUpsertResponse(
        ticket_id=req.ticket_id,
        chroma_id=chroma_id,
        status="created",
        embedding_dim=1024,
        total_documents=chroma_client.get_total_count(),
    )


# ---------------------------------------------------------------------------
# Bulk Seed
# ---------------------------------------------------------------------------

@app.post("/api/v1/rag/seed")
async def seed(req: RAGSeedRequest):
    """Bulk-load seed tickets (used for cold start)."""
    count = 0
    for ticket in req.tickets:
        try:
            chroma_client.upsert_document(ticket.ticket_id, ticket.content, ticket.metadata)
            count += 1
        except Exception as e:
            logger.warning("Seed upsert failed for %s: %s", ticket.ticket_id, e)

    # Rebuild BM25
    await _rebuild_bm25()

    return {
        "status": "ok",
        "seeded": count,
        "total_documents": chroma_client.get_total_count(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=SERVICE_PORT, reload=True)
