"""Async HTTP client for the RAG Service."""

from __future__ import annotations

import os
import logging
import httpx
from .schemas import RAGRetrieveRequest, RAGRetrieveResponse, RAGDocument, RAGContext

logger = logging.getLogger(__name__)

RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL", "http://rag-service:8200")
API_KEY = os.getenv("API_KEY", "dev-key-change-me")
TIMEOUT = float(os.getenv("RAG_TIMEOUT_SECONDS", "15.0"))


async def retrieve(query: str, top_k: int = 10, filters: dict | None = None) -> RAGRetrieveResponse:
    """Call the RAG service's /rag/retrieve endpoint."""
    req = RAGRetrieveRequest(query=query, top_k=top_k, filters=filters, retrieval_strategy="hybrid")

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT, connect=10.0, read=TIMEOUT, write=10.0)) as client:
        resp = await client.post(
            f"{RAG_SERVICE_URL}/api/v1/rag/retrieve",
            json=req.model_dump(),
            headers={"X-API-Key": API_KEY, "Accept": "application/json"},
        )
        resp.raise_for_status()
        return RAGRetrieveResponse(**resp.json())


async def pre_retrieve_for_incident(incident_text: str, line_profile: str = "general") -> RAGContext:
    """
    Supervisor calls this once before dispatching experts.

    Constructs a query from the incident description and retrieves
    the top-3 matching historical cases.
    """
    try:
        result = await retrieve(
            query=incident_text,
            top_k=3,
            # No metadata filters — the embedding similarity + LLM reranker
            # are sufficient for relevance selection.  Metadata filters were
            # excluding valid candidates whose documents lacked those fields.
        )
        return RAGContext(
            documents=result.documents,
            retrieved_at=None,  # set by caller
            retrieval_query=incident_text,
        )
    except Exception as e:
        logger.warning("RAG pre-retrieval failed: %s (%s)", e, type(e).__name__)
        return RAGContext(documents=[], retrieval_query=incident_text)
