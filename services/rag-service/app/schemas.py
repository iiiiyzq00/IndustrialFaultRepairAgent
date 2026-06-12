"""RAG service schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RAGDocument(BaseModel):
    ticket_id: str
    relevance_score: float = 0.0
    phenomenon_summary: str = ""
    root_cause_summary: str = ""
    fix_steps: List[str] = Field(default_factory=list)
    fault_category: str = ""
    severity: str = ""
    confidence: float = 0.0
    match_reason: str = ""


class RAGRetrieveRequest(BaseModel):
    query: str
    top_k: int = 10
    filters: Optional[Dict[str, Any]] = None
    retrieval_strategy: str = "hybrid"  # hybrid | vector_only | bm25_only


class RAGRetrieveResponse(BaseModel):
    query_id: str
    documents: List[RAGDocument] = Field(default_factory=list)
    retrieval_stats: Dict[str, Any] = Field(default_factory=dict)


class RAGUpsertRequest(BaseModel):
    ticket_id: str
    content: str  # Full text for embedding
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RAGUpsertResponse(BaseModel):
    ticket_id: str
    chroma_id: str
    status: str  # created | updated
    embedding_dim: int = 1024
    total_documents: int


class RAGSeedRequest(BaseModel):
    """Bulk seed request for cold start."""
    tickets: List[RAGUpsertRequest]
