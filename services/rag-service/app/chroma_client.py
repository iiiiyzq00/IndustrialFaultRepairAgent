"""
ChromaDB client wrapper.

Manages a persistent ChromaDB collection for industrial fault tickets.
Embeddings are generated with sentence-transformers (bge-large-zh-v1.5).
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

CHROMADB_HOST = os.getenv("CHROMADB_HOST", "chromadb")
CHROMADB_PORT = os.getenv("CHROMADB_PORT", "8000")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-zh-v1.5")
COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "industrial_faults")

# ─── Globals (lazy init) ─────────────────────────────────────

_client: Optional[chromadb.HttpClient] = None
_collection: Optional[Any] = None
_embedder: Optional[SentenceTransformer] = None


def _get_client() -> chromadb.HttpClient:
    global _client
    if _client is None:
        _client = chromadb.HttpClient(
            host=CHROMADB_HOST,
            port=CHROMADB_PORT,
            settings=Settings(anonymized_telemetry=False),
        )
        logger.info("ChromaDB client connected to %s:%s", CHROMADB_HOST, CHROMADB_PORT)
    return _client


def _get_collection():
    global _collection
    if _collection is None:
        client = _get_client()
        try:
            _collection = client.get_collection(name=COLLECTION_NAME)
            logger.info("Reusing existing ChromaDB collection: %s (%d docs)",
                        COLLECTION_NAME, _collection.count())
        except Exception:
            try:
                _collection = client.create_collection(
                    name=COLLECTION_NAME,
                    metadata={"hnsw:space": "cosine"},
                )
                logger.info("Created new ChromaDB collection: %s", COLLECTION_NAME)
            except Exception as e2:
                # Fallback: delete+recreate if creation fails (version skew)
                logger.warning("create_collection failed (%s), trying delete+recreate...", e2)
                try:
                    client.delete_collection(name=COLLECTION_NAME)
                except Exception:
                    pass
                _collection = client.create_collection(
                    name=COLLECTION_NAME,
                    metadata={"hnsw:space": "cosine"},
                )
                logger.info("Recreated ChromaDB collection: %s", COLLECTION_NAME)
    return _collection


def _get_embedder() -> SentenceTransformer:
    global _embedder
    if _embedder is None:
        device = os.getenv("RERANKER_DEVICE", "cpu")
        logger.info("Loading embedding model: %s (device=%s)", EMBEDDING_MODEL, device)
        _embedder = SentenceTransformer(EMBEDDING_MODEL, device=device)
        logger.info("Embedding model loaded successfully")
    return _embedder


# ─── Public API ───────────────────────────────────────────────

def vector_search(
    query: str,
    top_k: int = 30,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Dense vector retrieval using cosine similarity.
    Returns top_k documents with metadata.
    """
    coll = _get_collection()
    embedder = _get_embedder()

    query_embedding = embedder.encode(query, normalize_embeddings=True).tolist()

    where_filter = _build_where_filter(filters)
    results = coll.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    docs = []
    if results["ids"] and results["ids"][0]:
        for i, doc_id in enumerate(results["ids"][0]):
            docs.append({
                "chroma_id": doc_id,
                "content": results["documents"][0][i] if results["documents"] else "",
                "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                "distance": results["distances"][0][i] if results["distances"] else 1.0,
                "score": 1.0 - results["distances"][0][i] if results["distances"] else 0.0,
            })

    logger.debug("Vector search returned %d candidates for query[:50]", len(docs))
    return docs


def upsert_document(ticket_id: str, content: str, metadata: Dict[str, Any]) -> str:
    """
    Insert or update a document in ChromaDB.
    Returns the internal chroma_id.

    ChromaDB metadata only supports str, int, float, bool values.
    Lists and dicts are serialised to JSON strings automatically.
    """
    coll = _get_collection()
    embedder = _get_embedder()

    embedding = embedder.encode(content, normalize_embeddings=True).tolist()

    # Serialise complex metadata values to JSON strings
    safe_metadata = {}
    for k, v in metadata.items():
        if isinstance(v, (list, dict)):
            import json
            safe_metadata[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, (str, int, float, bool)):
            safe_metadata[k] = v
        else:
            safe_metadata[k] = str(v)

    # Check if already exists
    existing = coll.get(ids=[ticket_id])
    if existing and existing["ids"]:
        coll.update(
            ids=[ticket_id],
            embeddings=[embedding],
            documents=[content],
            metadatas=[safe_metadata],
        )
        logger.info("Updated document: %s", ticket_id)
    else:
        coll.add(
            ids=[ticket_id],
            embeddings=[embedding],
            documents=[content],
            metadatas=[safe_metadata],
        )
        logger.info("Inserted document: %s", ticket_id)

    return ticket_id


def get_total_count() -> int:
    """Return total number of documents in the collection."""
    return _get_collection().count()


def _build_where_filter(filters: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Convert our filter format to ChromaDB's where clause.

    Supported filters:
      - fault_category: str (exact match)
      - min_confidence: float (>=)
      - successful_only: bool
      - line_profile: str
    """
    if not filters:
        return None

    conditions = []

    if "fault_category" in filters:
        conditions.append({"fault_category": filters["fault_category"]})
    # NOTE: min_confidence / successful_only / line_profile filters are
    # intentionally NOT pushed into the ChromaDB where-clause because the
    # majority of seed / flywheel documents don't carry those metadata keys
    # yet.  Applying them here would silently exclude vector-search results
    # that the BM25 path still finds, hurting recall.  Post-retrieval
    # filtering in bm25_index._passes_filter() handles these softly instead.

    if len(conditions) == 0:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}
