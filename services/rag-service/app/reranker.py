"""
Two-stage reranker:

  Stage 1 (coarse): BGE-Reranker-v2-m3  — 30 candidates → 10
  Stage 2 (fine):   DeepSeek-V3 LLM       — 10 candidates → 3

The coarse reranker runs locally (CPU/GPU), the fine reranker calls
the external LLM API.
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List, Optional

import httpx
from sentence_transformers import CrossEncoder

logger = logging.getLogger(__name__)

RERANKER_MODEL = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-v2-m3")
RERANKER_DEVICE = os.getenv("RERANKER_DEVICE", "cpu")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")
COARSE_TOP_K = int(os.getenv("COARSE_TOP_K", "10"))
FINAL_TOP_K = int(os.getenv("FINAL_TOP_K", "3"))
HF_HOME = os.getenv("HF_HOME", os.path.expanduser("~/.cache/huggingface"))

# ─── Globals ──────────────────────────────────────────────────

_reranker: Optional[CrossEncoder] = None


def _find_local_snapshot(model_name: str) -> str | None:
    """Find the local snapshot directory for a HuggingFace model."""
    # Convert model name to cache path: BAAI/bge-reranker-v2-m3 → models--BAAI--bge-reranker-v2-m3
    cache_dir = os.path.join(HF_HOME, "hub")
    dir_name = "models--" + model_name.replace("/", "--")
    model_dir = os.path.join(cache_dir, dir_name, "snapshots")
    if os.path.isdir(model_dir):
        snapshots = sorted(os.listdir(model_dir))
        if snapshots:
            return os.path.join(model_dir, snapshots[-1])
    # Also try the non-hub cache location
    alt_dir = os.path.join(HF_HOME, dir_name, "snapshots")
    if os.path.isdir(alt_dir):
        snapshots = sorted(os.listdir(alt_dir))
        if snapshots:
            return os.path.join(alt_dir, snapshots[-1])
    return None


def _get_reranker() -> CrossEncoder:
    global _reranker
    if _reranker is None:
        logger.info("Loading reranker model: %s (device=%s)", RERANKER_MODEL, RERANKER_DEVICE)

        # Try loading from local snapshot first (offline-compatible)
        local_path = _find_local_snapshot(RERANKER_MODEL)
        if local_path:
            logger.info("Found local snapshot: %s", local_path)
            _reranker = CrossEncoder(local_path, device=RERANKER_DEVICE, max_length=512)
        else:
            logger.info("Local snapshot not found, trying hub download...")
            _reranker = CrossEncoder(RERANKER_MODEL, device=RERANKER_DEVICE, max_length=512)

        logger.info("Reranker model loaded successfully")
    return _reranker


# ─── Stage 1: BGE Coarse Rerank ───────────────────────────────

def coarse_rerank(query: str, candidates: List[Dict[str, Any]], top_k: int = COARSE_TOP_K) -> List[Dict[str, Any]]:
    """
    Use BGE-Reranker-v2-m3 to rescore candidates.
    Each candidate should have 'content' (text) and 'metadata'.
    Returns top_k rescored candidates.
    """
    if not candidates:
        return []

    reranker = _get_reranker()

    # Build (query, doc_text) pairs
    pairs = []
    for doc in candidates:
        doc_text = doc.get("content", "")
        if not doc_text:
            doc_text = json.dumps(doc.get("metadata", {}), ensure_ascii=False)
        pairs.append([query, doc_text[:500]])

    # Score all pairs
    scores = reranker.predict(pairs, show_progress_bar=False)

    # Sort by score descending
    ranked = sorted(
        zip(candidates, scores),
        key=lambda x: x[1],
        reverse=True,
    )

    results = []
    for doc, score in ranked[:top_k]:
        doc["rerank_score"] = round(float(score), 4)
        results.append(doc)

    logger.debug("BGE coarse rerank: %d → %d candidates", len(candidates), len(results))
    return results


# ─── Stage 2: DeepSeek-V3 Fine Rerank ─────────────────────────

async def fine_rerank(
    query: str,
    candidates: List[Dict[str, Any]],
    top_k: int = FINAL_TOP_K,
) -> List[Dict[str, Any]]:
    """
    Use DeepSeek-V3 to select the best-matching historical cases.
    Returns top_k candidates with match_reason annotations.
    """
    if not candidates:
        return []

    if not DEEPSEEK_API_KEY:
        logger.warning("DEEPSEEK_API_KEY not set — returning BGE top results without LLM rerank")
        for doc in candidates[:top_k]:
            doc["relevance_score"] = doc.get("rerank_score", doc.get("rrf_score", 0.5))
            doc["match_reason"] = "[BGE only] No LLM API key configured"
        return candidates[:top_k]

    prompt = _build_rerank_prompt(query, candidates)
    llm_result = await _call_llm(prompt)

    # LLM returns ordered ticket_ids with relevance scores
    ranked_ids = llm_result.get("rankings", [])
    id_map = {doc.get("ticket_id", ""): doc for doc in candidates}

    results = []
    for item in ranked_ids[:top_k]:
        tid = item.get("ticket_id", "")
        doc = id_map.get(tid)
        if doc:
            doc["relevance_score"] = item.get("relevance_score", 0.5)
            doc["match_reason"] = item.get("reason", "")
            results.append(doc)

    logger.debug("LLM fine rerank: %d → %d candidates", len(candidates), len(results))
    return results


# ─── Full Pipeline ────────────────────────────────────────────

async def rerank_pipeline(
    query: str,
    fused_candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Run both stages: BGE coarse → LLM fine."""
    coarse = coarse_rerank(query, fused_candidates)
    fine = await fine_rerank(query, coarse)
    return fine


# ─── Helpers ──────────────────────────────────────────────────

def _build_rerank_prompt(query: str, candidates: List[Dict[str, Any]]) -> str:
    lines = [
        "你是一个工业故障检索专家。请根据用户查询，对以下候选历史工单进行重排序。",
        "选出最可能与当前故障匹配的工单（最多3个），给出相关性评分(0-1)和匹配理由。",
        "",
        f"## 当前故障查询\n{query}",
        "",
        "## 候选历史工单",
    ]
    for i, doc in enumerate(candidates):
        meta = doc.get("metadata", {})
        lines.append(
            f"### 候选{i+1} (ticket_id={doc.get('ticket_id', '?')})\n"
            f"- 类别: {meta.get('fault_category', '?')}\n"
            f"- 严重度: {meta.get('severity', '?')}\n"
            f"- 内容: {doc.get('content', '')[:300]}"
        )
        lines.append("")

    lines.extend([
        "请输出严格JSON格式:",
        '{"rankings": [',
        '  {"ticket_id": "...", "relevance_score": 0.92, "reason": "为何匹配的简短理由"},',
        '  ...',
        ']}',
    ])
    return "\n".join(lines)


async def _call_llm(prompt: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是一个专业的检索重排序专家。始终输出严格JSON，不要markdown代码块。"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 1500,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1]
                if content.endswith("```"):
                    content = content[:-3]
            return json.loads(content)
    except Exception as e:
        logger.error("LLM fine rerank failed: %s", e)
        return {"rankings": []}
