#!/usr/bin/env python3
"""
Pre-download HuggingFace model files to local cache (no runtime loading).

Downloads the model weights/configs/tokenizers from HuggingFace Hub
without importing PyTorch, scipy, or sentence-transformers.
This avoids GLIBCXX/GPU driver version conflicts on the host.

The Docker container (which has its own libstdc++.so.6) will load
these cached files at runtime.

Usage:
    python3 scripts/download_models.py
    HF_ENDPOINT=https://hf-mirror.com python3 scripts/download_models.py
"""

from __future__ import annotations

import os
import sys
import argparse
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [models] %(message)s")
logger = logging.getLogger(__name__)

MODELS = [
    {
        "repo_id": "BAAI/bge-large-zh-v1.5",
        "description": "Chinese embedding model (1024-dim, ~1.3 GB)",
    },
    {
        "repo_id": "BAAI/bge-reranker-v2-m3",
        "description": "Multilingual cross-encoder reranker (~1.5 GB)",
    },
]


def main():
    parser = argparse.ArgumentParser(description="Download HF model files to cache")
    parser.add_argument(
        "--cache-dir",
        default=os.path.expanduser("~/.cache/huggingface"),
        help="Cache directory (default: ~/.cache/huggingface)",
    )
    args = parser.parse_args()

    cache_dir = os.path.abspath(args.cache_dir)
    os.makedirs(cache_dir, exist_ok=True)
    os.environ["HF_HOME"] = cache_dir

    # Use huggingface_hub to download files only (no model loading)
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        logger.info("Installing huggingface_hub...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "huggingface_hub", "-q"])
        from huggingface_hub import snapshot_download

    total_size = 0
    for i, model in enumerate(MODELS, 1):
        logger.info("=" * 60)
        logger.info("Model %d/%d: %s", i, len(MODELS), model["repo_id"])
        logger.info("  %s", model["description"])
        logger.info("=" * 60)

        t0 = time.monotonic()
        try:
            local_path = snapshot_download(
                repo_id=model["repo_id"],
                cache_dir=cache_dir,
                resume_download=True,
                max_workers=4,
            )
            elapsed = time.monotonic() - t0

            # Calculate total downloaded size
            size_mb = 0
            for root, dirs, files in os.walk(local_path):
                for f in files:
                    fp = os.path.join(root, f)
                    if os.path.isfile(fp):
                        size_mb += os.path.getsize(fp) / (1024 * 1024)

            total_size += size_mb
            logger.info("  ✅ Downloaded to: %s", local_path)
            logger.info("  Size: %.0f MB | Time: %.0fs", size_mb, elapsed)

        except Exception as e:
            logger.error("  ❌ Download failed: %s", e)
            logger.error("  Try setting a mirror: export HF_ENDPOINT=https://hf-mirror.com")

    # Verify
    logger.info("")
    logger.info("=" * 60)
    logger.info("Download complete. Total: %.0f MB", total_size)
    logger.info("Cache directory: %s", cache_dir)

    # List downloaded repos
    hub_dir = os.path.join(cache_dir, "hub")
    if os.path.isdir(hub_dir):
        logger.info("Cached models:")
        for entry in sorted(os.listdir(hub_dir)):
            if entry.startswith("models--"):
                model_name = entry.replace("models--", "").replace("--", "/")
                size_mb = _dir_size(os.path.join(hub_dir, entry)) / (1024 * 1024)
                logger.info("  ✅ %s (%.0f MB)", model_name, size_mb)

    logger.info("")
    logger.info("The Docker RAG service will use: %s:/root/.cache/huggingface", cache_dir)


def _dir_size(path: str) -> int:
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


if __name__ == "__main__":
    main()
