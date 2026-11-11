"""Test-only embedder doubles. Production code never fabricates vectors (BUG-MEM-EMB-03)."""
from __future__ import annotations

import hashlib
import math


def fake_vector(seed_text: str, dimension: int) -> list[float]:
    digest = hashlib.sha256(seed_text.encode("utf-8")).digest()
    raw = [((digest[i % len(digest)] / 255.0) * 2.0 - 1.0) for i in range(dimension)]
    norm = math.sqrt(sum(v * v for v in raw)) or 1.0
    return [round(v / norm, 6) for v in raw]


def fake_engine_embed(text, spec):
    return fake_vector(f"{spec.space_fingerprint}::{text}", spec.dimension)


def fake_registry_executor(spec, texts, _role):
    return [fake_vector(f"{spec.space_fingerprint}:{t}", spec.dimension) for t in texts]


class FakeAIEmbedBridge:
    """Mimics LIVE_BRIDGE.execute_sql returning an AI_EMBED row."""

    def __init__(self, dimension_by_model: dict[str, int]):
        self.dims = dimension_by_model

    def execute_sql(self, sql, bindings=None):
        model = sql.split("AI_EMBED('", 1)[1].split("'", 1)[0]
        text = str((bindings or {}).get("1", {}).get("value", "")) if isinstance(bindings, dict) else ""
        return {"mode": "live", "rows": [{"EMBEDDING": fake_vector(f"{model}:{text}", self.dims[model])}]}
