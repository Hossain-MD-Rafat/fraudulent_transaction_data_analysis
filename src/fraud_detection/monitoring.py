"""Lightweight operational logging for fraud scoring.

Logs intentionally exclude raw feature payloads and direct transaction IDs.
The hashed identifier supports request correlation without writing the original
identifier to the monitoring file.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any


def hash_identifier(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def monitoring_log_path() -> Path | None:
    configured = os.getenv("FRAUD_MONITORING_LOG")
    if not configured:
        return None
    return Path(configured)


def log_score_event(
    *,
    transaction_id: str,
    model_version: str,
    feature_contract_version: str,
    fraud_probability: float,
    model_risk_score: int,
    graph_risk_score: int | None,
    unified_risk_score: int,
    decision: str,
    expected_fraud_loss: float,
    graph_available: bool,
    explanation_method: str,
    path: Path | None = None,
) -> bool:
    """Append one JSONL scoring event when monitoring is configured."""
    target = path if path is not None else monitoring_log_path()
    if target is None:
        return False

    target.parent.mkdir(parents=True, exist_ok=True)
    event: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "transaction_hash": hash_identifier(transaction_id),
        "model_version": model_version,
        "feature_contract_version": feature_contract_version,
        "fraud_probability": round(float(fraud_probability), 8),
        "model_risk_score": int(model_risk_score),
        "graph_risk_score": None if graph_risk_score is None else int(graph_risk_score),
        "unified_risk_score": int(unified_risk_score),
        "decision": decision,
        "expected_fraud_loss": round(float(expected_fraud_loss), 2),
        "graph_available": bool(graph_available),
        "explanation_method": explanation_method,
    }
    with target.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, separators=(",", ":")) + "\n")
    return True
