"""Versioned model loading and inference utilities for Phase 3.

The production boundary is deliberately separate from training code. A scorer
loads a serialized sklearn-compatible pipeline plus JSON metadata describing
its feature contract and operating thresholds.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


NUMERIC_FEATURES = [
    "amt",
    "city_pop",
    "customer_age",
    "merchant_distance_km",
    "transaction_hour",
    "transaction_dayofweek",
    "transaction_month",
    "is_weekend",
    "is_night",
    "prior_txn_count",
    "prior_avg_amount",
    "prior_std_amount",
    "amount_to_prior_mean",
    "amount_zscore_prior",
    "minutes_since_prev_txn",
    "txns_prev_1h",
    "txns_prev_24h",
    "prior_same_merchant_count",
    "prior_same_category_count",
    "is_new_merchant_for_card",
    "is_new_category_for_card",
]

CATEGORICAL_FEATURES = ["category", "gender", "state"]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
FEATURE_CONTRACT_VERSION = "fraud_ds1_features_v1"


@dataclass(frozen=True)
class ModelBundle:
    model: Any
    model_version: str
    feature_contract_version: str
    review_threshold: float
    block_threshold: float | None
    metadata: dict[str, Any]


def _validate_metadata(metadata: dict[str, Any]) -> None:
    required = {
        "model_version",
        "feature_contract_version",
        "features",
        "review_threshold",
    }
    missing = sorted(required.difference(metadata))
    if missing:
        raise ValueError(f"Model metadata missing required fields: {missing}")

    if metadata["feature_contract_version"] != FEATURE_CONTRACT_VERSION:
        raise ValueError(
            "Unsupported feature contract version: "
            f"{metadata['feature_contract_version']}"
        )

    if list(metadata["features"]) != FEATURES:
        raise ValueError("Model metadata feature order does not match service contract")


def load_model_bundle(model_path: Path, metadata_path: Path) -> ModelBundle:
    """Load and validate a serialized model and its versioned metadata."""
    if not model_path.exists():
        raise FileNotFoundError(f"Model artifact not found: {model_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Model metadata not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8") as file:
        metadata = json.load(file)
    _validate_metadata(metadata)

    model = joblib.load(model_path)
    if not hasattr(model, "predict_proba"):
        raise TypeError("Loaded model must expose predict_proba")

    return ModelBundle(
        model=model,
        model_version=str(metadata["model_version"]),
        feature_contract_version=str(metadata["feature_contract_version"]),
        review_threshold=float(metadata["review_threshold"]),
        block_threshold=(
            None
            if metadata.get("block_threshold") is None
            else float(metadata["block_threshold"])
        ),
        metadata=metadata,
    )


def feature_row(payload: dict[str, Any]) -> pd.DataFrame:
    """Create one ordered model input row from validated API data."""
    missing = [name for name in FEATURES if name not in payload]
    if missing:
        raise ValueError(f"Missing inference features: {missing}")
    return pd.DataFrame([{name: payload[name] for name in FEATURES}], columns=FEATURES)


def predict_probability(bundle: ModelBundle, payload: dict[str, Any]) -> float:
    """Return the positive-class probability for one engineered feature row."""
    row = feature_row(payload)
    probabilities = np.asarray(bundle.model.predict_proba(row), dtype=float)
    if probabilities.shape != (1, 2):
        raise ValueError(
            "Model predict_proba must return shape (1, 2) for binary fraud scoring"
        )
    return float(np.clip(probabilities[0, 1], 0.0, 1.0))


def heuristic_reason_codes(payload: dict[str, Any]) -> list[str]:
    """Return transparent input-based reason codes until SHAP is attached.

    These are not presented as model feature attributions. They are simple
    investigator cues derived from the engineered feature values.
    """
    reasons: list[str] = []

    if float(payload.get("amount_zscore_prior", 0.0)) >= 3.0:
        reasons.append("AMOUNT_HIGH_VS_CUSTOMER_HISTORY")
    if float(payload.get("merchant_distance_km", 0.0)) >= 500.0:
        reasons.append("MERCHANT_FAR_FROM_CUSTOMER")
    if int(payload.get("txns_prev_1h", 0)) >= 3:
        reasons.append("HIGH_TRANSACTION_VELOCITY_1H")
    if int(payload.get("is_new_merchant_for_card", 0)) == 1:
        reasons.append("NEW_MERCHANT_FOR_CARD")
    if int(payload.get("is_new_category_for_card", 0)) == 1:
        reasons.append("NEW_CATEGORY_FOR_CARD")
    if int(payload.get("is_night", 0)) == 1:
        reasons.append("NIGHTTIME_TRANSACTION")

    return reasons[:5]
