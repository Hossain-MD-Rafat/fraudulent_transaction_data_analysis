"""FastAPI boundary for fraud scoring and operational decisions."""

from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
import sys

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.inference import (  # noqa: E402
    ModelBundle,
    heuristic_reason_codes,
    load_model_bundle,
    predict_probability,
)
from fraud_detection.risk import make_risk_decisions  # noqa: E402


app = FastAPI(
    title="Fraud Intelligence Decision API",
    version="0.2.0",
    description=(
        "Fraud scoring and decision service with a versioned inference feature "
        "contract, model metadata, risk scores, expected exposure, and actions."
    ),
)

DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "fraud_ds1_xgb_v1" / "model.joblib"
DEFAULT_METADATA_PATH = PROJECT_ROOT / "models" / "fraud_ds1_xgb_v1" / "metadata.json"


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class DecisionRequest(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=256)
    amount: float = Field(ge=0)
    fraud_probability: float = Field(ge=0, le=1)
    review_threshold: float = Field(default=0.50, gt=0, lt=1)
    block_threshold: float | None = Field(default=None, gt=0, le=1)

    @model_validator(mode="after")
    def validate_threshold_order(self) -> "DecisionRequest":
        if self.block_threshold is not None and self.block_threshold < self.review_threshold:
            raise ValueError("block_threshold must be greater than or equal to review_threshold")
        return self


class DecisionResponse(BaseModel):
    transaction_id: str
    amount: float
    fraud_probability: float
    risk_score: int
    expected_fraud_loss: float
    decision: str
    review_threshold: float
    block_threshold: float


class ScoreFeatures(BaseModel):
    amt: float = Field(ge=0)
    city_pop: float = Field(ge=0)
    customer_age: float = Field(ge=0, le=120)
    merchant_distance_km: float = Field(ge=0)
    transaction_hour: int = Field(ge=0, le=23)
    transaction_dayofweek: int = Field(ge=0, le=6)
    transaction_month: int = Field(ge=1, le=12)
    is_weekend: int = Field(ge=0, le=1)
    is_night: int = Field(ge=0, le=1)
    prior_txn_count: float = Field(ge=0)
    prior_avg_amount: float = Field(ge=0)
    prior_std_amount: float = Field(ge=0)
    amount_to_prior_mean: float = Field(ge=0)
    amount_zscore_prior: float
    minutes_since_prev_txn: float = Field(ge=0)
    txns_prev_1h: float = Field(ge=0)
    txns_prev_24h: float = Field(ge=0)
    prior_same_merchant_count: float = Field(ge=0)
    prior_same_category_count: float = Field(ge=0)
    is_new_merchant_for_card: int = Field(ge=0, le=1)
    is_new_category_for_card: int = Field(ge=0, le=1)
    category: str = Field(min_length=1, max_length=128)
    gender: str = Field(min_length=1, max_length=32)
    state: str = Field(min_length=1, max_length=32)


class ScoreRequest(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=256)
    features: ScoreFeatures


class ScoreResponse(BaseModel):
    transaction_id: str
    model_version: str
    feature_contract_version: str
    fraud_probability: float
    risk_score: int
    expected_fraud_loss: float
    decision: str
    review_threshold: float
    block_threshold: float
    reason_codes: list[str]
    explanation_method: str


def _effective_block_threshold(review_threshold: float, block_threshold: float | None) -> float:
    if block_threshold is not None:
        return float(block_threshold)
    return float(min(0.995, max(review_threshold + 0.20, review_threshold * 1.75)))


def _artifact_paths() -> tuple[Path, Path]:
    model_path = Path(os.getenv("FRAUD_MODEL_PATH", str(DEFAULT_MODEL_PATH)))
    metadata_path = Path(os.getenv("FRAUD_MODEL_METADATA_PATH", str(DEFAULT_METADATA_PATH)))
    return model_path, metadata_path


@lru_cache(maxsize=1)
def get_model_bundle() -> ModelBundle:
    model_path, metadata_path = _artifact_paths()
    return load_model_bundle(model_path=model_path, metadata_path=metadata_path)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="fraud-intelligence-decision-api",
        version=app.version,
    )


@app.post("/v1/decision", response_model=DecisionResponse)
def make_decision(payload: DecisionRequest) -> DecisionResponse:
    result = make_risk_decisions(
        probabilities=np.array([payload.fraud_probability], dtype=float),
        amounts=np.array([payload.amount], dtype=float),
        review_threshold=payload.review_threshold,
        block_threshold=payload.block_threshold,
    ).iloc[0]

    effective_block_threshold = _effective_block_threshold(
        payload.review_threshold, payload.block_threshold
    )

    return DecisionResponse(
        transaction_id=payload.transaction_id,
        amount=payload.amount,
        fraud_probability=float(result["fraud_probability"]),
        risk_score=int(result["risk_score"]),
        expected_fraud_loss=float(result["expected_fraud_loss"]),
        decision=str(result["decision"]),
        review_threshold=payload.review_threshold,
        block_threshold=effective_block_threshold,
    )


@app.post("/v1/score", response_model=ScoreResponse)
def score_transaction(payload: ScoreRequest) -> ScoreResponse:
    try:
        bundle = get_model_bundle()
    except (FileNotFoundError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Model artifact unavailable: {exc}") from exc

    feature_payload = payload.features.model_dump()
    probability = predict_probability(bundle, feature_payload)
    block_threshold = _effective_block_threshold(
        bundle.review_threshold, bundle.block_threshold
    )
    result = make_risk_decisions(
        probabilities=np.array([probability], dtype=float),
        amounts=np.array([payload.features.amt], dtype=float),
        review_threshold=bundle.review_threshold,
        block_threshold=block_threshold,
    ).iloc[0]

    return ScoreResponse(
        transaction_id=payload.transaction_id,
        model_version=bundle.model_version,
        feature_contract_version=bundle.feature_contract_version,
        fraud_probability=float(result["fraud_probability"]),
        risk_score=int(result["risk_score"]),
        expected_fraud_loss=float(result["expected_fraud_loss"]),
        decision=str(result["decision"]),
        review_threshold=float(bundle.review_threshold),
        block_threshold=block_threshold,
        reason_codes=heuristic_reason_codes(feature_payload),
        explanation_method="heuristic_input_signals_v1",
    )
