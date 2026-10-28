"""FastAPI boundary for operational fraud decisions.

Phase 3 starts by exposing the already-tested risk policy as a service. This
endpoint intentionally accepts a model probability rather than loading a model
artifact yet; model serving will be added once artifact versioning and feature
contracts are defined.
"""

from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel, Field, model_validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.risk import make_risk_decisions


app = FastAPI(
    title="Fraud Intelligence Decision API",
    version="0.1.0",
    description=(
        "Operational decision layer for converting fraud probabilities into "
        "risk scores, expected exposure, and approve/review/block actions."
    ),
)


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

    effective_block_threshold = (
        payload.block_threshold
        if payload.block_threshold is not None
        else min(
            0.995,
            max(
                payload.review_threshold + 0.20,
                payload.review_threshold * 1.75,
            ),
        )
    )

    return DecisionResponse(
        transaction_id=payload.transaction_id,
        amount=payload.amount,
        fraud_probability=float(result["fraud_probability"]),
        risk_score=int(result["risk_score"]),
        expected_fraud_loss=float(result["expected_fraud_loss"]),
        decision=str(result["decision"]),
        review_threshold=payload.review_threshold,
        block_threshold=float(effective_block_threshold),
    )
