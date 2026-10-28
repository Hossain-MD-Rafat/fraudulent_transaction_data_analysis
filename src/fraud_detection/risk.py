"""Operational risk-scoring helpers for fraud decisions.

These utilities separate model probability from business action. A model can
rank transactions well while the decision threshold still depends on fraud
loss, manual-review cost, and review capacity.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def expected_fraud_loss(probabilities: np.ndarray, amounts: pd.Series | np.ndarray) -> np.ndarray:
    """Expected dollar exposure if the transaction is approved without review."""
    probs = np.asarray(probabilities, dtype=float)
    amts = np.asarray(amounts, dtype=float)
    return np.clip(probs, 0.0, 1.0) * np.maximum(amts, 0.0)


def threshold_cost(
    y_true: pd.Series | np.ndarray,
    probabilities: np.ndarray,
    amounts: pd.Series | np.ndarray,
    threshold: float,
    false_positive_review_cost: float = 5.0,
    fraud_loss_multiplier: float = 1.0,
) -> float:
    """Estimate operational cost for one classification threshold.

    False negatives incur the transaction amount multiplied by
    ``fraud_loss_multiplier``. False positives incur a fixed manual-review
    cost. This is deliberately configurable because real institutions have
    different chargeback, recovery, and investigation economics.
    """
    y = np.asarray(y_true, dtype=int)
    probs = np.asarray(probabilities, dtype=float)
    amts = np.maximum(np.asarray(amounts, dtype=float), 0.0)
    predicted = probs >= threshold

    false_negative_cost = amts[(y == 1) & (~predicted)].sum() * fraud_loss_multiplier
    false_positive_cost = ((y == 0) & predicted).sum() * false_positive_review_cost
    return float(false_negative_cost + false_positive_cost)


def choose_cost_sensitive_threshold(
    y_true: pd.Series | np.ndarray,
    probabilities: np.ndarray,
    amounts: pd.Series | np.ndarray,
    false_positive_review_cost: float = 5.0,
    fraud_loss_multiplier: float = 1.0,
    grid_size: int = 500,
) -> tuple[float, float]:
    """Choose the validation threshold with the lowest estimated business cost."""
    probs = np.asarray(probabilities, dtype=float)
    if probs.size == 0:
        return 0.5, 0.0

    candidates = np.unique(
        np.concatenate(
            [
                np.linspace(0.001, 0.999, grid_size),
                np.clip(probs, 0.001, 0.999),
            ]
        )
    )

    costs = np.array(
        [
            threshold_cost(
                y_true,
                probs,
                amounts,
                threshold,
                false_positive_review_cost=false_positive_review_cost,
                fraud_loss_multiplier=fraud_loss_multiplier,
            )
            for threshold in candidates
        ]
    )
    best_idx = int(np.argmin(costs))
    return float(candidates[best_idx]), float(costs[best_idx])


def make_risk_decisions(
    probabilities: np.ndarray,
    amounts: pd.Series | np.ndarray,
    review_threshold: float,
    block_threshold: float | None = None,
) -> pd.DataFrame:
    """Convert probabilities into analyst-friendly scores and decisions.

    ``review_threshold`` should normally be selected on validation data.
    ``block_threshold`` defaults to a more conservative threshold above the
    review threshold. In a real deployment, the block policy should be set by
    the fraud/risk organization rather than by model metrics alone.
    """
    probs = np.clip(np.asarray(probabilities, dtype=float), 0.0, 1.0)
    if block_threshold is None:
        block_threshold = min(0.995, max(review_threshold + 0.20, review_threshold * 1.75))
    block_threshold = max(float(block_threshold), float(review_threshold))

    risk_score = np.rint(probs * 100.0).astype(int)
    decision = np.where(
        probs >= block_threshold,
        "BLOCK",
        np.where(probs >= review_threshold, "REVIEW", "APPROVE"),
    )

    return pd.DataFrame(
        {
            "fraud_probability": probs,
            "risk_score": risk_score,
            "expected_fraud_loss": expected_fraud_loss(probs, amounts),
            "decision": decision,
        }
    )
