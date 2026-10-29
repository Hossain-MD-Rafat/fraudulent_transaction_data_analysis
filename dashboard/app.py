"""Streamlit analyst dashboard for Phase 3 fraud intelligence.

The dashboard is intentionally useful before live model serving is introduced:
analysts can upload scored transaction output from the benchmark/training
pipeline, prioritize cases by expected fraud exposure, and test the operational
decision policy on individual probabilities.
"""

from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.risk import make_risk_decisions


st.set_page_config(
    page_title="Fraud Intelligence Analyst Console",
    page_icon="🔎",
    layout="wide",
)

st.title("Fraud Intelligence Analyst Console")
st.caption(
    "Prioritize fraud alerts by probability and financial exposure, then inspect "
    "the operational approve/review/block policy."
)


def normalize_scored_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common prediction-column names into the dashboard contract."""
    out = df.copy()

    probability_candidates = [
        "fraud_probability",
        "probability",
        "predicted_probability",
        "y_probability",
    ]
    amount_candidates = ["amount", "amt", "transaction_amount"]
    id_candidates = ["transaction_id", "TransactionID", "trans_num", "transactionid"]

    probability_col = next((c for c in probability_candidates if c in out.columns), None)
    amount_col = next((c for c in amount_candidates if c in out.columns), None)
    id_col = next((c for c in id_candidates if c in out.columns), None)

    if probability_col is None:
        raise ValueError(
            "CSV must contain a fraud probability column such as fraud_probability."
        )
    if amount_col is None:
        raise ValueError("CSV must contain an amount column such as amount or amt.")

    out["fraud_probability"] = pd.to_numeric(out[probability_col], errors="coerce")
    out["amount"] = pd.to_numeric(out[amount_col], errors="coerce")
    out["transaction_id"] = (
        out[id_col].astype(str)
        if id_col is not None
        else pd.Series([f"row-{i}" for i in range(len(out))], index=out.index)
    )

    out = out.dropna(subset=["fraud_probability", "amount"])
    out = out[out["fraud_probability"].between(0, 1)]
    out = out[out["amount"] >= 0]
    return out


def enrich_decisions(
    df: pd.DataFrame,
    review_threshold: float,
    block_threshold: float,
) -> pd.DataFrame:
    """Attach risk score, expected exposure and operational decision."""
    decisions = make_risk_decisions(
        probabilities=df["fraud_probability"].to_numpy(),
        amounts=df["amount"].to_numpy(),
        review_threshold=review_threshold,
        block_threshold=block_threshold,
    )
    enriched = df.reset_index(drop=True).copy()
    for column in decisions.columns:
        enriched[column] = decisions[column].to_numpy()
    return enriched


with st.sidebar:
    st.header("Decision policy")
    review_threshold = st.slider(
        "Review threshold",
        min_value=0.01,
        max_value=0.95,
        value=0.50,
        step=0.01,
    )
    block_threshold = st.slider(
        "Block threshold",
        min_value=float(review_threshold),
        max_value=1.00,
        value=max(0.90, float(review_threshold)),
        step=0.01,
    )
    st.caption(
        "These are demonstration policy controls. Production thresholds should be "
        "approved by the fraud/risk organization using measured business costs."
    )

summary_tab, queue_tab, sandbox_tab = st.tabs(
    ["Portfolio overview", "Investigation queue", "Decision sandbox"]
)

uploaded_file = st.file_uploader(
    "Upload a scored transaction CSV",
    type=["csv"],
    help=(
        "Required: fraud probability and transaction amount. A transaction ID is "
        "recommended but optional."
    ),
)

scored: pd.DataFrame | None = None
if uploaded_file is not None:
    try:
        raw = pd.read_csv(uploaded_file)
        scored = enrich_decisions(
            normalize_scored_transactions(raw),
            review_threshold=review_threshold,
            block_threshold=block_threshold,
        )
    except Exception as exc:
        st.error(f"Could not load scored transactions: {exc}")

with summary_tab:
    if scored is None:
        st.info("Upload a scored CSV to populate the portfolio overview.")
    else:
        total_transactions = len(scored)
        review_count = int((scored["decision"] == "REVIEW").sum())
        block_count = int((scored["decision"] == "BLOCK").sum())
        exposure = float(scored["expected_fraud_loss"].sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Transactions", f"{total_transactions:,}")
        c2.metric("Review queue", f"{review_count:,}")
        c3.metric("Block candidates", f"{block_count:,}")
        c4.metric("Expected exposure", f"${exposure:,.2f}")

        st.subheader("Decision distribution")
        distribution = (
            scored["decision"].value_counts().rename_axis("decision").reset_index(name="count")
        )
        st.bar_chart(distribution.set_index("decision"))

        st.subheader("Risk-score distribution")
        bins = pd.cut(
            scored["risk_score"],
            bins=[-1, 20, 40, 60, 80, 100],
            labels=["0–20", "21–40", "41–60", "61–80", "81–100"],
        )
        risk_distribution = bins.value_counts(sort=False)
        st.bar_chart(risk_distribution)

with queue_tab:
    if scored is None:
        st.info("Upload a scored CSV to build the investigation queue.")
    else:
        actionable = scored[scored["decision"] != "APPROVE"].copy()
        actionable = actionable.sort_values(
            ["expected_fraud_loss", "fraud_probability"], ascending=False
        )

        decision_filter = st.multiselect(
            "Decisions",
            options=["REVIEW", "BLOCK"],
            default=["REVIEW", "BLOCK"],
        )
        minimum_exposure = st.number_input(
            "Minimum expected exposure ($)",
            min_value=0.0,
            value=0.0,
            step=25.0,
        )

        filtered = actionable[
            actionable["decision"].isin(decision_filter)
            & (actionable["expected_fraud_loss"] >= minimum_exposure)
        ]

        st.caption(
            "Cases are ordered by expected fraud exposure first, so high-dollar "
            "risk can be investigated before low-value alerts."
        )
        display_columns = [
            "transaction_id",
            "amount",
            "fraud_probability",
            "risk_score",
            "expected_fraud_loss",
            "decision",
        ]
        st.dataframe(
            filtered[display_columns],
            use_container_width=True,
            hide_index=True,
        )

with sandbox_tab:
    st.subheader("Single-transaction policy simulator")
    transaction_id = st.text_input("Transaction ID", value="demo-transaction")
    amount = st.number_input("Amount ($)", min_value=0.0, value=1000.0, step=50.0)
    probability = st.slider(
        "Fraud probability",
        min_value=0.0,
        max_value=1.0,
        value=0.60,
        step=0.01,
    )

    decision = make_risk_decisions(
        probabilities=np.array([probability]),
        amounts=np.array([amount]),
        review_threshold=review_threshold,
        block_threshold=block_threshold,
    ).iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Risk score", f"{int(decision['risk_score'])}/100")
    c2.metric("Expected exposure", f"${float(decision['expected_fraud_loss']):,.2f}")
    c3.metric("Decision", str(decision["decision"]))

    st.json(
        {
            "transaction_id": transaction_id,
            "amount": amount,
            "fraud_probability": probability,
            "risk_score": int(decision["risk_score"]),
            "expected_fraud_loss": round(float(decision["expected_fraud_loss"]), 2),
            "decision": str(decision["decision"]),
        }
    )
