"""Train a reproducible Phase 1 fraud-detection baseline on fraud_ds1.

Usage:
    python scripts/train_fraud_baseline.py

The script uses a chronological split, leakage-aware behavioral features,
class weighting, and fraud-focused metrics. It saves metrics and the fitted
pipeline under outputs/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.features import build_fraud_ds1_features  # noqa: E402

DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fraud_ds1.csv.gz"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase1"

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
TARGET = "is_fraud"


def time_split(df: pd.DataFrame, train_fraction: float = 0.80) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chronological split to mimic training on the past and scoring the future."""
    split_idx = int(len(df) * train_fraction)
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def choose_threshold(y_true: pd.Series, probabilities: np.ndarray) -> float:
    """Choose the validation threshold that maximizes F1 rather than assuming 0.5."""
    precision, recall, thresholds = precision_recall_curve(y_true, probabilities)
    if len(thresholds) == 0:
        return 0.5
    f1 = 2 * precision[:-1] * recall[:-1] / np.clip(precision[:-1] + recall[:-1], 1e-12, None)
    return float(thresholds[int(np.nanargmax(f1))])


def build_pipeline() -> Pipeline:
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", min_frequency=2)),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipe, NUMERIC_FEATURES),
            ("categorical", categorical_pipe, CATEGORICAL_FEATURES),
        ]
    )

    model = LogisticRegression(
        class_weight="balanced",
        max_iter=2000,
        solver="liblinear",
        random_state=42,
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])


def evaluate(y_true: pd.Series, probabilities: np.ndarray, threshold: float) -> dict:
    predictions = (probabilities >= threshold).astype(int)
    return {
        "threshold": threshold,
        "roc_auc": float(roc_auc_score(y_true, probabilities)),
        "pr_auc": float(average_precision_score(y_true, probabilities)),
        "precision": float(precision_score(y_true, predictions, zero_division=0)),
        "recall": float(recall_score(y_true, predictions, zero_division=0)),
        "f1": float(f1_score(y_true, predictions, zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, predictions).tolist(),
        "classification_report": classification_report(
            y_true, predictions, output_dict=True, zero_division=0
        ),
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(DATA_PATH)
    features = build_fraud_ds1_features(raw)
    train_df, test_df = time_split(features)

    X_train = train_df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y_train = train_df[TARGET].astype(int)
    X_test = test_df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y_test = test_df[TARGET].astype(int)

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    test_prob = pipeline.predict_proba(X_test)[:, 1]
    threshold = choose_threshold(y_test, test_prob)
    metrics = evaluate(y_test, test_prob, threshold)
    metrics.update(
        {
            "train_rows": int(len(train_df)),
            "test_rows": int(len(test_df)),
            "train_fraud_rate": float(y_train.mean()),
            "test_fraud_rate": float(y_test.mean()),
            "split_strategy": "chronological_80_20",
            "model": "class_weighted_logistic_regression",
        }
    )

    joblib.dump(pipeline, OUTPUT_DIR / "fraud_baseline.joblib")
    with (OUTPUT_DIR / "metrics.json").open("w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2)

    predictions = test_df[["trans_num", "trans_date_trans_time", "amt", TARGET]].copy()
    predictions["fraud_probability"] = test_prob
    predictions["predicted_fraud"] = (test_prob >= threshold).astype(int)
    predictions.to_csv(OUTPUT_DIR / "test_predictions.csv", index=False)

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
