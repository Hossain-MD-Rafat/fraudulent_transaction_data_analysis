"""Benchmark interpretable and tree-based fraud models on fraud_ds1.

The evaluation is intentionally chronological:
    train (oldest 70%) -> validation (next 15%) -> test (newest 15%)

Thresholds are selected only on validation data. The final test set remains
untouched until the chosen model and threshold are fixed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.features import build_fraud_ds1_features  # noqa: E402
from fraud_detection.risk import (  # noqa: E402
    choose_cost_sensitive_threshold,
    make_risk_decisions,
    threshold_cost,
)

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
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = "is_fraud"


def chronological_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    train_end = int(n * 0.70)
    validation_end = int(n * 0.85)
    return (
        df.iloc[:train_end].copy(),
        df.iloc[train_end:validation_end].copy(),
        df.iloc[validation_end:].copy(),
    )


def build_preprocessor() -> ColumnTransformer:
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
    return ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipe, NUMERIC_FEATURES),
            ("categorical", categorical_pipe, CATEGORICAL_FEATURES),
        ]
    )


def model_registry(scale_pos_weight: float) -> dict[str, object]:
    return {
        "logistic_regression": LogisticRegression(
            class_weight="balanced",
            max_iter=2000,
            solver="liblinear",
            random_state=42,
        ),
        "random_forest": RandomForestClassifier(
            n_estimators=500,
            max_depth=None,
            min_samples_leaf=2,
            class_weight="balanced_subsample",
            n_jobs=-1,
            random_state=42,
        ),
        "xgboost": XGBClassifier(
            n_estimators=500,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            min_child_weight=2,
            reg_lambda=1.0,
            objective="binary:logistic",
            eval_metric="aucpr",
            scale_pos_weight=max(scale_pos_weight, 1.0),
            random_state=42,
            n_jobs=-1,
        ),
    }


def score_metrics(y_true: pd.Series, probabilities: np.ndarray, threshold: float) -> dict:
    predicted = (probabilities >= threshold).astype(int)
    return {
        "roc_auc": float(roc_auc_score(y_true, probabilities)),
        "pr_auc": float(average_precision_score(y_true, probabilities)),
        "precision": float(precision_score(y_true, predicted, zero_division=0)),
        "recall": float(recall_score(y_true, predicted, zero_division=0)),
        "f1": float(f1_score(y_true, predicted, zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, predicted).tolist(),
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(DATA_PATH)
    feature_df = build_fraud_ds1_features(raw)
    train_df, validation_df, test_df = chronological_split(feature_df)

    X_train, y_train = train_df[FEATURES], train_df[TARGET].astype(int)
    X_validation, y_validation = validation_df[FEATURES], validation_df[TARGET].astype(int)
    X_test, y_test = test_df[FEATURES], test_df[TARGET].astype(int)

    negatives = int((y_train == 0).sum())
    positives = int((y_train == 1).sum())
    scale_pos_weight = negatives / max(positives, 1)

    benchmark: dict[str, dict] = {}
    fitted: dict[str, Pipeline] = {}

    for name, estimator in model_registry(scale_pos_weight).items():
        pipeline = Pipeline(
            steps=[
                ("preprocessor", build_preprocessor()),
                ("model", estimator),
            ]
        )
        pipeline.fit(X_train, y_train)

        validation_prob = pipeline.predict_proba(X_validation)[:, 1]
        threshold, validation_cost = choose_cost_sensitive_threshold(
            y_validation,
            validation_prob,
            validation_df["amt"],
            false_positive_review_cost=5.0,
            fraud_loss_multiplier=1.0,
        )

        benchmark[name] = {
            "threshold": threshold,
            "validation_estimated_cost": validation_cost,
            "validation": score_metrics(y_validation, validation_prob, threshold),
        }
        fitted[name] = pipeline

    # Primary selection criterion: validation PR-AUC. Estimated cost is retained
    # as an operational metric and used for threshold selection within each model.
    best_name = max(benchmark, key=lambda name: benchmark[name]["validation"]["pr_auc"])
    best_pipeline = fitted[best_name]
    best_threshold = float(benchmark[best_name]["threshold"])

    test_prob = best_pipeline.predict_proba(X_test)[:, 1]
    test_metrics = score_metrics(y_test, test_prob, best_threshold)
    test_metrics["estimated_cost"] = threshold_cost(
        y_test,
        test_prob,
        test_df["amt"],
        best_threshold,
        false_positive_review_cost=5.0,
        fraud_loss_multiplier=1.0,
    )

    decisions = make_risk_decisions(
        test_prob,
        test_df["amt"],
        review_threshold=best_threshold,
    )

    predictions = test_df[["trans_num", "trans_date_trans_time", "amt", TARGET]].reset_index(drop=True)
    predictions = pd.concat([predictions, decisions], axis=1)
    predictions.to_csv(OUTPUT_DIR / "best_model_test_predictions.csv", index=False)

    metadata = {
        "best_model": best_name,
        "selection_metric": "validation_pr_auc",
        "review_threshold": best_threshold,
        "split_strategy": "chronological_70_15_15",
        "false_positive_review_cost": 5.0,
        "fraud_loss_multiplier": 1.0,
        "rows": {
            "train": len(train_df),
            "validation": len(validation_df),
            "test": len(test_df),
        },
        "fraud_rates": {
            "train": float(y_train.mean()),
            "validation": float(y_validation.mean()),
            "test": float(y_test.mean()),
        },
        "models": benchmark,
        "final_test": test_metrics,
    }

    joblib.dump(best_pipeline, OUTPUT_DIR / "best_fraud_model.joblib")
    with (OUTPUT_DIR / "model_benchmark.json").open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
