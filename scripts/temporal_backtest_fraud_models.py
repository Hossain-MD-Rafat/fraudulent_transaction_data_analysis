"""Rolling temporal backtest for Phase 1 fraud model selection.

This is the more robust rare-event validation path:

1. Reserve the newest 15% as a final untouched test window.
2. Inside the earlier 85%, create expanding-window folds.
3. Generate out-of-time validation probabilities for every candidate model.
4. Select the model by pooled out-of-fold PR-AUC.
5. Select its operating threshold using pooled out-of-fold business cost.
6. Refit the selected model on all pre-test history and evaluate once on test.

Usage:
    python scripts/temporal_backtest_fraud_models.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
for path in (PROJECT_ROOT, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fraud_detection.evaluation import (  # noqa: E402
    expanding_window_folds,
    summarize_fold_labels,
)
from fraud_detection.features import build_fraud_ds1_features  # noqa: E402
from fraud_detection.risk import (  # noqa: E402
    choose_cost_sensitive_threshold,
    make_risk_decisions,
    threshold_cost,
)
from scripts.benchmark_fraud_models import (  # noqa: E402
    FEATURES,
    TARGET,
    build_preprocessor,
    model_registry,
    score_metrics,
)

DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fraud_ds1.csv.gz"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase1"
FINAL_TEST_FRACTION = 0.15
N_FOLDS = 4
MIN_TRAIN_FRACTION = 0.40


def class_weight_ratio(y: pd.Series) -> float:
    negatives = int((y == 0).sum())
    positives = int((y == 1).sum())
    return negatives / max(positives, 1)


def fit_pipeline(model_name: str, X: pd.DataFrame, y: pd.Series) -> Pipeline:
    registry = model_registry(class_weight_ratio(y))
    pipeline = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            ("model", registry[model_name]),
        ]
    )
    pipeline.fit(X, y)
    return pipeline


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(DATA_PATH)
    feature_df = build_fraud_ds1_features(raw).reset_index(drop=True)

    final_test_start = int(len(feature_df) * (1.0 - FINAL_TEST_FRACTION))
    development_df = feature_df.iloc[:final_test_start].copy()
    test_df = feature_df.iloc[final_test_start:].copy()

    folds = expanding_window_folds(
        len(feature_df),
        final_test_fraction=FINAL_TEST_FRACTION,
        n_folds=N_FOLDS,
        min_train_fraction=MIN_TRAIN_FRACTION,
    )
    fold_label_summary = summarize_fold_labels(feature_df, folds, TARGET)

    candidate_names = list(model_registry(1.0).keys())
    model_results: dict[str, dict] = {}

    for model_name in candidate_names:
        pooled_probabilities: list[np.ndarray] = []
        pooled_labels: list[np.ndarray] = []
        pooled_amounts: list[np.ndarray] = []
        per_fold: list[dict] = []

        for fold_idx, fold in enumerate(folds, start=1):
            train_df = feature_df.iloc[fold.train_start : fold.train_end]
            validation_df = feature_df.iloc[fold.validation_start : fold.validation_end]

            y_train = train_df[TARGET].astype(int)
            y_validation = validation_df[TARGET].astype(int)

            if y_train.nunique() < 2:
                raise ValueError(f"Fold {fold_idx} training data has only one class.")
            if y_validation.sum() == 0:
                raise ValueError(
                    f"Fold {fold_idx} validation data has no fraud cases; "
                    "adjust temporal fold boundaries."
                )

            pipeline = fit_pipeline(model_name, train_df[FEATURES], y_train)
            validation_prob = pipeline.predict_proba(validation_df[FEATURES])[:, 1]

            fold_pr_auc = float(average_precision_score(y_validation, validation_prob))
            fold_roc_auc = float(roc_auc_score(y_validation, validation_prob))
            per_fold.append(
                {
                    "fold": fold_idx,
                    "train_rows": int(len(train_df)),
                    "validation_rows": int(len(validation_df)),
                    "validation_positive": int(y_validation.sum()),
                    "pr_auc": fold_pr_auc,
                    "roc_auc": fold_roc_auc,
                }
            )

            pooled_probabilities.append(validation_prob)
            pooled_labels.append(y_validation.to_numpy())
            pooled_amounts.append(validation_df["amt"].to_numpy())

        oof_prob = np.concatenate(pooled_probabilities)
        oof_y = np.concatenate(pooled_labels)
        oof_amounts = np.concatenate(pooled_amounts)

        threshold, estimated_cost = choose_cost_sensitive_threshold(
            oof_y,
            oof_prob,
            oof_amounts,
            false_positive_review_cost=5.0,
            fraud_loss_multiplier=1.0,
        )

        model_results[model_name] = {
            "pooled_oof_pr_auc": float(average_precision_score(oof_y, oof_prob)),
            "pooled_oof_roc_auc": float(roc_auc_score(oof_y, oof_prob)),
            "mean_fold_pr_auc": float(np.mean([fold["pr_auc"] for fold in per_fold])),
            "std_fold_pr_auc": float(np.std([fold["pr_auc"] for fold in per_fold])),
            "threshold": float(threshold),
            "estimated_oof_cost": float(estimated_cost),
            "oof_positive": int(oof_y.sum()),
            "oof_rows": int(len(oof_y)),
            "folds": per_fold,
        }

    best_model_name = max(
        model_results,
        key=lambda name: model_results[name]["pooled_oof_pr_auc"],
    )
    best_threshold = float(model_results[best_model_name]["threshold"])

    y_development = development_df[TARGET].astype(int)
    final_pipeline = fit_pipeline(
        best_model_name,
        development_df[FEATURES],
        y_development,
    )

    y_test = test_df[TARGET].astype(int)
    test_prob = final_pipeline.predict_proba(test_df[FEATURES])[:, 1]
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
    predictions = test_df[
        ["trans_num", "trans_date_trans_time", "amt", TARGET]
    ].reset_index(drop=True)
    predictions = pd.concat([predictions, decisions], axis=1)
    predictions.to_csv(OUTPUT_DIR / "temporal_backtest_test_predictions.csv", index=False)

    results = {
        "method": "expanding_window_oof_model_selection",
        "selection_metric": "pooled_oof_pr_auc",
        "final_test_fraction": FINAL_TEST_FRACTION,
        "n_folds": N_FOLDS,
        "min_train_fraction": MIN_TRAIN_FRACTION,
        "fold_label_summary": fold_label_summary,
        "models": model_results,
        "best_model": best_model_name,
        "review_threshold": best_threshold,
        "development_rows": int(len(development_df)),
        "development_positive": int(y_development.sum()),
        "test_rows": int(len(test_df)),
        "test_positive": int(y_test.sum()),
        "final_test": test_metrics,
        "business_cost_assumptions": {
            "false_positive_review_cost": 5.0,
            "fraud_loss_multiplier": 1.0,
        },
    }

    joblib.dump(final_pipeline, OUTPUT_DIR / "temporal_backtest_best_model.joblib")
    with (OUTPUT_DIR / "temporal_backtest.json").open("w", encoding="utf-8") as file:
        json.dump(results, file, indent=2)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
