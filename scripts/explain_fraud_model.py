"""Generate global SHAP feature importance for the selected Phase 1 model.

Run after:
    python scripts/benchmark_fraud_models.py

Outputs:
    outputs/phase1/shap_feature_importance.csv
    outputs/phase1/shap_top_features.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import shap

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.features import build_fraud_ds1_features  # noqa: E402

DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fraud_ds1.csv.gz"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "phase1"
MODEL_PATH = OUTPUT_DIR / "best_fraud_model.joblib"

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


def chronological_test_slice(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[int(len(df) * 0.85) :].copy()


def to_dense(matrix) -> np.ndarray:
    return matrix.toarray() if hasattr(matrix, "toarray") else np.asarray(matrix)


def extract_positive_class_shap(values: np.ndarray) -> np.ndarray:
    """Normalize common binary-class SHAP output shapes to [rows, features]."""
    arr = np.asarray(values)
    if arr.ndim == 3:
        # Recent SHAP may return [rows, features, classes].
        return arr[:, :, 1]
    return arr


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"{MODEL_PATH} not found. Run scripts/benchmark_fraud_models.py first."
        )

    raw = pd.read_csv(DATA_PATH)
    features = build_fraud_ds1_features(raw)
    test_df = chronological_test_slice(features)

    pipeline = joblib.load(MODEL_PATH)
    preprocessor = pipeline.named_steps["preprocessor"]
    estimator = pipeline.named_steps["model"]

    sample = test_df[FEATURES].sample(n=min(300, len(test_df)), random_state=42)
    transformed = to_dense(preprocessor.transform(sample))
    feature_names = np.asarray(preprocessor.get_feature_names_out(), dtype=str)

    model_name = estimator.__class__.__name__
    if model_name in {"RandomForestClassifier", "XGBClassifier"}:
        explainer = shap.TreeExplainer(estimator)
        shap_values = extract_positive_class_shap(explainer.shap_values(transformed))
    elif model_name == "LogisticRegression":
        background = transformed[: min(200, len(transformed))]
        explainer = shap.LinearExplainer(estimator, background)
        shap_values = extract_positive_class_shap(explainer.shap_values(transformed))
    else:
        background = transformed[: min(100, len(transformed))]
        explainer = shap.Explainer(estimator.predict_proba, background)
        explanation = explainer(transformed)
        shap_values = extract_positive_class_shap(explanation.values)

    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    importance = (
        pd.DataFrame({"feature": feature_names, "mean_abs_shap": mean_abs_shap})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    importance.to_csv(OUTPUT_DIR / "shap_feature_importance.csv", index=False)

    top_features = {
        "model": model_name,
        "sample_rows": int(len(sample)),
        "top_features": importance.head(20).to_dict(orient="records"),
    }
    with (OUTPUT_DIR / "shap_top_features.json").open("w", encoding="utf-8") as file:
        json.dump(top_features, file, indent=2)

    print(json.dumps(top_features, indent=2))


if __name__ == "__main__":
    main()
