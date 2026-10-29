"""Build an aggregate feature-distribution reference for drift monitoring."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.drift import build_reference_profile  # noqa: E402
from fraud_detection.features import build_fraud_ds1_features  # noqa: E402
from fraud_detection.inference import CATEGORICAL_FEATURES, NUMERIC_FEATURES  # noqa: E402

DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fraud_ds1.csv.gz"
OUTPUT_PATH = PROJECT_ROOT / "models" / "fraud_ds1_xgb_v1" / "drift_reference.json"


def main() -> None:
    raw = pd.read_csv(DATA_PATH)
    features = build_fraud_ds1_features(raw)
    train = features.iloc[: int(len(features) * 0.70)].copy()
    profile = build_reference_profile(train, NUMERIC_FEATURES, CATEGORICAL_FEATURES)
    profile.update(
        {
            "source": "fraud_ds1_training_window",
            "split": "oldest_70_percent",
            "feature_contract_version": "fraud_ds1_features_v1",
        }
    )
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    print(f"Wrote drift reference to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
