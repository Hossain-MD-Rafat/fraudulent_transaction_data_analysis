"""End-to-end smoke test for the real packaged Phase 3 fraud model.

Expected sequence:
    python scripts/benchmark_fraud_models.py
    python scripts/package_fraud_model.py
    python scripts/smoke_test_packaged_model.py

The script loads the packaged artifact, rebuilds leakage-aware features from the
committed fraud dataset, scores one chronologically recent transaction, and
verifies the probability/metadata contract.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.features import build_fraud_ds1_features  # noqa: E402
from fraud_detection.inference import (  # noqa: E402
    FEATURES,
    FEATURE_CONTRACT_VERSION,
    load_model_bundle,
    predict_probability,
)

DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fraud_ds1.csv.gz"
MODEL_DIR = PROJECT_ROOT / "models" / "fraud_ds1_xgb_v1"


def main() -> None:
    raw = pd.read_csv(DATA_PATH)
    feature_df = build_fraud_ds1_features(raw)
    sample = feature_df.iloc[-1]

    bundle = load_model_bundle(
        model_path=MODEL_DIR / "model.joblib",
        metadata_path=MODEL_DIR / "metadata.json",
    )
    payload = {name: sample[name] for name in FEATURES}
    probability = predict_probability(bundle, payload)

    assert bundle.feature_contract_version == FEATURE_CONTRACT_VERSION
    assert 0.0 <= probability <= 1.0
    assert bundle.metadata.get("artifact_sha256")

    print(
        json.dumps(
            {
                "status": "ok",
                "model_version": bundle.model_version,
                "feature_contract_version": bundle.feature_contract_version,
                "fraud_probability": probability,
                "transaction_reference": str(sample.get("trans_num", "latest_row")),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
