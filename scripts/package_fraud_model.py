"""Package the selected Phase 1 model for the Phase 3 scoring service.

Run after:
    python scripts/benchmark_fraud_models.py

This copies the selected serialized pipeline into a versioned local model
folder and writes metadata used by the API. The binary model folder is ignored
by Git and is intended to be generated locally or in CI/CD.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil

from src.fraud_detection.inference import FEATURE_CONTRACT_VERSION, FEATURES

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE1_DIR = PROJECT_ROOT / "outputs" / "phase1"
SOURCE_MODEL = PHASE1_DIR / "best_fraud_model.joblib"
SOURCE_BENCHMARK = PHASE1_DIR / "model_benchmark.json"
MODEL_VERSION = "fraud_ds1_xgb_v1"
TARGET_DIR = PROJECT_ROOT / "models" / MODEL_VERSION


def main() -> None:
    if not SOURCE_MODEL.exists() or not SOURCE_BENCHMARK.exists():
        raise FileNotFoundError(
            "Run `python scripts/benchmark_fraud_models.py` before packaging the model."
        )

    with SOURCE_BENCHMARK.open("r", encoding="utf-8") as file:
        benchmark = json.load(file)

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SOURCE_MODEL, TARGET_DIR / "model.joblib")

    metadata = {
        "model_version": MODEL_VERSION,
        "model_type": benchmark["best_model"],
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "features": FEATURES,
        "review_threshold": float(benchmark["review_threshold"]),
        "block_threshold": None,
        "selection_metric": benchmark["selection_metric"],
        "split_strategy": benchmark["split_strategy"],
        "final_test": benchmark["final_test"],
        "training_source": "fraud_ds1",
        "artifact_source": "outputs/phase1/best_fraud_model.joblib",
    }

    with (TARGET_DIR / "metadata.json").open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    print(f"Packaged model: {TARGET_DIR}")


if __name__ == "__main__":
    main()
