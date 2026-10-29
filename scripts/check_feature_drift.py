"""Evaluate engineered transaction features against the saved drift reference.

Usage:
    python scripts/check_feature_drift.py path/to/engineered_features.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_detection.drift import evaluate_drift  # noqa: E402
from fraud_detection.inference import FEATURES  # noqa: E402

DEFAULT_REFERENCE = PROJECT_ROOT / "models" / "fraud_ds1_xgb_v1" / "drift_reference.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "runtime" / "drift_report.json"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    frame = pd.read_csv(args.csv_path)
    missing = [feature for feature in FEATURES if feature not in frame.columns]
    if missing:
        raise ValueError(f"Input batch is missing inference features: {missing}")

    reference = json.loads(args.reference.read_text(encoding="utf-8"))
    report = evaluate_drift(frame[FEATURES], reference)
    report["feature_contract_version"] = reference.get("feature_contract_version")
    report["reference_source"] = reference.get("source")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
