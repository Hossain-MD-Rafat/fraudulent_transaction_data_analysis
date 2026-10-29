import pandas as pd

from fraud_detection.drift import build_reference_profile, evaluate_drift


def test_drift_report_detects_shifted_numeric_distribution():
    reference_frame = pd.DataFrame(
        {
            "amount": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19] * 20,
            "state": ["NY", "NY", "NJ", "NY", "NJ"] * 40,
        }
    )
    profile = build_reference_profile(reference_frame, ["amount"], ["state"])

    similar = pd.DataFrame(
        {
            "amount": [11, 12, 13, 14, 15, 16, 17, 18] * 10,
            "state": ["NY", "NJ"] * 40,
        }
    )
    shifted = pd.DataFrame(
        {
            "amount": [100, 120, 140, 160, 180] * 16,
            "state": ["CA"] * 80,
        }
    )

    similar_report = evaluate_drift(similar, profile)
    shifted_report = evaluate_drift(shifted, profile)

    assert shifted_report["numeric"]["amount"]["psi"] > similar_report["numeric"]["amount"]["psi"]
    assert shifted_report["categorical"]["state"]["total_variation"] > similar_report["categorical"]["state"]["total_variation"]
    assert shifted_report["high_drift_feature_count"] >= 1
