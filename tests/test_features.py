import numpy as np
import pandas as pd

from src.fraud_detection.features import build_fraud_ds1_features, haversine_km


def test_haversine_zero_distance():
    result = haversine_km(
        pd.Series([40.0]),
        pd.Series([-73.0]),
        pd.Series([40.0]),
        pd.Series([-73.0]),
    )
    assert np.isclose(result.iloc[0], 0.0)


def test_prior_features_exclude_current_transaction():
    df = pd.DataFrame(
        {
            "trans_date_trans_time": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"],
            "dob": ["1990-01-01", "1990-01-01"],
            "cc_num": [1, 1],
            "amt": [10.0, 100.0],
            "lat": [40.0, 40.0],
            "long": [-73.0, -73.0],
            "merch_lat": [40.0, 40.1],
            "merch_long": [-73.0, -73.1],
            "merchant": ["A", "B"],
            "category": ["grocery", "travel"],
        }
    )

    result = build_fraud_ds1_features(df)

    assert np.isnan(result.loc[0, "prior_avg_amount"])
    assert result.loc[1, "prior_avg_amount"] == 10.0
    assert result.loc[1, "amount_to_prior_mean"] == 10.0
    assert result.loc[1, "prior_txn_count"] == 1
