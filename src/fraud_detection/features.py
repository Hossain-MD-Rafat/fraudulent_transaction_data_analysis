"""Feature engineering utilities for transaction fraud detection.

The functions in this module are intentionally leakage-aware: behavioral
features are computed from *prior* transactions only for each card/customer.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    """Vectorized Haversine distance in kilometers."""
    lat1_r = np.radians(lat1.astype(float))
    lon1_r = np.radians(lon1.astype(float))
    lat2_r = np.radians(lat2.astype(float))
    lon2_r = np.radians(lon2.astype(float))

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def build_fraud_ds1_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create temporal, geographic and prior-behavior features for fraud_ds1.

    Notes
    -----
    - Rows are sorted chronologically before rolling/expanding features.
    - Current transaction values are excluded from historical aggregates by
      applying ``shift(1)`` before rolling statistics.
    - The original PII-like text columns are not required by the model.
    """
    data = df.copy()

    data["trans_date_trans_time"] = pd.to_datetime(data["trans_date_trans_time"], errors="coerce")
    data["dob"] = pd.to_datetime(data["dob"], errors="coerce")
    data = data.sort_values("trans_date_trans_time").reset_index(drop=True)

    ts = data["trans_date_trans_time"]
    data["transaction_hour"] = ts.dt.hour
    data["transaction_dayofweek"] = ts.dt.dayofweek
    data["transaction_month"] = ts.dt.month
    data["is_weekend"] = (data["transaction_dayofweek"] >= 5).astype(int)
    data["is_night"] = data["transaction_hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    data["customer_age"] = ((ts.dt.normalize() - data["dob"]).dt.days / 365.25).clip(lower=0)
    data["merchant_distance_km"] = haversine_km(
        data["lat"], data["long"], data["merch_lat"], data["merch_long"]
    )

    card_group = data.groupby("cc_num", sort=False)

    # Prior transaction amount statistics only.
    data["prior_txn_count"] = card_group.cumcount()
    data["prior_avg_amount"] = card_group["amt"].transform(
        lambda s: s.shift(1).expanding(min_periods=1).mean()
    )
    data["prior_std_amount"] = card_group["amt"].transform(
        lambda s: s.shift(1).expanding(min_periods=2).std()
    )
    data["amount_to_prior_mean"] = data["amt"] / data["prior_avg_amount"].replace(0, np.nan)
    data["amount_zscore_prior"] = (
        (data["amt"] - data["prior_avg_amount"]) / data["prior_std_amount"].replace(0, np.nan)
    )

    # Time since the previous card transaction.
    data["minutes_since_prev_txn"] = card_group["trans_date_trans_time"].diff().dt.total_seconds() / 60.0

    # Velocity features use strictly earlier transactions.
    data["txns_prev_1h"] = 0.0
    data["txns_prev_24h"] = 0.0
    for _, idx in data.groupby("cc_num", sort=False).groups.items():
        ordered_idx = list(idx)
        times = data.loc[ordered_idx, "trans_date_trans_time"]
        marker = pd.Series(1.0, index=times)
        prev_1h = marker.rolling("1h", closed="left").sum().fillna(0.0).to_numpy()
        prev_24h = marker.rolling("24h", closed="left").sum().fillna(0.0).to_numpy()
        data.loc[ordered_idx, "txns_prev_1h"] = prev_1h
        data.loc[ordered_idx, "txns_prev_24h"] = prev_24h

    # First-time merchant/category signals relative to the card's prior history.
    data["prior_same_merchant_count"] = data.groupby(["cc_num", "merchant"]).cumcount()
    data["prior_same_category_count"] = data.groupby(["cc_num", "category"]).cumcount()
    data["is_new_merchant_for_card"] = (data["prior_same_merchant_count"] == 0).astype(int)
    data["is_new_category_for_card"] = (data["prior_same_category_count"] == 0).astype(int)

    return data
