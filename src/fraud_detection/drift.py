"""Distribution-based feature drift monitoring.

Reference profiles contain aggregate distributions only; raw training rows do
not need to be persisted with the deployed service.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def numeric_reference(series: pd.Series, bins: int = 10) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=float)
    if values.size == 0:
        return {"edges": [], "proportions": [], "missing_rate": 1.0}

    quantiles = np.linspace(0.0, 1.0, bins + 1)
    edges = np.unique(np.quantile(values, quantiles))
    if edges.size < 2:
        center = float(edges[0])
        edges = np.array([center - 1e-9, center + 1e-9])
    edges[0] = -np.inf
    edges[-1] = np.inf
    counts, _ = np.histogram(values, bins=edges)
    proportions = counts / max(counts.sum(), 1)
    return {
        "edges": [float(x) if np.isfinite(x) else ("-inf" if x < 0 else "inf") for x in edges],
        "proportions": proportions.astype(float).tolist(),
        "missing_rate": float(series.isna().mean()),
    }


def categorical_reference(series: pd.Series, top_k: int = 25) -> dict[str, Any]:
    normalized = series.fillna("__MISSING__").astype(str)
    counts = normalized.value_counts(normalize=True)
    top = counts.head(top_k)
    other = max(0.0, 1.0 - float(top.sum()))
    proportions = {str(k): float(v) for k, v in top.items()}
    if other > 0:
        proportions["__OTHER__"] = other
    return {"proportions": proportions}


def build_reference_profile(
    frame: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
) -> dict[str, Any]:
    return {
        "row_count": int(len(frame)),
        "numeric": {
            feature: numeric_reference(frame[feature]) for feature in numeric_features
        },
        "categorical": {
            feature: categorical_reference(frame[feature]) for feature in categorical_features
        },
    }


def _decode_edges(values: list[float | str]) -> np.ndarray:
    decoded = []
    for value in values:
        if value == "-inf":
            decoded.append(-np.inf)
        elif value == "inf":
            decoded.append(np.inf)
        else:
            decoded.append(float(value))
    return np.asarray(decoded, dtype=float)


def population_stability_index(
    series: pd.Series,
    reference: dict[str, Any],
    epsilon: float = 1e-6,
) -> float:
    edges = _decode_edges(reference.get("edges", []))
    expected = np.asarray(reference.get("proportions", []), dtype=float)
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=float)
    if edges.size < 2 or expected.size == 0:
        return 0.0
    counts, _ = np.histogram(values, bins=edges)
    actual = counts / max(counts.sum(), 1)
    expected = np.clip(expected, epsilon, None)
    actual = np.clip(actual, epsilon, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def categorical_total_variation(
    series: pd.Series,
    reference: dict[str, Any],
) -> float:
    expected = dict(reference.get("proportions", {}))
    normalized = series.fillna("__MISSING__").astype(str)
    actual_counts = normalized.value_counts(normalize=True).to_dict()

    known = set(expected).difference({"__OTHER__"})
    actual: dict[str, float] = {key: float(actual_counts.get(key, 0.0)) for key in known}
    actual_other = sum(float(value) for key, value in actual_counts.items() if key not in known)
    if "__OTHER__" in expected or actual_other > 0:
        actual["__OTHER__"] = actual_other

    keys = set(expected) | set(actual)
    distance = 0.5 * sum(abs(float(actual.get(k, 0.0)) - float(expected.get(k, 0.0))) for k in keys)
    return float(distance)


def evaluate_drift(frame: pd.DataFrame, reference: dict[str, Any]) -> dict[str, Any]:
    numeric_report: dict[str, Any] = {}
    for feature, profile in reference.get("numeric", {}).items():
        psi = population_stability_index(frame[feature], profile)
        numeric_report[feature] = {
            "psi": psi,
            "status": "high" if psi >= 0.25 else ("moderate" if psi >= 0.10 else "low"),
        }

    categorical_report: dict[str, Any] = {}
    for feature, profile in reference.get("categorical", {}).items():
        tvd = categorical_total_variation(frame[feature], profile)
        categorical_report[feature] = {
            "total_variation": tvd,
            "status": "high" if tvd >= 0.30 else ("moderate" if tvd >= 0.15 else "low"),
        }

    high_features = [
        feature for feature, item in {**numeric_report, **categorical_report}.items()
        if item["status"] == "high"
    ]
    return {
        "rows_evaluated": int(len(frame)),
        "numeric": numeric_report,
        "categorical": categorical_report,
        "high_drift_features": high_features,
        "high_drift_feature_count": len(high_features),
    }
