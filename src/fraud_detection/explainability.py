"""Local model explanations for operational fraud scoring.

SHAP is imported lazily so the core scorer can still be imported in minimal
environments. Explanations are returned as ranked, signed contributions for the
model's transformed feature space.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from .inference import ModelBundle, feature_row


def _to_dense(matrix: Any) -> np.ndarray:
    return matrix.toarray() if hasattr(matrix, "toarray") else np.asarray(matrix)


def _positive_class_values(values: Any) -> np.ndarray:
    arr = np.asarray(values)
    if arr.ndim == 3:
        return arr[:, :, 1]
    if arr.ndim == 1:
        return arr.reshape(1, -1)
    return arr


def local_shap_explanation(
    bundle: ModelBundle,
    payload: dict[str, Any],
    top_k: int = 5,
) -> list[dict[str, float | str]]:
    """Return top signed SHAP contributions for one transaction.

    Positive SHAP values push the model toward fraud; negative values push it
    toward the legitimate class. This function expects the packaged model to be
    the sklearn-compatible pipeline produced by the training workflow.
    """
    try:
        import shap  # imported lazily because it is not needed for base scoring
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("SHAP is not installed in the scoring environment") from exc

    pipeline = bundle.model
    if not hasattr(pipeline, "named_steps"):
        raise TypeError("SHAP explanations require a pipeline with named_steps")

    preprocessor = pipeline.named_steps.get("preprocessor")
    estimator = pipeline.named_steps.get("model")
    if preprocessor is None or estimator is None:
        raise TypeError("Model pipeline must contain preprocessor and model steps")

    row = feature_row(payload)
    transformed = _to_dense(preprocessor.transform(row))
    feature_names = np.asarray(preprocessor.get_feature_names_out(), dtype=str)

    model_name = estimator.__class__.__name__
    if model_name in {"RandomForestClassifier", "XGBClassifier"}:
        explainer = shap.TreeExplainer(estimator)
        values = _positive_class_values(explainer.shap_values(transformed))
    elif model_name == "LogisticRegression":
        explainer = shap.LinearExplainer(estimator, transformed)
        values = _positive_class_values(explainer.shap_values(transformed))
    else:
        explainer = shap.Explainer(estimator.predict_proba, transformed)
        explanation = explainer(transformed)
        values = _positive_class_values(explanation.values)

    row_values = np.asarray(values[0], dtype=float)
    order = np.argsort(np.abs(row_values))[::-1][: max(int(top_k), 1)]

    return [
        {
            "feature": str(feature_names[index]),
            "shap_value": float(row_values[index]),
            "direction": "toward_fraud" if row_values[index] >= 0 else "toward_legitimate",
        }
        for index in order
    ]
