import json
from pathlib import Path
import sys

import joblib
import numpy as np
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api.main as api_main
from fraud_detection.inference import FEATURE_CONTRACT_VERSION, FEATURES


client = TestClient(api_main.app)


class FakeProbabilityModel:
    """Small serializable model double for API contract tests."""

    def predict_proba(self, frame):
        return np.tile(np.array([[0.08, 0.92]], dtype=float), (len(frame), 1))


def _score_features() -> dict:
    return {
        "amt": 1200.0,
        "city_pop": 150000.0,
        "customer_age": 38.0,
        "merchant_distance_km": 850.0,
        "transaction_hour": 2,
        "transaction_dayofweek": 5,
        "transaction_month": 10,
        "is_weekend": 1,
        "is_night": 1,
        "prior_txn_count": 12.0,
        "prior_avg_amount": 85.0,
        "prior_std_amount": 30.0,
        "amount_to_prior_mean": 14.1,
        "amount_zscore_prior": 4.2,
        "minutes_since_prev_txn": 7.0,
        "txns_prev_1h": 4.0,
        "txns_prev_24h": 8.0,
        "prior_same_merchant_count": 0.0,
        "prior_same_category_count": 1.0,
        "is_new_merchant_for_card": 1,
        "is_new_category_for_card": 1,
        "category": "shopping_net",
        "gender": "M",
        "state": "NY",
    }


def _configure_fake_artifact(tmp_path, monkeypatch) -> None:
    model_path = tmp_path / "model.joblib"
    metadata_path = tmp_path / "metadata.json"
    joblib.dump(FakeProbabilityModel(), model_path)
    metadata = {
        "model_version": "test_model_v1",
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "features": FEATURES,
        "review_threshold": 0.50,
        "block_threshold": 0.90,
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setenv("FRAUD_MODEL_PATH", str(model_path))
    monkeypatch.setenv("FRAUD_MODEL_METADATA_PATH", str(metadata_path))
    api_main.get_model_bundle.cache_clear()


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "fraud-intelligence-decision-api"


def test_review_decision_contains_expected_exposure():
    response = client.post(
        "/v1/decision",
        json={
            "transaction_id": "txn-001",
            "amount": 1000.0,
            "fraud_probability": 0.60,
            "review_threshold": 0.50,
            "block_threshold": 0.90,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["decision"] == "REVIEW"
    assert payload["risk_score"] == 60
    assert payload["expected_fraud_loss"] == 600.0


def test_block_decision():
    response = client.post(
        "/v1/decision",
        json={
            "transaction_id": "txn-002",
            "amount": 2500.0,
            "fraud_probability": 0.96,
            "review_threshold": 0.50,
            "block_threshold": 0.90,
        },
    )
    assert response.status_code == 200
    assert response.json()["decision"] == "BLOCK"


def test_invalid_threshold_order_is_rejected():
    response = client.post(
        "/v1/decision",
        json={
            "transaction_id": "txn-003",
            "amount": 100.0,
            "fraud_probability": 0.30,
            "review_threshold": 0.70,
            "block_threshold": 0.60,
        },
    )
    assert response.status_code == 422


def test_score_endpoint_loads_versioned_artifact(tmp_path, monkeypatch):
    _configure_fake_artifact(tmp_path, monkeypatch)

    response = client.post(
        "/v1/score",
        json={"transaction_id": "txn-score-001", "features": _score_features()},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["model_version"] == "test_model_v1"
    assert payload["feature_contract_version"] == FEATURE_CONTRACT_VERSION
    assert payload["fraud_probability"] == 0.92
    assert payload["risk_score"] == 92
    assert payload["decision"] == "BLOCK"
    assert payload["expected_fraud_loss"] == 1104.0
    assert "AMOUNT_HIGH_VS_CUSTOMER_HISTORY" in payload["reason_codes"]
    assert "MERCHANT_FAR_FROM_CUSTOMER" in payload["reason_codes"]
    assert payload["explanation_method"] == "heuristic_input_signals_v1"


def test_score_endpoint_returns_503_when_artifact_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("FRAUD_MODEL_PATH", str(tmp_path / "missing.joblib"))
    monkeypatch.setenv("FRAUD_MODEL_METADATA_PATH", str(tmp_path / "missing.json"))
    api_main.get_model_bundle.cache_clear()

    response = client.post(
        "/v1/score",
        json={"transaction_id": "txn-score-002", "features": _score_features()},
    )
    assert response.status_code == 503
    assert "Model artifact unavailable" in response.json()["detail"]
