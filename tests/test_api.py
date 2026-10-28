from pathlib import Path
import sys

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.main import app


client = TestClient(app)


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
