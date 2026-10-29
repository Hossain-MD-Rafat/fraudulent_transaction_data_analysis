import json

from fraud_detection.monitoring import hash_identifier, log_score_event


def test_hash_identifier_is_stable_and_not_plaintext():
    value = "txn-sensitive-123"
    hashed = hash_identifier(value)
    assert hashed == hash_identifier(value)
    assert value not in hashed
    assert len(hashed) == 24


def test_log_score_event_writes_redacted_jsonl(tmp_path):
    path = tmp_path / "scores.jsonl"
    wrote = log_score_event(
        transaction_id="txn-sensitive-123",
        model_version="model-v1",
        feature_contract_version="features-v1",
        fraud_probability=0.82,
        model_risk_score=82,
        graph_risk_score=60,
        unified_risk_score=78,
        decision="REVIEW",
        expected_fraud_loss=410.0,
        graph_available=True,
        explanation_method="shap_local_v1",
        path=path,
    )
    assert wrote is True
    event = json.loads(path.read_text(encoding="utf-8").strip())
    assert event["transaction_hash"] == hash_identifier("txn-sensitive-123")
    assert "txn-sensitive-123" not in path.read_text(encoding="utf-8")
    assert event["model_version"] == "model-v1"
    assert event["unified_risk_score"] == 78
