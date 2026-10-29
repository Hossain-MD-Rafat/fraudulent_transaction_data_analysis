from scripts.summarize_scoring_kpis import summarize


def test_scoring_kpi_summary():
    events = [
        {
            "decision": "APPROVE",
            "fraud_probability": 0.10,
            "unified_risk_score": 10,
            "expected_fraud_loss": 5.0,
            "graph_available": False,
            "model_version": "m1",
        },
        {
            "decision": "REVIEW",
            "fraud_probability": 0.60,
            "unified_risk_score": 65,
            "expected_fraud_loss": 120.0,
            "graph_available": True,
            "model_version": "m1",
        },
        {
            "decision": "BLOCK",
            "fraud_probability": 0.95,
            "unified_risk_score": 90,
            "expected_fraud_loss": 950.0,
            "graph_available": True,
            "model_version": "m2",
        },
    ]

    summary = summarize(events)
    assert summary["event_count"] == 3
    assert summary["decision_counts"] == {"APPROVE": 1, "REVIEW": 1, "BLOCK": 1}
    assert summary["graph_enrichment_rate"] == 2 / 3
    assert summary["total_expected_fraud_loss"] == 1075.0
    assert summary["models"] == {"m1": 2, "m2": 1}
