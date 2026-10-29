from fraud_detection.graph_risk import (
    apply_graph_escalation_policy,
    score_graph_signals,
    unified_risk_score,
)


def test_graph_risk_score_increases_with_suspicious_signals():
    low = score_graph_signals(
        {
            "laundering_neighbors": 0,
            "sent_24h": 1,
            "received_24h": 1,
            "counterparties_24h": 1,
            "rapid_pass_throughs": 0,
            "cycles_3hop": 0,
        }
    )
    high = score_graph_signals(
        {
            "laundering_neighbors": 3,
            "sent_24h": 12,
            "received_24h": 10,
            "counterparties_24h": 15,
            "rapid_pass_throughs": 3,
            "cycles_3hop": 2,
        }
    )
    assert 0 <= low <= 100
    assert 0 <= high <= 100
    assert high > low


def test_unified_risk_uses_model_only_without_graph():
    assert unified_risk_score(73, None) == 73


def test_unified_risk_blends_model_and_graph():
    assert unified_risk_score(80, 40) == 72


def test_graph_policy_escalates_approve_to_review():
    decision, reason = apply_graph_escalation_policy(
        "APPROVE", 70, review_threshold=60, block_threshold=85
    )
    assert decision == "REVIEW"
    assert reason == "GRAPH_ESCALATION_REVIEW"


def test_graph_policy_can_escalate_to_block():
    decision, reason = apply_graph_escalation_policy(
        "REVIEW", 90, review_threshold=60, block_threshold=85
    )
    assert decision == "BLOCK"
    assert reason == "GRAPH_ESCALATION_BLOCK"


def test_graph_policy_never_deescalates_model_decision():
    decision, reason = apply_graph_escalation_policy(
        "BLOCK", 10, review_threshold=60, block_threshold=85
    )
    assert decision == "BLOCK"
    assert reason is None
