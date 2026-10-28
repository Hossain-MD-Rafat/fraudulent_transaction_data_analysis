import numpy as np

from fraud_detection.risk import (
    choose_cost_sensitive_threshold,
    expected_fraud_loss,
    make_risk_decisions,
    threshold_cost,
)


def test_expected_fraud_loss_is_probability_times_amount():
    result = expected_fraud_loss(np.array([0.0, 0.25, 1.0]), np.array([100.0, 200.0, 50.0]))
    np.testing.assert_allclose(result, np.array([0.0, 50.0, 50.0]))


def test_false_negative_amount_drives_threshold_cost():
    y = np.array([0, 1])
    probabilities = np.array([0.20, 0.40])
    amounts = np.array([20.0, 1000.0])

    high_threshold_cost = threshold_cost(y, probabilities, amounts, threshold=0.50)
    low_threshold_cost = threshold_cost(y, probabilities, amounts, threshold=0.30)

    assert high_threshold_cost == 1000.0
    assert low_threshold_cost == 0.0


def test_cost_sensitive_threshold_can_prioritize_large_fraud():
    y = np.array([0, 0, 1])
    probabilities = np.array([0.10, 0.20, 0.30])
    amounts = np.array([25.0, 25.0, 5000.0])

    threshold, cost = choose_cost_sensitive_threshold(
        y,
        probabilities,
        amounts,
        false_positive_review_cost=5.0,
    )

    assert threshold <= 0.30
    assert cost <= 10.0


def test_risk_decisions_create_score_exposure_and_actions():
    output = make_risk_decisions(
        probabilities=np.array([0.05, 0.55, 0.95]),
        amounts=np.array([100.0, 200.0, 1000.0]),
        review_threshold=0.50,
        block_threshold=0.90,
    )

    assert output["risk_score"].tolist() == [5, 55, 95]
    assert output["decision"].tolist() == ["APPROVE", "REVIEW", "BLOCK"]
    np.testing.assert_allclose(output["expected_fraud_loss"], [5.0, 110.0, 950.0])
