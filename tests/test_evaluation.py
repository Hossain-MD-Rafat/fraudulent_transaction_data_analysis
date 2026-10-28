import pandas as pd

from fraud_detection.evaluation import expanding_window_folds, summarize_fold_labels


def test_expanding_window_folds_preserve_time_order_and_holdout():
    folds = expanding_window_folds(
        n_rows=100,
        final_test_fraction=0.15,
        n_folds=4,
        min_train_fraction=0.40,
    )

    assert len(folds) == 4
    assert folds[0].train_end == 40
    assert folds[-1].validation_end == 85

    previous_validation_end = None
    for fold in folds:
        assert fold.train_start == 0
        assert fold.train_end == fold.validation_start
        assert fold.validation_start < fold.validation_end <= 85
        if previous_validation_end is not None:
            assert fold.validation_start == previous_validation_end
        previous_validation_end = fold.validation_end


def test_summarize_fold_labels_reports_positive_counts():
    df = pd.DataFrame({"is_fraud": [0] * 80 + [1] * 20})
    folds = expanding_window_folds(
        n_rows=100,
        final_test_fraction=0.10,
        n_folds=2,
        min_train_fraction=0.50,
    )

    summary = summarize_fold_labels(df, folds, "is_fraud")

    assert len(summary) == 2
    assert summary[0]["fold"] == 1
    assert summary[1]["validation_positive"] > 0
