"""Time-aware evaluation utilities for rare-event fraud models."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TemporalFold:
    """Row boundaries for one expanding-window temporal validation fold."""

    train_start: int
    train_end: int
    validation_start: int
    validation_end: int


def expanding_window_folds(
    n_rows: int,
    final_test_fraction: float = 0.15,
    n_folds: int = 4,
    min_train_fraction: float = 0.40,
) -> list[TemporalFold]:
    """Create expanding-train / forward-validation folds before a final holdout.

    The newest ``final_test_fraction`` of rows is never included in a fold.
    Validation windows partition the remaining history after the initial
    training block. This keeps every validation observation strictly later
    than its training observations.
    """
    if n_rows < 10:
        raise ValueError("At least 10 rows are required for temporal folds.")
    if not 0 < final_test_fraction < 0.5:
        raise ValueError("final_test_fraction must be between 0 and 0.5.")
    if n_folds < 2:
        raise ValueError("n_folds must be at least 2.")
    if not 0.2 <= min_train_fraction < (1.0 - final_test_fraction):
        raise ValueError("min_train_fraction leaves insufficient validation history.")

    development_end = int(n_rows * (1.0 - final_test_fraction))
    initial_train_end = int(n_rows * min_train_fraction)
    remaining = development_end - initial_train_end
    if remaining < n_folds:
        raise ValueError("Not enough rows to create the requested number of folds.")

    boundaries = np.linspace(initial_train_end, development_end, n_folds + 1, dtype=int)
    folds: list[TemporalFold] = []
    for idx in range(n_folds):
        validation_start = int(boundaries[idx])
        validation_end = int(boundaries[idx + 1])
        folds.append(
            TemporalFold(
                train_start=0,
                train_end=validation_start,
                validation_start=validation_start,
                validation_end=validation_end,
            )
        )
    return folds


def summarize_fold_labels(
    df: pd.DataFrame,
    folds: list[TemporalFold],
    target: str,
) -> list[dict[str, int | float]]:
    """Return class counts/rates so rare-event fold quality is transparent."""
    summary: list[dict[str, int | float]] = []
    for fold_idx, fold in enumerate(folds, start=1):
        train_y = df.iloc[fold.train_start : fold.train_end][target]
        validation_y = df.iloc[fold.validation_start : fold.validation_end][target]
        summary.append(
            {
                "fold": fold_idx,
                "train_rows": int(len(train_y)),
                "train_positive": int(train_y.sum()),
                "train_rate": float(train_y.mean()),
                "validation_rows": int(len(validation_y)),
                "validation_positive": int(validation_y.sum()),
                "validation_rate": float(validation_y.mean()),
            }
        )
    return summary
