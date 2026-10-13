"""
Evaluation module for the DSS150P reproduction pipeline.

Responsibilities:
- Load the trained model artifact.
- Rebuild the exact test split using the same seed and strategy.
- Predict on the test set.
- Compute accuracy, precision, recall, F1, ROC-AUC.
- Write outputs/metrics.json (overwriting).
- Write outputs/predictions.csv with collision_index + y_true + y_pred + y_proba.
- Compare computed metrics against metadata/expected_metrics.json within
  the declared tolerance. If the baseline is null, populate it instead.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

from src.config import Settings, configure_logging, get_settings
from src.train import select_features

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Metric container
# ----------------------------------------------------------------------
@dataclass
class Metrics:
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray) -> Metrics:
    return Metrics(
        accuracy=float(accuracy_score(y_true, y_pred)),
        precision=float(precision_score(y_true, y_pred, zero_division=0)),
        recall=float(recall_score(y_true, y_pred, zero_division=0)),
        f1=float(f1_score(y_true, y_pred, zero_division=0)),
        roc_auc=float(roc_auc_score(y_true, y_proba)),
    )


# ----------------------------------------------------------------------
# Baseline comparison
# ----------------------------------------------------------------------
def compare_to_baseline(settings: Settings, metrics: Metrics) -> dict[str, Any]:
    """
    Read metadata/expected_metrics.json.
    - If metrics are null: populate and freeze the baseline.
    - If metrics are present: verify each within tolerance.
    """
    path = settings.metadata_dir / "expected_metrics.json"
    baseline = json.loads(path.read_text(encoding="utf-8"))

    metrics_dict = metrics.to_dict()
    comparison: dict[str, Any] = {"per_metric": {}, "status": "ok"}

    all_null = all(baseline["metrics"].get(k) is None for k in metrics_dict)

    if all_null:
        baseline["metrics"] = metrics_dict
        baseline["_comment"] = (
            "Baseline frozen on first authoritative run. "
            "Subsequent runs must reproduce each metric within tolerance."
        )
        path.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
        comparison["status"] = "baseline_frozen"
        log.info("Baseline metrics frozen in %s", path)
        return comparison

    for name, value in metrics_dict.items():
        expected = baseline["metrics"].get(name)
        tol = baseline["tolerance"].get(name)
        if expected is None or tol is None:
            comparison["per_metric"][name] = {"value": value, "status": "no_baseline"}
            continue
        delta = abs(value - expected)
        ok = delta <= tol
        comparison["per_metric"][name] = {
            "value": value,
            "expected": expected,
            "delta": delta,
            "tolerance": tol,
            "status": "pass" if ok else "fail",
        }
        if not ok:
            comparison["status"] = "fail"

    return comparison


# ----------------------------------------------------------------------
# Main evaluation
# ----------------------------------------------------------------------
def evaluate(settings: Settings | None = None) -> dict[str, Any]:
    s = settings or get_settings()
    s.outputs_dir.mkdir(parents=True, exist_ok=True)

    curated = pd.read_parquet(s.curated_dir / "curated.parquet")
    features = select_features(curated)
    X = curated[features].copy()
    y = curated["is_fatal"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=s.test_size,
        random_state=s.random_seed,
        stratify=y,
        shuffle=True,
    )

    bundle = joblib.load(s.model_dir / "model.joblib")
    model = bundle["model"]
    log.info("Loaded model artifact: %s", s.model_dir / "model.joblib")

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = compute_metrics(y_test.to_numpy(), y_pred, y_proba)
    log.info("Metrics: %s", metrics.to_dict())

    metrics_path = s.metrics_file
    metrics_path.write_text(
        json.dumps(
            {
                "dataset": "uk_dft_stats19_collision",
                "dataset_checksum_sha256": s.source_checksum,
                "model": {
                    "algorithm": s.algorithm,
                    "random_seed": s.random_seed,
                    "test_size": s.test_size,
                    "hyperparameters": s.hyperparameters,
                },
                "metrics": metrics.to_dict(),
                "row_counts": {
                    "total": len(curated),
                    "train": len(X_train),
                    "test": len(X_test),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("Metrics written: %s", metrics_path)

    predictions = pd.DataFrame(
        {
            "collision_index": curated.loc[X_test.index, "collision_index"].values,
            "y_true": y_test.to_numpy(),
            "y_pred": y_pred,
            "y_proba": y_proba,
        }
    )
    pred_path = s.outputs_dir / "predictions.csv"
    predictions.to_csv(pred_path, index=False)
    log.info("Predictions written: %s (%d rows)", pred_path, len(predictions))

    comparison = compare_to_baseline(s, metrics)
    log.info("Baseline comparison: %s", comparison["status"])

    return {
        "metrics": metrics.to_dict(),
        "comparison": comparison,
        "predictions_rows": len(predictions),
    }


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(description="DSS150P evaluate")
    parser.parse_args()

    s = get_settings()
    configure_logging(s)

    result = evaluate(s)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    _cli()