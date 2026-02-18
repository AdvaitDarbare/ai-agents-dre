from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

import numpy as np

from src.utils.database import get_connection


@dataclass
class BacktestReport:
    dataset_name: str
    metric_name: str
    samples: int
    true_anomalies: int
    predicted_anomalies: int
    true_positive: int
    false_positive: int
    false_negative: int
    precision: float
    recall: float
    false_positive_rate: float
    false_negative_rate: float

    def to_dict(self) -> Dict[str, object]:
        return {
            "dataset_name": self.dataset_name,
            "metric_name": self.metric_name,
            "samples": self.samples,
            "true_anomalies": self.true_anomalies,
            "predicted_anomalies": self.predicted_anomalies,
            "confusion": {
                "tp": self.true_positive,
                "fp": self.false_positive,
                "fn": self.false_negative,
            },
            "precision": self.precision,
            "recall": self.recall,
            "false_positive_rate": self.false_positive_rate,
            "false_negative_rate": self.false_negative_rate,
        }


class MonitorBacktestingHarness:
    """
    Backtesting harness for anomaly detector tuning.

    Labels are generated from robust outlier boundaries over full history
    and compared against online z-score predictions (rolling window).
    """

    def _load_metric_history(self, dataset_name: str, metric_name: str, limit: int = 500) -> List[float]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT metric_value
                    FROM metric_history
                    WHERE dataset_name = %s
                      AND metric_name = %s
                    ORDER BY timestamp ASC
                    LIMIT %s
                    """,
                    (dataset_name, metric_name, limit),
                )
                return [float(row[0]) for row in cur.fetchall() if row[0] is not None]

    @staticmethod
    def _robust_labels(values: Sequence[float]) -> List[int]:
        if len(values) < 8:
            return [0 for _ in values]
        arr = np.asarray(values, dtype=float)
        median = float(np.median(arr))
        mad = float(np.median(np.abs(arr - median)))
        if mad == 0:
            return [0 for _ in values]
        labels: List[int] = []
        for value in arr:
            robust_z = abs((value - median) / (1.4826 * mad))
            labels.append(1 if robust_z >= 3.5 else 0)
        return labels

    @staticmethod
    def _rolling_z_predictions(values: Sequence[float], warmup: int = 12, z_threshold: float = 3.0) -> List[int]:
        preds: List[int] = []
        for idx, current in enumerate(values):
            if idx < warmup:
                preds.append(0)
                continue
            window = np.asarray(values[max(0, idx - 90):idx], dtype=float)
            if window.size < 3:
                preds.append(0)
                continue
            mean = float(np.mean(window))
            std = float(np.std(window, ddof=1)) if window.size > 1 else 0.0
            if std == 0:
                preds.append(0)
                continue
            z = abs((float(current) - mean) / std)
            preds.append(1 if z >= z_threshold else 0)
        return preds

    def run(self, dataset_name: str, metric_name: str = "row_count", limit: int = 500) -> Dict[str, object]:
        values = self._load_metric_history(dataset_name, metric_name, limit=limit)
        if len(values) < 12:
            return {
                "dataset_name": dataset_name,
                "metric_name": metric_name,
                "samples": len(values),
                "status": "insufficient_history",
                "message": "Need at least 12 metric points for backtesting.",
            }

        labels = self._robust_labels(values)
        preds = self._rolling_z_predictions(values)

        tp = fp = fn = tn = 0
        for truth, pred in zip(labels, preds):
            if truth == 1 and pred == 1:
                tp += 1
            elif truth == 0 and pred == 1:
                fp += 1
            elif truth == 1 and pred == 0:
                fn += 1
            else:
                tn += 1

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        fpr = fp / (fp + tn) if (fp + tn) else 0.0
        fnr = fn / (fn + tp) if (fn + tp) else 0.0

        report = BacktestReport(
            dataset_name=dataset_name,
            metric_name=metric_name,
            samples=len(values),
            true_anomalies=sum(labels),
            predicted_anomalies=sum(preds),
            true_positive=tp,
            false_positive=fp,
            false_negative=fn,
            precision=round(precision, 4),
            recall=round(recall, 4),
            false_positive_rate=round(fpr, 4),
            false_negative_rate=round(fnr, 4),
        )
        return report.to_dict()
