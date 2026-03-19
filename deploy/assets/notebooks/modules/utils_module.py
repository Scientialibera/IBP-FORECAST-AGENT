# Fabric Notebook -- Module
# utils_module.py -- Metrics, time splits, MLflow helpers

import time
import re
import numpy as np
import pandas as pd
import uuid
from datetime import datetime, timezone


def compute_metrics(y_true, y_pred) -> dict:
    """Compute RMSE, MAE, MAPE, R2, and bias."""
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true, y_pred = y_true[mask], y_pred[mask]
    if len(y_true) == 0:
        return {"rmse": None, "mae": None, "mape": None, "r2": None, "bias": None}

    residuals = y_pred - y_true
    rmse = float(np.sqrt(np.mean(residuals ** 2)))
    mae = float(np.mean(np.abs(residuals)))
    bias = float(np.mean(residuals))

    nonzero = y_true != 0
    mape = float(np.mean(np.abs(residuals[nonzero] / y_true[nonzero])) * 100) if nonzero.any() else None

    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else None

    return {"rmse": rmse, "mae": mae, "mape": mape, "r2": r2, "bias": bias}


def time_split(df: pd.DataFrame, date_column: str, test_ratio: float = 0.2):
    """Time-based train/test split."""
    df = df.sort_values(date_column).reset_index(drop=True)
    split_idx = int(len(df) * (1 - test_ratio))
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def generate_version_id() -> str:
    """Generate a unique version ID for a forecast run."""
    return str(uuid.uuid4())


def current_snapshot_month() -> str:
    """Return the current YYYY-MM for snapshot tagging."""
    return datetime.utcnow().strftime("%Y-%m")


def _parse_blocked_until(error_msg: str) -> float:
    """Extract seconds to wait from Fabric 429 'blocked until' message."""
    match = re.search(r"until:\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*(AM|PM)?)\s*\(UTC\)", str(error_msg))
    if match:
        try:
            ts_str = match.group(1).strip()
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"):
                try:
                    blocked_until = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                    wait = (blocked_until - datetime.now(timezone.utc)).total_seconds()
                    return max(wait + 2, 0)
                except ValueError:
                    continue
        except Exception:
            pass
    return 0


def _mlflow_with_retry(fn, max_attempts: int = 3, base_wait: float = 5):
    """Call *fn* with retry on Fabric 429 rate limits."""
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            err_str = str(e)
            if "429" not in err_str and "RequestBlocked" not in err_str:
                raise
            wait = _parse_blocked_until(err_str)
            if wait <= 0:
                wait = base_wait * (2 ** attempt)
            print(f"[mlflow] Rate-limited (429), waiting {wait:.0f}s before retry {attempt + 1}/{max_attempts}...")
            time.sleep(wait)
    return fn()


def log_metrics_to_mlflow(metrics: dict, prefix: str = "") -> None:
    """Log metrics dict to active MLflow run (rate-limit aware)."""
    try:
        import mlflow
        batch = {(f"{prefix}_{k}" if prefix else k): v for k, v in metrics.items() if v is not None}
        def _log():
            for name, val in batch.items():
                mlflow.log_metric(name, val)
        _mlflow_with_retry(_log)
    except Exception as e:
        print(f"[mlflow] Warning: could not log metrics: {e}")


def ensure_experiment(experiment_name: str) -> None:
    """Attach to existing MLflow experiment by ID (not name) to avoid root-level duplicates."""
    try:
        import mlflow
        mlflow.autolog(disable=True)
        def _attach():
            exp = mlflow.get_experiment_by_name(experiment_name)
            if exp:
                mlflow.set_experiment(experiment_id=exp.experiment_id)
        _mlflow_with_retry(_attach)
    except Exception as e:
        print(f"[mlflow] Warning: could not set experiment '{experiment_name}': {e}")
