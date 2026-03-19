# Fabric Notebook -- Module
# utils_module.py -- Metrics, time splits, MLflow helpers

import numpy as np
import pandas as pd
import uuid
from datetime import datetime


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


def log_metrics_to_mlflow(metrics: dict, prefix: str = "") -> None:
    """Log metrics dict to active MLflow run."""
    try:
        import mlflow
        for k, v in metrics.items():
            if v is not None:
                name = f"{prefix}_{k}" if prefix else k
                mlflow.log_metric(name, v)
    except Exception as e:
        print(f"[mlflow] Warning: could not log metrics: {e}")


def ensure_experiment(experiment_name: str) -> None:
    """Attach to existing MLflow experiment by ID (not name) to avoid root-level duplicates."""
    try:
        import mlflow
        mlflow.autolog(disable=True)
        exp = mlflow.get_experiment_by_name(experiment_name)
        if exp:
            mlflow.set_experiment(experiment_id=exp.experiment_id)
        # If not found, skip -- deploy script should have pre-created it in the right folder.
        # Do NOT call set_experiment(name) as that creates at workspace root.
    except Exception as e:
        print(f"[mlflow] Warning: could not set experiment '{experiment_name}': {e}")
