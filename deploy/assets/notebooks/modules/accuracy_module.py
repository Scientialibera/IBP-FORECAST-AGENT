# Fabric Notebook -- Module
# accuracy_module.py -- Retrospective accuracy tracking: compare prior forecasts to actuals

import pandas as pd
import numpy as np
from datetime import datetime


def evaluate_forecast_accuracy(forecast_df: pd.DataFrame,
                               actuals_df: pd.DataFrame,
                               grain_columns: list,
                               period_column: str = "period",
                               forecast_col: str = "forecast_tons",
                               actual_col: str = "tons") -> pd.DataFrame:
    """
    Compare prior forecast snapshots to actuals.
    Returns per-grain accuracy metrics (MAPE, bias, RMSE).
    """
    merge_keys = grain_columns + [period_column]

    actual_sub = actuals_df[merge_keys + [actual_col]].copy()
    actual_sub.rename(columns={actual_col: "actual_tons"}, inplace=True)

    merged = forecast_df.merge(actual_sub, on=merge_keys, how="inner")

    if merged.empty:
        return pd.DataFrame()

    records = []
    groups = merged.groupby(grain_columns + ["version_id", "snapshot_month", "model_type"])

    for group_key, group in groups:
        if isinstance(group_key, tuple):
            key_dict = {}
            idx = 0
            for col in grain_columns:
                key_dict[col] = group_key[idx]
                idx += 1
            version_id = group_key[idx]
            snapshot_month = group_key[idx + 1]
            model_type = group_key[idx + 2]
        else:
            key_dict = {}
            version_id = ""
            snapshot_month = ""
            model_type = ""

        y_true = group["actual_tons"].values
        y_pred = group[forecast_col].values
        metrics = compute_metrics(y_true, y_pred)

        record = {
            **key_dict,
            "version_id": version_id,
            "snapshot_month": snapshot_month,
            "model_type": model_type,
            "n_periods": len(group),
            "mape": metrics.get("mape"),
            "bias": metrics.get("bias"),
            "rmse": metrics.get("rmse"),
            "mae": metrics.get("mae"),
            "r2": metrics.get("r2"),
            "evaluated_at": datetime.utcnow().isoformat(),
        }
        records.append(record)

    return pd.DataFrame(records)


def aggregate_accuracy_by_level(accuracy_df: pd.DataFrame,
                                group_columns: list) -> pd.DataFrame:
    """Aggregate accuracy metrics by specified hierarchy level."""
    if accuracy_df.empty:
        return pd.DataFrame()

    valid_cols = [c for c in group_columns if c in accuracy_df.columns]
    if not valid_cols:
        return pd.DataFrame()

    agg = accuracy_df.groupby(valid_cols + ["model_type"]).agg(
        avg_mape=("mape", "mean"),
        avg_bias=("bias", "mean"),
        avg_rmse=("rmse", "mean"),
        n_grains=("version_id", "count"),
    ).reset_index()

    return agg


def recommend_model_by_grain(accuracy_df: pd.DataFrame,
                             grain_columns: list,
                             metric: str = "mape") -> pd.DataFrame:
    """Recommend the best model per grain based on accuracy metrics."""
    if accuracy_df.empty:
        return pd.DataFrame()

    valid_cols = [c for c in grain_columns if c in accuracy_df.columns]
    best = (
        accuracy_df
        .sort_values(valid_cols + [metric])
        .groupby(valid_cols)
        .first()
        .reset_index()
    )
    best = best[valid_cols + ["model_type", metric]].copy()
    best.rename(columns={metric: f"best_{metric}"}, inplace=True)
    return best
