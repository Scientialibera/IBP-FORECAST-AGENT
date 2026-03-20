# Fabric Notebook -- Module
# scoring_module.py -- Batch scoring using MLflow-persisted models (no fallback refit)

import warnings
import pickle
import os
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")


def _load_grain_models(experiment_name: str, run_name_prefix: str, artifact_filename: str) -> dict:
    """Load the pickled grain-models dict from the latest matching MLflow run.
    Raises on failure -- no silent fallbacks."""
    import mlflow
    exp = mlflow.get_experiment_by_name(experiment_name)
    if not exp:
        raise ValueError(f"Experiment '{experiment_name}' not found in MLflow")

    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=f"tags.mlflow.runName LIKE '%{run_name_prefix}%'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError(
            f"No MLflow runs matching '%{run_name_prefix}%' in experiment '{experiment_name}'. "
            f"Have the 04_train_* notebooks been executed first?"
        )

    run_id = runs.iloc[0]["run_id"]
    print(f"[scoring] Loading models from run {run_id} ({run_name_prefix})")

    local_dir = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="models")
    pkl_path = os.path.join(local_dir, artifact_filename)
    with open(pkl_path, "rb") as f:
        models = pickle.load(f)
    print(f"[scoring] Loaded {len(models)} grain models")
    return models


def _future_dates(last_period_str: str, horizon: int):
    last_date = pd.to_datetime(last_period_str)
    return pd.date_range(start=last_date + pd.DateOffset(months=1), periods=horizon, freq="MS")


def _build_rows(future_dates, preds, grain_key, grain_columns, model_type):
    rows = []
    for d, p in zip(future_dates, preds):
        row = {"period": str(d.date()), "forecast_tons": float(p), "model_type": model_type}
        for k, col in enumerate(grain_columns):
            row[col] = grain_key[k] if k < len(grain_key) else ""
        rows.append(row)
    return rows


def forecast_sarima_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                            target_column: str, horizon: int, order: tuple,
                            seasonal_order: tuple,
                            experiment_name: str = "") -> pd.DataFrame:
    """Load SARIMA models from MLflow and forecast forward."""
    grain_models = _load_grain_models(experiment_name, "sarima", "sarima_models.pkl")

    results = []
    groups = df.groupby(grain_columns)
    skipped = 0

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)
        key_str = "|".join(str(g) for g in grain_key)
        group_sorted = group.sort_values(date_column)
        last_period = group_sorted["period"].iloc[-1]

        if key_str not in grain_models:
            skipped += 1
            continue

        fitted = grain_models[key_str]
        preds = fitted.forecast(steps=horizon)
        future = _future_dates(last_period, horizon)
        results.extend(_build_rows(future, preds, grain_key, grain_columns, "sarima"))

    if skipped:
        print(f"[scoring] SARIMA: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_prophet_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                             target_column: str, horizon: int,
                             yearly: bool = True, weekly: bool = False,
                             changepoint_prior: float = 0.05,
                             experiment_name: str = "") -> pd.DataFrame:
    """Load Prophet models from MLflow and forecast forward."""
    grain_models = _load_grain_models(experiment_name, "prophet", "prophet_models.pkl")

    results = []
    groups = df.groupby(grain_columns)
    skipped = 0

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)
        key_str = "|".join(str(g) for g in grain_key)

        if key_str not in grain_models:
            skipped += 1
            continue

        model = grain_models[key_str]
        future = model.make_future_dataframe(periods=horizon, freq="MS")
        forecast = model.predict(future)
        future_forecast = forecast.iloc[-horizon:]
        for _, frow in future_forecast.iterrows():
            r = {"period": str(frow["ds"].date()), "forecast_tons": float(frow["yhat"]),
                 "model_type": "prophet"}
            for k, col in enumerate(grain_columns):
                r[col] = grain_key[k] if k < len(grain_key) else ""
            results.append(r)

    if skipped:
        print(f"[scoring] Prophet: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_var_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, feature_columns: list,
                         horizon: int, maxlags: int = 12, ic: str = "aic",
                         experiment_name: str = "") -> pd.DataFrame:
    """Load VAR models from MLflow and forecast forward."""
    grain_models = _load_grain_models(experiment_name, "var", "var_models.pkl")

    results = []
    groups = df.groupby(grain_columns)
    skipped = 0

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)
        key_str = "|".join(str(g) for g in grain_key)
        group_sorted = group.sort_values(date_column)
        last_period = group_sorted["period"].iloc[-1]

        if key_str not in grain_models:
            skipped += 1
            continue

        model_data = grain_models[key_str]
        fitted = model_data["fitted"]
        cols = model_data["columns"]
        full_data = group_sorted[cols].dropna().values
        forecast_input = full_data[-fitted.k_ar:]
        forecast = fitted.forecast(forecast_input, steps=horizon)
        preds = forecast[:, 0]
        future = _future_dates(last_period, horizon)
        results.extend(_build_rows(future, preds, grain_key, grain_columns, "var"))

    if skipped:
        print(f"[scoring] VAR: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_ets_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, horizon: int, trend: str = "add",
                         seasonal: str = "add", seasonal_periods: int = 12,
                         experiment_name: str = "") -> pd.DataFrame:
    """Load ETS models from MLflow and forecast forward."""
    grain_models = _load_grain_models(experiment_name, "exp_smoothing", "ets_models.pkl")

    results = []
    groups = df.groupby(grain_columns)
    skipped = 0

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)
        key_str = "|".join(str(g) for g in grain_key)
        group_sorted = group.sort_values(date_column)
        last_period = group_sorted["period"].iloc[-1]

        if key_str not in grain_models:
            skipped += 1
            continue

        fitted = grain_models[key_str]
        preds = fitted.forecast(steps=horizon)
        future = _future_dates(last_period, horizon)
        results.extend(_build_rows(future, preds, grain_key, grain_columns, "exp_smoothing"))

    if skipped:
        print(f"[scoring] ETS: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)
