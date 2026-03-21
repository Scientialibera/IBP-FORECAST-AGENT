# Fabric Notebook -- Module
# scoring_module.py -- Batch scoring using MLflow-persisted models (no fallback refit)

import warnings
import pickle
import os
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("ibp")

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
    logger.info(f"[scoring] Loading models from run {run_id} ({run_name_prefix})")

    local_dir = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="models")
    pkl_path = os.path.join(local_dir, artifact_filename)
    with open(pkl_path, "rb") as f:
        models = pickle.load(f)
    logger.info(f"[scoring] Loaded {len(models)} grain models")
    return models


def _future_dates(last_period_str: str, horizon: int):
    last_date = pd.to_datetime(last_period_str)
    return pd.date_range(
        start=last_date + pd.DateOffset(**{freq_params("offset_kwarg"): 1}),
        periods=horizon,
        freq=freq_params("code"),
    )


def _clip_forecasts(preds, historical_values, multiplier=5.0):
    """Clip forecasts to a sane range based on historical data.
    Prevents SARIMA/VAR numerical explosions from corrupting results."""
    arr = np.asarray(preds, dtype=float)
    hist = np.asarray(historical_values, dtype=float)
    hist = hist[np.isfinite(hist)]
    if len(hist) == 0:
        return arr
    cap = max(abs(hist.max()), abs(hist.mean())) * multiplier
    return np.clip(arr, 0.0, cap)


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
        raw_preds = fitted.forecast(steps=horizon)
        hist_vals = group_sorted[target_column].dropna().values
        preds = _clip_forecasts(raw_preds, hist_vals)
        future = _future_dates(last_period, horizon)
        results.extend(_build_rows(future, preds, grain_key, grain_columns, "sarima"))

    if skipped:
        logger.warning(f"[scoring] SARIMA: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_prophet_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                             target_column: str, horizon: int,
                             yearly=True, weekly: bool = False,
                             changepoint_prior: float = 0.05,
                             experiment_name: str = "") -> pd.DataFrame:
    """Load Prophet models from MLflow and forecast forward."""
    grain_models = _load_grain_models(experiment_name, "prophet", "prophet_models.pkl")

    prophet_freq = freq_params("prophet_freq")
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
        group_sorted = group.sort_values(date_column)
        last_period = pd.to_datetime(group_sorted["period"].iloc[-1])

        future = model.make_future_dataframe(periods=horizon, freq=prophet_freq)
        future = future[future["ds"] > last_period].head(horizon)
        if future.empty:
            future = pd.date_range(
                start=last_period + pd.DateOffset(**{freq_params("offset_kwarg"): 1}),
                periods=horizon, freq=freq_params("code"),
            ).to_frame(index=False, name="ds")

        forecast = model.predict(future)
        hist_vals = group_sorted[target_column].dropna().values
        preds = _clip_forecasts(forecast["yhat"].values, hist_vals)

        results.extend(_build_rows(
            pd.to_datetime(forecast["ds"]), preds, grain_key, grain_columns, "prophet"
        ))

    if skipped:
        logger.warning(f"[scoring] Prophet: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_var_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, feature_columns: list,
                         horizon: int, maxlags: int = None, ic: str = "aic",
                         experiment_name: str = "") -> pd.DataFrame:
    """Load VAR models from MLflow and forecast forward."""
    maxlags = maxlags or freq_params("var_maxlags")
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
        raw_preds = forecast[:, 0]
        hist_vals = group_sorted[target_column].dropna().values
        preds = _clip_forecasts(raw_preds, hist_vals)
        future = _future_dates(last_period, horizon)
        results.extend(_build_rows(future, preds, grain_key, grain_columns, "var"))

    if skipped:
        logger.warning(f"[scoring] VAR: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_ets_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, horizon: int, trend: str = "add",
                         seasonal: str = "add", seasonal_periods: int = None,
                         experiment_name: str = "") -> pd.DataFrame:
    """Load ETS models from MLflow and forecast forward."""
    seasonal_periods = seasonal_periods or freq_params("seasonal_periods")
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
        logger.warning(f"[scoring] ETS: skipped {skipped} grains (no trained model)")
    return pd.DataFrame(results)


def forecast_lightgbm_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                               target_column: str, horizon: int,
                               experiment_name: str = "") -> pd.DataFrame:
    """Load global LightGBM model from MLflow and forecast forward recursively.

    At each step the prediction is appended to the history buffer so that
    lag and rolling features can be recomputed for the next step.
    """
    model_data = _load_grain_models(experiment_name, "lightgbm", "lightgbm_model.pkl")
    model = model_data["model"]
    feature_cols = model_data["feature_cols"]
    label_encoders = model_data["label_encoders"]

    lags = freq_params("default_lags")
    rolling_wins = freq_params("default_rolling")
    domain_cols = cfg("feature_columns")
    ppy = freq_params("periods_per_year")
    freq = cfg("frequency")

    results = []
    groups = df.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        group_sorted = group.sort_values(date_column)
        last_period = group_sorted["period"].iloc[-1]
        history = group_sorted[target_column].dropna().values.tolist()
        last_row = group_sorted.iloc[-1]

        grain_enc = {}
        for i, gc in enumerate(grain_columns):
            enc_col = f"{gc}_enc"
            if enc_col in feature_cols:
                le = label_encoders.get(gc)
                try:
                    grain_enc[enc_col] = le.transform([grain_key[i]])[0]
                except (ValueError, KeyError):
                    grain_enc[enc_col] = -1

        future_dates = _future_dates(last_period, horizon)

        for fut_date in future_dates:
            features = {}

            for l in lags:
                features[f"{target_column}_lag_{l}"] = (
                    history[-l] if len(history) >= l else np.nan
                )

            for w in rolling_wins:
                window = history[-w:] if len(history) >= w else history
                features[f"{target_column}_roll_mean_{w}"] = (
                    np.mean(window) if window else 0
                )
                features[f"{target_column}_roll_std_{w}"] = (
                    np.std(window) if len(window) > 1 else 0
                )

            if freq == "D":
                period_of_year = fut_date.dayofyear
            elif freq == "W":
                period_of_year = fut_date.isocalendar()[1]
            else:
                period_of_year = fut_date.month
            features["month_sin"] = np.sin(2 * np.pi * period_of_year / ppy)
            features["month_cos"] = np.cos(2 * np.pi * period_of_year / ppy)
            features["quarter"] = (fut_date.month - 1) // 3 + 1

            for dc in domain_cols:
                features[dc] = (
                    float(last_row[dc]) if dc in last_row.index else 0
                )

            features.update(grain_enc)

            X = np.array([[features.get(c, 0) for c in feature_cols]])
            X = np.nan_to_num(X, nan=0.0)
            pred = max(0, float(model.predict(X)[0]))

            history.append(pred)
            row = {"period": str(fut_date.date()), "forecast_tons": pred,
                   "model_type": "lightgbm"}
            for k, gc in enumerate(grain_columns):
                row[gc] = grain_key[k] if k < len(grain_key) else ""
            results.append(row)

    logger.info(f"[scoring] LightGBM: {len(results)} forecast rows")
    return pd.DataFrame(results)
