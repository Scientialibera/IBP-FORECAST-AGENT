# Fabric Notebook -- Module
# train_sarima_module.py -- SARIMA training per grain with tuning + MLflow persistence

import warnings
import pickle
import tempfile
import os
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX

warnings.filterwarnings("ignore")


def train_sarima_single(series: pd.Series, order: tuple, seasonal_order: tuple,
                        test_ratio: float = 0.2) -> dict:
    values = series.dropna().values
    n = len(values)
    split = int(n * (1 - test_ratio))
    if split < 12:
        return {"status": "insufficient_data", "predictions": [], "metrics": {}}

    train, test = values[:split], values[split:]
    try:
        model = SARIMAX(train, order=order, seasonal_order=seasonal_order,
                        enforce_stationarity=False, enforce_invertibility=False)
        fitted = model.fit(disp=False, maxiter=200)
        preds = fitted.forecast(steps=len(test))
        metrics = compute_metrics(test, preds)
        return {"status": "success", "predictions": preds.tolist(), "metrics": metrics}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def _refit_full(series_values, order, seasonal_order):
    model = SARIMAX(series_values, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    return model.fit(disp=False, maxiter=200)


def train_sarima_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                           target_column: str, order: tuple, seasonal_order: tuple,
                           test_ratio: float = 0.2, experiment_name: str = "",
                           model_name: str = "", min_series_length: int = 24,
                           tuning_enabled: bool = False, tuning_n_iter: int = 10,
                           tuning_n_splits: int = 3, tuning_metric: str = "rmse") -> tuple:
    if experiment_name:
        ensure_experiment(experiment_name)

    results = []
    all_metrics = []
    grain_models = {}
    grain_best_params = {}

    groups = df.groupby(grain_columns)
    total = len(groups)
    label = "[sarima+tune]" if tuning_enabled else "[sarima]"
    print(f"{label} Training on {total} grain combinations...")

    for i, (grain_key, group) in enumerate(groups):
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        series = group.sort_values(date_column)[target_column]
        if len(series) < min_series_length:
            continue

        grain_order = order
        grain_seasonal = seasonal_order
        key_str = "|".join(str(g) for g in grain_key)

        if tuning_enabled:
            try:
                tune_result = tune_sarima(series.dropna().values,
                                          n_iter=tuning_n_iter, n_splits=tuning_n_splits,
                                          metric=tuning_metric)
                bp = tune_result["best_params"]
                if bp and tune_result["best_score"] < float("inf"):
                    grain_order = tuple(bp.get("order", order))
                    grain_seasonal = tuple(bp.get("seasonal_order", seasonal_order))
                    grain_best_params[key_str] = {"order": grain_order, "seasonal_order": grain_seasonal,
                                                  "cv_score": tune_result["best_score"]}
            except Exception:
                pass

        result = train_sarima_single(series, grain_order, grain_seasonal, test_ratio)

        if result["status"] == "success" and result["predictions"]:
            split_idx = int(len(series) * (1 - test_ratio))
            test_dates = group.sort_values(date_column)["period"].iloc[split_idx:].values
            n_preds = min(len(result["predictions"]), len(test_dates))
            for j in range(n_preds):
                row = {date_column: str(test_dates[j]), "actual": float(series.iloc[split_idx + j]),
                       "predicted": result["predictions"][j], "model_type": "sarima"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
            all_metrics.append(result["metrics"])

            try:
                full_fitted = _refit_full(series.dropna().values, grain_order, grain_seasonal)
                grain_models[key_str] = full_fitted
            except Exception:
                pass

        if (i + 1) % 50 == 0:
            print(f"{label} Processed {i + 1}/{total} grains")

    agg_metrics = {}
    if all_metrics:
        for key in all_metrics[0]:
            vals = [m[key] for m in all_metrics if m.get(key) is not None]
            agg_metrics[key] = float(np.mean(vals)) if vals else None

        if experiment_name:
            try:
                import mlflow
                with mlflow.start_run(run_name=f"{model_name}_aggregate") as run:
                    log_metrics_to_mlflow(agg_metrics, prefix="sarima")
                    mlflow.log_metric("n_grains", len(grain_models))
                    mlflow.log_metric("tuning_enabled", int(tuning_enabled))
                    if grain_best_params:
                        mlflow.log_metric("n_tuned_grains", len(grain_best_params))
                    tmp = tempfile.mkdtemp()
                    pkl_path = os.path.join(tmp, "sarima_models.pkl")
                    with open(pkl_path, "wb") as f:
                        pickle.dump(grain_models, f)
                    mlflow.log_artifact(pkl_path, "models")
                    if grain_best_params:
                        params_path = os.path.join(tmp, "sarima_best_params.pkl")
                        with open(params_path, "wb") as f:
                            pickle.dump(grain_best_params, f)
                        mlflow.log_artifact(params_path, "tuning")
                    print(f"{label} Logged {len(grain_models)} grain models to MLflow run {run.info.run_id}")
            except Exception as e:
                print(f"{label} MLflow logging warning: {e}")

    tuned_str = f", {len(grain_best_params)} tuned" if tuning_enabled else ""
    print(f"{label} Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}{tuned_str}")
    return pd.DataFrame(results), agg_metrics
