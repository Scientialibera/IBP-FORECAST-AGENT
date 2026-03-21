# Fabric Notebook -- Module
# train_var_module.py -- VAR training per grain with tuning + MLflow persistence

import warnings
import pickle
import tempfile
import os
import pandas as pd
import numpy as np
from statsmodels.tsa.api import VAR as VARModel
import logging

logger = logging.getLogger("ibp")

warnings.filterwarnings("ignore")


def train_var_single(df: pd.DataFrame, target_column: str, feature_columns: list,
                     maxlags: int = None, ic: str = "aic", test_ratio: float = 0.2) -> dict:
    maxlags = maxlags or freq_params("var_maxlags")
    cols = [target_column] + [c for c in feature_columns if c in df.columns and c != target_column]
    subset = df[cols].dropna()
    n = len(subset)
    split = int(n * (1 - test_ratio))
    if split < maxlags + 2:
        return {"status": "insufficient_data", "predictions": [], "metrics": {}}

    train = subset.iloc[:split]
    test = subset.iloc[split:]
    try:
        model = VARModel(train.values)
        fitted = model.fit(maxlags=maxlags, ic=ic)
        forecast_input = train.values[-fitted.k_ar:]
        forecast = fitted.forecast(forecast_input, steps=len(test))
        raw_preds = forecast[:, 0]
        cap = max(abs(train[target_column].max()), abs(train[target_column].mean())) * 5.0
        preds = np.clip(raw_preds, 0.0, cap)
        metrics = compute_metrics(test[target_column].values, preds)
        return {"status": "success", "predictions": preds.tolist(),
                "metrics": metrics, "lag_order": fitted.k_ar, "columns": cols}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def _refit_full(df_subset_values, maxlags, ic):
    model = VARModel(df_subset_values)
    return model.fit(maxlags=maxlags, ic=ic)


def train_var_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                        target_column: str, feature_columns: list,
                        maxlags: int = None, ic: str = "aic", test_ratio: float = 0.2,
                        experiment_name: str = "", model_name: str = "",
                        min_series_length: int | None = None,
                        tuning_enabled: bool = False, tuning_n_iter: int = 8,
                        tuning_n_splits: int = 3, tuning_metric: str = "rmse") -> tuple:
    min_series_length = min_series_length or freq_params("min_train_periods")
    maxlags = maxlags or freq_params("var_maxlags")
    if experiment_name:
        ensure_experiment(experiment_name)

    results = []
    all_metrics = []
    grain_models = {}
    grain_best_params = {}

    groups = df.groupby(grain_columns)
    total = len(groups)
    label = "[var+tune]" if tuning_enabled else "[var]"
    logger.info(f"{label} Training on {total} grain combinations...")

    for i, (grain_key, group) in enumerate(groups):
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        group_sorted = group.sort_values(date_column).copy()
        if len(group_sorted) < min_series_length:
            continue

        numeric_feats = [c for c in feature_columns if c in group_sorted.columns
                         and group_sorted[c].dtype in ["float64", "int64", "float32", "int32"]
                         and group_sorted[c].notna().sum() > min_series_length * 0.5]
        if not numeric_feats:
            continue

        g_maxlags, g_ic = maxlags, ic
        key_str = "|".join(str(g) for g in grain_key)

        if tuning_enabled:
            try:
                cols = [target_column] + [c for c in numeric_feats if c != target_column]
                data_arr = group_sorted[cols].dropna().values
                if len(data_arr) > min_series_length:
                    tune_result = tune_var_single(data_arr, n_iter=tuning_n_iter,
                                                  n_splits=tuning_n_splits, metric=tuning_metric)
                    bp = tune_result["best_params"]
                    if bp and tune_result["best_score"] < float("inf"):
                        g_maxlags = bp.get("maxlags", maxlags)
                        g_ic = bp.get("ic", ic)
                        grain_best_params[key_str] = {"maxlags": g_maxlags, "ic": g_ic,
                                                      "cv_score": tune_result["best_score"]}
            except Exception:
                pass

        result = train_var_single(group_sorted, target_column, numeric_feats, g_maxlags, g_ic, test_ratio)

        if result["status"] == "success" and result["predictions"]:
            split_idx = int(len(group_sorted) * (1 - test_ratio))
            test_rows = group_sorted.iloc[split_idx:]
            n_preds = min(len(result["predictions"]), len(test_rows))
            for j in range(n_preds):
                row = {date_column: str(test_rows["period"].iloc[j]),
                       "actual": float(test_rows[target_column].iloc[j]),
                       "predicted": result["predictions"][j], "model_type": "var"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
            all_metrics.append(result["metrics"])

            try:
                cols = result.get("columns", [target_column] + numeric_feats)
                full_data = group_sorted[cols].dropna().values
                full_fitted = _refit_full(full_data, g_maxlags, g_ic)
                grain_models[key_str] = {"fitted": full_fitted, "columns": cols}
            except Exception:
                pass

        if (i + 1) % 50 == 0:
            logger.info(f"{label} Processed {i + 1}/{total} grains")

    agg_metrics = {}
    if all_metrics:
        for key in all_metrics[0]:
            vals = [m[key] for m in all_metrics if m.get(key) is not None]
            agg_metrics[key] = float(np.mean(vals)) if vals else None

        if experiment_name:
            try:
                import mlflow
                with mlflow.start_run(run_name=f"{model_name}_aggregate") as run:
                    log_metrics_to_mlflow(agg_metrics, prefix="var")
                    mlflow.log_metric("n_grains", len(grain_models))
                    mlflow.log_metric("tuning_enabled", int(tuning_enabled))
                    if grain_best_params:
                        mlflow.log_metric("n_tuned_grains", len(grain_best_params))
                    tmp = tempfile.mkdtemp()
                    pkl_path = os.path.join(tmp, "var_models.pkl")
                    with open(pkl_path, "wb") as f:
                        pickle.dump(grain_models, f)
                    mlflow.log_artifact(pkl_path, "models")
                    if grain_best_params:
                        params_path = os.path.join(tmp, "var_best_params.pkl")
                        with open(params_path, "wb") as f:
                            pickle.dump(grain_best_params, f)
                        mlflow.log_artifact(params_path, "tuning")
                    logger.info(f"{label} Logged {len(grain_models)} grain models to MLflow run {run.info.run_id}")
            except Exception as e:
                logger.warning(f"{label} MLflow logging warning: {e}")

    tuned_str = f", {len(grain_best_params)} tuned" if tuning_enabled else ""
    logger.info(f"{label} Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}{tuned_str}")
    return pd.DataFrame(results), agg_metrics
