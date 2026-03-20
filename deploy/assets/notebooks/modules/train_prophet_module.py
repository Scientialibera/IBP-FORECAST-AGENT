# Fabric Notebook -- Module
# train_prophet_module.py -- Prophet training per grain with tuning + MLflow persistence

import warnings
import pickle
import tempfile
import os
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")


def _get_prophet_class():
    try:
        from prophet import Prophet
        return Prophet
    except ImportError:
        from fbprophet import Prophet
        return Prophet


def train_prophet_single(series_df: pd.DataFrame, yearly_seasonality: bool = True,
                         weekly_seasonality: bool = False, changepoint_prior: float = 0.05,
                         test_ratio: float = 0.2) -> dict:
    try:
        Prophet = _get_prophet_class()
    except ImportError:
        return {"status": "error: prophet not installed", "predictions": [], "metrics": {}}

    df = series_df.dropna(subset=["y"]).copy()
    n = len(df)
    split = int(n * (1 - test_ratio))
    if split < 12:
        return {"status": "insufficient_data", "predictions": [], "metrics": {}}

    train = df.iloc[:split]
    test = df.iloc[split:]
    try:
        model = Prophet(yearly_seasonality=yearly_seasonality, weekly_seasonality=weekly_seasonality,
                        daily_seasonality=False, changepoint_prior_scale=changepoint_prior)
        model.fit(train[["ds", "y"]])
        forecast = model.predict(test[["ds"]].copy())
        preds = forecast["yhat"].values
        metrics = compute_metrics(test["y"].values, preds)
        return {"status": "success", "predictions": preds.tolist(), "metrics": metrics}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def _refit_full(full_df, yearly, weekly, cp):
    Prophet = _get_prophet_class()
    model = Prophet(yearly_seasonality=yearly, weekly_seasonality=weekly,
                    daily_seasonality=False, changepoint_prior_scale=cp)
    model.fit(full_df[["ds", "y"]])
    return model


def train_prophet_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                            target_column: str, yearly_seasonality: bool = True,
                            weekly_seasonality: bool = False, changepoint_prior: float = 0.05,
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
    label = "[prophet+tune]" if tuning_enabled else "[prophet]"
    print(f"{label} Training on {total} grain combinations...")

    for i, (grain_key, group) in enumerate(groups):
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        group_sorted = group.sort_values(date_column).copy()
        if len(group_sorted) < min_series_length:
            continue

        prophet_df = pd.DataFrame({
            "ds": pd.to_datetime(group_sorted["period"]),
            "y": group_sorted[target_column].values,
        })

        g_yearly, g_weekly, g_cp = yearly_seasonality, weekly_seasonality, changepoint_prior
        key_str = "|".join(str(g) for g in grain_key)

        if tuning_enabled:
            try:
                clean = prophet_df.dropna(subset=["y"])
                tune_result = random_search_cv(
                    clean["y"].values,
                    lambda train_vals, horizon, **kw: prophet_fit_predict(
                        clean["ds"].values[:len(train_vals)], train_vals, horizon, **kw),
                    PROPHET_PARAM_GRID,
                    n_iter=tuning_n_iter, n_splits=tuning_n_splits, metric=tuning_metric,
                )
                bp = tune_result["best_params"]
                if bp and tune_result["best_score"] < float("inf"):
                    g_cp = bp.get("changepoint_prior_scale", changepoint_prior)
                    g_yearly = bp.get("yearly_seasonality", yearly_seasonality)
                    g_weekly = bp.get("weekly_seasonality", weekly_seasonality)
                    grain_best_params[key_str] = {
                        "changepoint_prior_scale": g_cp,
                        "yearly_seasonality": g_yearly,
                        "weekly_seasonality": g_weekly,
                        "cv_score": tune_result["best_score"],
                    }
            except Exception:
                pass

        result = train_prophet_single(prophet_df, g_yearly, g_weekly, g_cp, test_ratio)

        if result["status"] == "success" and result["predictions"]:
            split_idx = int(len(group_sorted) * (1 - test_ratio))
            test_rows = group_sorted.iloc[split_idx:]
            n_preds = min(len(result["predictions"]), len(test_rows))
            for j in range(n_preds):
                row = {date_column: str(test_rows["period"].iloc[j]),
                       "actual": float(test_rows[target_column].iloc[j]),
                       "predicted": result["predictions"][j], "model_type": "prophet"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
            all_metrics.append(result["metrics"])

            try:
                full_model = _refit_full(prophet_df, g_yearly, g_weekly, g_cp)
                grain_models[key_str] = full_model
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
                    log_metrics_to_mlflow(agg_metrics, prefix="prophet")
                    mlflow.log_metric("n_grains", len(grain_models))
                    mlflow.log_metric("tuning_enabled", int(tuning_enabled))
                    if grain_best_params:
                        mlflow.log_metric("n_tuned_grains", len(grain_best_params))
                    tmp = tempfile.mkdtemp()
                    pkl_path = os.path.join(tmp, "prophet_models.pkl")
                    with open(pkl_path, "wb") as f:
                        pickle.dump(grain_models, f)
                    mlflow.log_artifact(pkl_path, "models")
                    if grain_best_params:
                        params_path = os.path.join(tmp, "prophet_best_params.pkl")
                        with open(params_path, "wb") as f:
                            pickle.dump(grain_best_params, f)
                        mlflow.log_artifact(params_path, "tuning")
                    print(f"{label} Logged {len(grain_models)} grain models to MLflow run {run.info.run_id}")
            except Exception as e:
                print(f"{label} MLflow logging warning: {e}")

    tuned_str = f", {len(grain_best_params)} tuned" if tuning_enabled else ""
    print(f"{label} Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}{tuned_str}")
    return pd.DataFrame(results), agg_metrics
