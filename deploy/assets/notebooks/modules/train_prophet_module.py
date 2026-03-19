# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# METADATA ********************

# CELL ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# train_prophet_module.py -- Prophet training per grain

# CELL ********************

import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")


def train_prophet_single(series_df: pd.DataFrame, yearly_seasonality: bool = True,
                         weekly_seasonality: bool = False, changepoint_prior: float = 0.05,
                         test_ratio: float = 0.2) -> dict:
    """Fit Prophet on a single time series. Expects columns 'ds' and 'y'."""
    try:
        from prophet import Prophet
    except ImportError:
        try:
            from fbprophet import Prophet
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
        model = Prophet(
            yearly_seasonality=yearly_seasonality,
            weekly_seasonality=weekly_seasonality,
            daily_seasonality=False,
            changepoint_prior_scale=changepoint_prior,
        )
        model.fit(train[["ds", "y"]])

        future = test[["ds"]].copy()
        forecast = model.predict(future)
        preds = forecast["yhat"].values

        from utils_module import compute_metrics
        metrics = compute_metrics(test["y"].values, preds)
        return {"status": "success", "predictions": preds.tolist(),
                "metrics": metrics, "fitted_model": model}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def train_prophet_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                            target_column: str, yearly_seasonality: bool = True,
                            weekly_seasonality: bool = False, changepoint_prior: float = 0.05,
                            test_ratio: float = 0.2, experiment_name: str = "",
                            model_name: str = "", min_series_length: int = 24) -> tuple:
    """Fit Prophet per grain combination."""
    from utils_module import ensure_experiment, log_metrics_to_mlflow

    if experiment_name:
        ensure_experiment(experiment_name)

    results = []
    all_metrics = []

    groups = df.groupby(grain_columns)
    total = len(groups)
    print(f"[prophet] Training on {total} grain combinations...")

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

        result = train_prophet_single(prophet_df, yearly_seasonality,
                                      weekly_seasonality, changepoint_prior, test_ratio)

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

        if (i + 1) % 50 == 0:
            print(f"[prophet] Processed {i + 1}/{total} grains")

    agg_metrics = {}
    if all_metrics:
        for key in all_metrics[0]:
            vals = [m[key] for m in all_metrics if m.get(key) is not None]
            agg_metrics[key] = float(np.mean(vals)) if vals else None

        if experiment_name:
            try:
                import mlflow
                with mlflow.start_run(run_name=f"{model_name}_aggregate"):
                    log_metrics_to_mlflow(agg_metrics, prefix="prophet")
            except Exception:
                pass

    print(f"[prophet] Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}")
    return pd.DataFrame(results), agg_metrics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
