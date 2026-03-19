# Fabric Notebook -- Module
# train_sarima_module.py -- SARIMA training per grain

import warnings
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX

warnings.filterwarnings("ignore")


def train_sarima_single(series: pd.Series, order: tuple, seasonal_order: tuple,
                        test_ratio: float = 0.2) -> dict:
    """Fit SARIMA on a single time series, return predictions and metrics."""
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
        return {"status": "success", "predictions": preds.tolist(),
                "metrics": metrics, "fitted_model": fitted}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def train_sarima_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                           target_column: str, order: tuple, seasonal_order: tuple,
                           test_ratio: float = 0.2, experiment_name: str = "",
                           model_name: str = "", min_series_length: int = 24) -> tuple:
    """Fit SARIMA per grain combination."""
    if experiment_name:
        ensure_experiment(experiment_name)

    results = []
    all_metrics = []

    groups = df.groupby(grain_columns)
    total = len(groups)
    print(f"[sarima] Training on {total} grain combinations...")

    for i, (grain_key, group) in enumerate(groups):
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        series = group.sort_values(date_column)[target_column]
        if len(series) < min_series_length:
            continue

        result = train_sarima_single(series, order, seasonal_order, test_ratio)

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

        if (i + 1) % 50 == 0:
            print(f"[sarima] Processed {i + 1}/{total} grains")

    agg_metrics = {}
    if all_metrics:
        for key in all_metrics[0]:
            vals = [m[key] for m in all_metrics if m.get(key) is not None]
            agg_metrics[key] = float(np.mean(vals)) if vals else None

        if experiment_name:
            try:
                import mlflow
                with mlflow.start_run(run_name=f"{model_name}_aggregate"):
                    log_metrics_to_mlflow(agg_metrics, prefix="sarima")
            except Exception:
                pass

    print(f"[sarima] Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}")
    return pd.DataFrame(results), agg_metrics
