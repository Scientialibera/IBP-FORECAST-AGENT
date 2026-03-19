# Fabric Notebook -- Module
# train_var_module.py -- VAR (Vector Autoregression) training per grain
# Multivariate time series: forecasts target using correlated features.

import warnings
import pandas as pd
import numpy as np
from statsmodels.tsa.api import VAR as VARModel

warnings.filterwarnings("ignore")


def train_var_single(df: pd.DataFrame, target_column: str, feature_columns: list,
                     maxlags: int = 12, ic: str = "aic", test_ratio: float = 0.2) -> dict:
    """Fit VAR on multiple columns for a single grain."""
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
        preds = forecast[:, 0]

        from utils_module import compute_metrics
        metrics = compute_metrics(test[target_column].values, preds)
        return {"status": "success", "predictions": preds.tolist(),
                "metrics": metrics, "lag_order": fitted.k_ar}
    except Exception as e:
        return {"status": f"error: {e}", "predictions": [], "metrics": {}}


def train_var_per_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                        target_column: str, feature_columns: list,
                        maxlags: int = 12, ic: str = "aic", test_ratio: float = 0.2,
                        experiment_name: str = "", model_name: str = "",
                        min_series_length: int = 24) -> tuple:
    """Fit VAR per grain combination using target + correlated features."""
    from utils_module import ensure_experiment, log_metrics_to_mlflow

    if experiment_name:
        ensure_experiment(experiment_name)

    results = []
    all_metrics = []

    groups = df.groupby(grain_columns)
    total = len(groups)
    print(f"[var] Training on {total} grain combinations...")

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

        result = train_var_single(group_sorted, target_column, numeric_feats,
                                  maxlags, ic, test_ratio)

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

        if (i + 1) % 50 == 0:
            print(f"[var] Processed {i + 1}/{total} grains")

    agg_metrics = {}
    if all_metrics:
        for key in all_metrics[0]:
            vals = [m[key] for m in all_metrics if m.get(key) is not None]
            agg_metrics[key] = float(np.mean(vals)) if vals else None

        if experiment_name:
            try:
                import mlflow
                with mlflow.start_run(run_name=f"{model_name}_aggregate"):
                    log_metrics_to_mlflow(agg_metrics, prefix="var")
            except Exception:
                pass

    print(f"[var] Complete. {len(results)} prediction rows, avg MAPE={agg_metrics.get('mape', 'N/A')}")
    return pd.DataFrame(results), agg_metrics
