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

# scoring_module.py -- Batch scoring / forward forecasting for all model types

# CELL ********************

import warnings
import pandas as pd
import numpy as np
from datetime import datetime

warnings.filterwarnings("ignore")


def forecast_sarima_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                            target_column: str, horizon: int, order: tuple,
                            seasonal_order: tuple) -> pd.DataFrame:
    """Re-fit SARIMA on full history per grain and forecast forward."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    results = []
    groups = df.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        series = group.sort_values(date_column)[target_column].dropna()
        if len(series) < 24:
            continue

        try:
            model = SARIMAX(series.values, order=order, seasonal_order=seasonal_order,
                            enforce_stationarity=False, enforce_invertibility=False)
            fitted = model.fit(disp=False, maxiter=200)
            preds = fitted.forecast(steps=horizon)

            last_date = pd.to_datetime(group.sort_values(date_column)["period"].iloc[-1])
            future_dates = pd.date_range(start=last_date + pd.DateOffset(months=1),
                                         periods=horizon, freq="MS")

            for j, (d, p) in enumerate(zip(future_dates, preds)):
                row = {"period": str(d.date()), "forecast_tons": float(p), "model_type": "sarima"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
        except Exception:
            continue

    return pd.DataFrame(results)


def forecast_prophet_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                             target_column: str, horizon: int,
                             yearly: bool = True, weekly: bool = False,
                             changepoint_prior: float = 0.05) -> pd.DataFrame:
    """Re-fit Prophet on full history per grain and forecast forward."""
    try:
        from prophet import Prophet
    except ImportError:
        from fbprophet import Prophet

    results = []
    groups = df.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        group_sorted = group.sort_values(date_column)
        if len(group_sorted) < 24:
            continue

        prophet_df = pd.DataFrame({
            "ds": pd.to_datetime(group_sorted["period"]),
            "y": group_sorted[target_column].values,
        }).dropna()

        try:
            model = Prophet(yearly_seasonality=yearly, weekly_seasonality=weekly,
                            daily_seasonality=False, changepoint_prior_scale=changepoint_prior)
            model.fit(prophet_df)
            future = model.make_future_dataframe(periods=horizon, freq="MS")
            forecast = model.predict(future)
            future_forecast = forecast.iloc[-horizon:]

            for _, row in future_forecast.iterrows():
                r = {"period": str(row["ds"].date()), "forecast_tons": float(row["yhat"]),
                     "model_type": "prophet"}
                for k, col in enumerate(grain_columns):
                    r[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(r)
        except Exception:
            continue

    return pd.DataFrame(results)


def forecast_var_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, feature_columns: list,
                         horizon: int, maxlags: int = 12, ic: str = "aic") -> pd.DataFrame:
    """Re-fit VAR on full history per grain and forecast forward."""
    from statsmodels.tsa.api import VAR as VARModel

    results = []
    groups = df.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        group_sorted = group.sort_values(date_column)
        cols = [target_column] + [c for c in feature_columns if c in group_sorted.columns
                                  and group_sorted[c].dtype in ["float64", "int64"]]
        subset = group_sorted[cols].dropna()
        if len(subset) < maxlags + 5:
            continue

        try:
            model = VARModel(subset.values)
            fitted = model.fit(maxlags=maxlags, ic=ic)
            forecast_input = subset.values[-fitted.k_ar:]
            forecast = fitted.forecast(forecast_input, steps=horizon)
            preds = forecast[:, 0]

            last_date = pd.to_datetime(group_sorted["period"].iloc[-1])
            future_dates = pd.date_range(start=last_date + pd.DateOffset(months=1),
                                         periods=horizon, freq="MS")

            for j, (d, p) in enumerate(zip(future_dates, preds)):
                row = {"period": str(d.date()), "forecast_tons": float(p), "model_type": "var"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
        except Exception:
            continue

    return pd.DataFrame(results)


def forecast_ets_forward(df: pd.DataFrame, date_column: str, grain_columns: list,
                         target_column: str, horizon: int, trend: str = "add",
                         seasonal: str = "add", seasonal_periods: int = 12) -> pd.DataFrame:
    """Re-fit Exponential Smoothing on full history per grain and forecast forward."""
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    results = []
    groups = df.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        series = group.sort_values(date_column)[target_column].dropna()
        if len(series) < seasonal_periods * 2:
            continue

        try:
            model = ExponentialSmoothing(series.values, trend=trend, seasonal=seasonal,
                                         seasonal_periods=seasonal_periods)
            fitted = model.fit(optimized=True)
            preds = fitted.forecast(steps=horizon)

            last_date = pd.to_datetime(group.sort_values(date_column)["period"].iloc[-1])
            future_dates = pd.date_range(start=last_date + pd.DateOffset(months=1),
                                         periods=horizon, freq="MS")

            for j, (d, p) in enumerate(zip(future_dates, preds)):
                row = {"period": str(d.date()), "forecast_tons": float(p), "model_type": "exp_smoothing"}
                for k, col in enumerate(grain_columns):
                    row[col] = grain_key[k] if k < len(grain_key) else ""
                results.append(row)
        except Exception:
            continue

    return pd.DataFrame(results)

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
