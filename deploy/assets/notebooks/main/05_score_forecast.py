# Fabric Notebook
# 05_score_forecast.py -- Forward forecasting with all enabled models
# Phase 1: Core Capability

# @parameters
silver_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/scoring_module


silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")
gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")

import pandas as pd

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
forecast_horizon = cfg("forecast_horizon")
experiment_name = named(cfg("experiment_name"))
sarima_order = tuple(cfg("sarima_order"))
sarima_seasonal_order = tuple(list(cfg("sarima_order")) + [freq_params("sarima_seasonal_s")])
ets_trend = cfg("exp_smoothing_trend") or "add"
ets_seasonal = cfg("exp_smoothing_seasonal") or "add"
ets_seasonal_periods = freq_params("seasonal_periods")
prophet_yearly = str(cfg("prophet_yearly_seasonality") or "true").lower() == "true"
prophet_weekly = str(cfg("prophet_weekly_seasonality") or "false").lower() == "true"
prophet_cp = float(cfg("prophet_changepoint_prior") or 0.05)
var_maxlags = freq_params("var_maxlags")
var_ic = cfg("var_ic") or "aic"

if not silver_lakehouse_id or not gold_lakehouse_id:
    raise ValueError("silver_lakehouse_id and gold_lakehouse_id are required.")

logger.info("[score] Loading feature table from silver.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, cfg("feature_table"))
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
logger.info(f"[score] Loaded {len(pdf)} rows. Forecasting {forecast_horizon} periods ahead.")

MODEL_SCORERS = {
    "sarima": lambda: forecast_sarima_forward(
        pdf, date_column, grain_columns, target_column,
        forecast_horizon, sarima_order, sarima_seasonal_order,
        experiment_name=experiment_name),
    "prophet": lambda: forecast_prophet_forward(
        pdf, date_column, grain_columns, target_column,
        forecast_horizon, prophet_yearly, prophet_weekly, prophet_cp,
        experiment_name=experiment_name),
    "var": lambda: forecast_var_forward(
        pdf, date_column, grain_columns, target_column,
        feature_columns, forecast_horizon, var_maxlags, var_ic,
        experiment_name=experiment_name),
    "exp_smoothing": lambda: forecast_ets_forward(
        pdf, date_column, grain_columns, target_column,
        forecast_horizon, ets_trend, ets_seasonal, ets_seasonal_periods,
        experiment_name=experiment_name),
    "lightgbm": lambda: forecast_lightgbm_forward(
        pdf, date_column, grain_columns, target_column,
        forecast_horizon, experiment_name=experiment_name),
}

models_enabled = cfg("models_enabled")
logger.info(f"[score] Enabled models: {models_enabled}")

all_forecasts = []

for model_type in models_enabled:
    scorer = MODEL_SCORERS.get(model_type)
    if scorer is None:
        logger.warning(f"[score] No scorer registered for '{model_type}', skipping.")
        continue

    logger.info(f"[score] Forecasting with {model_type}...")
    try:
        fc = scorer()
        if not fc.empty:
            all_forecasts.append(fc)
            logger.info(f"[score] {model_type}: {len(fc)} forecast rows")
        else:
            logger.warning(f"[score] {model_type}: 0 forecast rows")
    except Exception as e:
        logger.error(f"[score] {model_type} failed: {e}")

if all_forecasts:
    combined = pd.concat(all_forecasts, ignore_index=True)
    combined_spark = spark.createDataFrame(combined)
    write_lakehouse_table(combined_spark, silver_lakehouse_id, cfg("raw_forecasts_table"), mode="overwrite")
    logger.info(f"[score] Wrote {len(combined)} raw forecast rows to silver.{cfg('raw_forecasts_table')}")
else:
    logger.warning("[score] WARNING: No forecasts produced.")

logger.info("[score] Complete.")
