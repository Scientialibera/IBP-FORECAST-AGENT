# Fabric Notebook
# 05_score_forecast.py -- Forward forecasting with all trained models
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

all_forecasts = []

# SARIMA
logger.info("[score] Forecasting with SARIMA...")
try:
    sarima_fc = forecast_sarima_forward(pdf, date_column, grain_columns, target_column,
                                        forecast_horizon, sarima_order, sarima_seasonal_order,
                                        experiment_name=experiment_name)
    if not sarima_fc.empty:
        all_forecasts.append(sarima_fc)
        logger.info(f"[score] SARIMA: {len(sarima_fc)} forecast rows")
except Exception as e:
    logger.error(f"[score] SARIMA failed: {e}")

# Prophet
logger.info("[score] Forecasting with Prophet...")
try:
    prophet_fc = forecast_prophet_forward(pdf, date_column, grain_columns, target_column,
                                          forecast_horizon, prophet_yearly, prophet_weekly, prophet_cp,
                                          experiment_name=experiment_name)
    if not prophet_fc.empty:
        all_forecasts.append(prophet_fc)
        logger.info(f"[score] Prophet: {len(prophet_fc)} forecast rows")
except Exception as e:
    logger.error(f"[score] Prophet failed: {e}")

# VAR
logger.info("[score] Forecasting with VAR...")
try:
    var_fc = forecast_var_forward(pdf, date_column, grain_columns, target_column,
                                  feature_columns, forecast_horizon, var_maxlags, var_ic)
    if not var_fc.empty:
        all_forecasts.append(var_fc)
        logger.info(f"[score] VAR: {len(var_fc)} forecast rows")
except Exception as e:
    logger.error(f"[score] VAR failed: {e}")

# Exponential Smoothing
logger.info("[score] Forecasting with Exp Smoothing...")
try:
    ets_fc = forecast_ets_forward(pdf, date_column, grain_columns, target_column,
                                   forecast_horizon, ets_trend, ets_seasonal, ets_seasonal_periods,
                                   experiment_name=experiment_name)
    if not ets_fc.empty:
        all_forecasts.append(ets_fc)
        logger.info(f"[score] Exp Smoothing: {len(ets_fc)} forecast rows")
except Exception as e:
    logger.error(f"[score] Exp Smoothing failed: {e}")

# Combine and write raw forecasts to silver (pre-versioning)
if all_forecasts:
    combined = pd.concat(all_forecasts, ignore_index=True)
    combined_spark = spark.createDataFrame(combined)
    write_lakehouse_table(combined_spark, silver_lakehouse_id, cfg("raw_forecasts_table"), mode="overwrite")
    logger.info(f"[score] Wrote {len(combined)} raw forecast rows to silver.{cfg('raw_forecasts_table')}")
else:
    logger.warning("[score] WARNING: No forecasts produced.")

logger.info("[score] Complete.")
