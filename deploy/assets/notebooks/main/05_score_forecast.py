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

# 05_score_forecast.py -- Forward forecasting with all trained models
# Phase 1: Core Capability

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run scoring_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

params = get_notebook_params()

silver_lakehouse_id = params["silver_lakehouse_id"]
gold_lakehouse_id = params["gold_lakehouse_id"]
date_column = params["date_column"]
target_column = params["target_column"]
grain_columns = parse_list_param(params["grain_columns"])
feature_columns = parse_list_param(params["feature_columns"])
forecast_horizon = int(params.get("forecast_horizon") or 6)
sarima_order = tuple(parse_int_list_param(params.get("sarima_order") or "[1,1,1]"))
sarima_seasonal_order = tuple(parse_int_list_param(params.get("sarima_seasonal_order") or "[1,1,1,12]"))
ets_trend = params.get("exp_smoothing_trend") or "add"
ets_seasonal = params.get("exp_smoothing_seasonal") or "add"
ets_seasonal_periods = int(params.get("exp_smoothing_seasonal_periods") or 12)
prophet_yearly = str(params.get("prophet_yearly_seasonality") or "true").lower() == "true"
prophet_weekly = str(params.get("prophet_weekly_seasonality") or "false").lower() == "true"
prophet_cp = float(params.get("prophet_changepoint_prior") or 0.05)
var_maxlags = int(params.get("var_maxlags") or 12)
var_ic = params.get("var_ic") or "aic"

if not silver_lakehouse_id or not gold_lakehouse_id:
    raise ValueError("silver_lakehouse_id and gold_lakehouse_id are required.")

print("[score] Loading feature table from silver.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[score] Loaded {len(pdf)} rows. Forecasting {forecast_horizon} periods ahead.")

all_forecasts = []

# SARIMA
print("[score] Forecasting with SARIMA...")
try:
    sarima_fc = forecast_sarima_forward(pdf, date_column, grain_columns, target_column,
                                        forecast_horizon, sarima_order, sarima_seasonal_order)
    if not sarima_fc.empty:
        all_forecasts.append(sarima_fc)
        print(f"[score] SARIMA: {len(sarima_fc)} forecast rows")
except Exception as e:
    print(f"[score] SARIMA failed: {e}")

# Prophet
print("[score] Forecasting with Prophet...")
try:
    prophet_fc = forecast_prophet_forward(pdf, date_column, grain_columns, target_column,
                                          forecast_horizon, prophet_yearly, prophet_weekly, prophet_cp)
    if not prophet_fc.empty:
        all_forecasts.append(prophet_fc)
        print(f"[score] Prophet: {len(prophet_fc)} forecast rows")
except Exception as e:
    print(f"[score] Prophet failed: {e}")

# VAR
print("[score] Forecasting with VAR...")
try:
    var_fc = forecast_var_forward(pdf, date_column, grain_columns, target_column,
                                  feature_columns, forecast_horizon, var_maxlags, var_ic)
    if not var_fc.empty:
        all_forecasts.append(var_fc)
        print(f"[score] VAR: {len(var_fc)} forecast rows")
except Exception as e:
    print(f"[score] VAR failed: {e}")

# Exponential Smoothing
print("[score] Forecasting with Exp Smoothing...")
try:
    ets_fc = forecast_ets_forward(pdf, date_column, grain_columns, target_column,
                                   forecast_horizon, ets_trend, ets_seasonal, ets_seasonal_periods)
    if not ets_fc.empty:
        all_forecasts.append(ets_fc)
        print(f"[score] Exp Smoothing: {len(ets_fc)} forecast rows")
except Exception as e:
    print(f"[score] Exp Smoothing failed: {e}")

# Combine and write raw forecasts to silver (pre-versioning)
if all_forecasts:
    combined = pd.concat(all_forecasts, ignore_index=True)
    combined_spark = spark.createDataFrame(combined)
    write_lakehouse_table(combined_spark, silver_lakehouse_id, "raw_forecasts", mode="overwrite")
    print(f"[score] Wrote {len(combined)} raw forecast rows to silver.raw_forecasts")
else:
    print("[score] WARNING: No forecasts produced.")

print("[score] Complete.")

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
