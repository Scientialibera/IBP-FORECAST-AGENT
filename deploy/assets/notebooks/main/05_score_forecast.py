# Fabric Notebook
# 05_score_forecast.py

# @parameters
silver_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/scoring_module

import pandas as pd

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
forecast_horizon = cfg("forecast_horizon")

print("[score] Loading feature table from silver.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[score] Loaded {len(pdf)} rows. Forecasting {forecast_horizon} periods ahead.")

all_forecasts = []

print("[score] Forecasting with SARIMA...")
try:
    sarima_fc = forecast_sarima_forward(pdf, date_column, grain_columns, target_column,
                                        forecast_horizon, tuple(cfg("sarima_order")), tuple(cfg("sarima_seasonal_order")))
    if not sarima_fc.empty:
        all_forecasts.append(sarima_fc)
        print(f"  SARIMA: {len(sarima_fc)} rows")
except Exception as e:
    print(f"  SARIMA failed: {e}")

print("[score] Forecasting with Prophet...")
try:
    prophet_fc = forecast_prophet_forward(pdf, date_column, grain_columns, target_column,
                                          forecast_horizon, cfg("prophet_yearly_seasonality"),
                                          cfg("prophet_weekly_seasonality"), cfg("prophet_changepoint_prior"))
    if not prophet_fc.empty:
        all_forecasts.append(prophet_fc)
        print(f"  Prophet: {len(prophet_fc)} rows")
except Exception as e:
    print(f"  Prophet failed: {e}")

print("[score] Forecasting with VAR...")
try:
    var_fc = forecast_var_forward(pdf, date_column, grain_columns, target_column,
                                  feature_columns, forecast_horizon, cfg("var_maxlags"), cfg("var_ic"))
    if not var_fc.empty:
        all_forecasts.append(var_fc)
        print(f"  VAR: {len(var_fc)} rows")
except Exception as e:
    print(f"  VAR failed: {e}")

print("[score] Forecasting with Exp Smoothing...")
try:
    ets_fc = forecast_ets_forward(pdf, date_column, grain_columns, target_column,
                                   forecast_horizon, cfg("exp_smoothing_trend"),
                                   cfg("exp_smoothing_seasonal"), cfg("exp_smoothing_seasonal_periods"))
    if not ets_fc.empty:
        all_forecasts.append(ets_fc)
        print(f"  Exp Smoothing: {len(ets_fc)} rows")
except Exception as e:
    print(f"  Exp Smoothing failed: {e}")

if all_forecasts:
    combined = pd.concat(all_forecasts, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), silver_lakehouse_id, "raw_forecasts", mode="overwrite")
    print(f"[score] Wrote {len(combined)} raw forecast rows")
else:
    print("[score] WARNING: No forecasts produced.")
print("[score] Complete.")
