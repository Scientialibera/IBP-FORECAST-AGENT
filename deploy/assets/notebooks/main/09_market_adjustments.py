# Fabric Notebook
# 09_market_adjustments.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/override_module
# %run ../modules/versioning_module

forecast_table = cfg("output_table")
adj_table = cfg("adjustments_table")
scale = cfg("default_scale_factor")

import pandas as pd

print("[market] Loading forecast and market adjustments.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
adj_df = read_lakehouse_table(spark, bronze_lakehouse_id, adj_table).toPandas()
print(f"[market] Forecast: {len(fc_df)} rows, Adjustments: {len(adj_df)} rows")

period_col = "period" if "period" in fc_df.columns else "period_date"
if period_col == "period" and "period" not in adj_df.columns and "period_date" in adj_df.columns:
    adj_df["period"] = pd.to_datetime(adj_df["period_date"]).dt.to_period("M").astype(str)
result = apply_market_adjustments(fc_df, adj_df, market_column="market_id",
                                  period_column=period_col, default_factor=scale)

versioned, vid = stamp_forecast_version(result, version_type="market_adjusted")
append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned)
print(f"[market] Wrote {len(versioned)} adjusted rows (version {vid})")
print("[market] Complete.")
