# Fabric Notebook
# 08_sales_overrides.py

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
overrides_table = cfg("overrides_table")
grain_columns = cfg("grain_columns")

import pandas as pd

print("[overrides] Loading forecast and overrides data.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
overrides_df = read_lakehouse_table(spark, bronze_lakehouse_id, overrides_table).toPandas()
print(f"[overrides] Forecast: {len(fc_df)} rows, Overrides: {len(overrides_df)} rows")

period_col = "period" if "period" in fc_df.columns else "period_date"
if period_col == "period" and "period" not in overrides_df.columns and "period_date" in overrides_df.columns:
    overrides_df["period"] = pd.to_datetime(overrides_df["period_date"]).dt.to_period("M").astype(str)
result = apply_sales_overrides(fc_df, overrides_df, grain_columns=grain_columns, period_column=period_col)

versioned, vid = stamp_forecast_version(result, version_type="sales_override")
append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned)
print(f"[overrides] Wrote {len(versioned)} override rows (version {vid})")
print("[overrides] Complete.")
