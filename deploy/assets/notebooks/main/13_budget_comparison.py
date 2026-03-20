# Fabric Notebook
# 13_budget_comparison.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

import pandas as pd

forecast_table = cfg("output_table")
budget_table = cfg("budget_table")
comparison_output = cfg("comparison_output_table")
over_thresh = cfg("over_forecast_threshold")
under_thresh = cfg("under_forecast_threshold")
grain_columns = cfg("grain_columns")

print("[budget] Comparing forecast vs budget.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
budget_df = read_lakehouse_table(spark, bronze_lakehouse_id, budget_table).toPandas()
print(f"[budget] Forecast: {len(fc_df)} rows ({list(fc_df.columns[:8])})")
print(f"[budget] Budget:   {len(budget_df)} rows ({list(budget_df.columns[:8])})")

fc_system = fc_df[fc_df["version_type"] == "system"].copy() if "version_type" in fc_df.columns else fc_df.copy()

if "period" in fc_system.columns and "period" not in budget_df.columns and "period_date" in budget_df.columns:
    budget_df["period"] = pd.to_datetime(budget_df["period_date"]).dt.to_period("M").astype(str)

merge_keys = [c for c in grain_columns if c in fc_system.columns and c in budget_df.columns]
date_key = "period" if "period" in fc_system.columns and "period" in budget_df.columns else (
    "period_date" if "period_date" in fc_system.columns and "period_date" in budget_df.columns else None)
if date_key:
    merge_keys.append(date_key)
print(f"[budget] Merge keys: {merge_keys}")

if merge_keys:
    merged = fc_system.merge(budget_df, on=merge_keys, how="inner", suffixes=("_fc", "_bgt"))
    print(f"[budget] Merged: {len(merged)} rows")
    fc_col = "forecast_tons" if "forecast_tons" in merged.columns else "tons"
    bgt_col = "budget_tons"
    if len(merged) > 0 and fc_col in merged.columns and bgt_col in merged.columns:
        merged["variance_pct"] = (merged[fc_col] - merged[bgt_col]) / merged[bgt_col].replace(0, float("nan"))
        merged["flag"] = merged["variance_pct"].apply(
            lambda v: "over" if v > over_thresh else ("under" if v < under_thresh else "ok"))
        write_lakehouse_table(spark.createDataFrame(merged), gold_lakehouse_id, comparison_output, mode="overwrite")
        print(f"[budget] {len(merged)} comparison rows written")
    else:
        print(f"[budget] No matching data or missing columns. fc_col={fc_col}, bgt_col={bgt_col}")
else:
    print("[budget] No common columns for merge")
print("[budget] Complete.")
