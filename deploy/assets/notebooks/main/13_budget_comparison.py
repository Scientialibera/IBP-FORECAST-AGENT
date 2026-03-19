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
hierarchy = cfg("hierarchy_levels")

print("[budget] Comparing forecast vs budget.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
budget_df = read_lakehouse_table(spark, bronze_lakehouse_id, budget_table).toPandas()

common = list(set(fc_df.columns) & set(budget_df.columns) - {"tons", "budget_tons"})
if common:
    merged = fc_df.merge(budget_df, on=common, how="inner", suffixes=("_fc", "_bgt"))
    if "tons" in merged.columns and "budget_tons" in merged.columns:
        merged["variance_pct"] = (merged["tons"] - merged["budget_tons"]) / merged["budget_tons"].replace(0, float("nan"))
        merged["flag"] = merged["variance_pct"].apply(
            lambda v: "over" if v > over_thresh else ("under" if v < under_thresh else "ok"))
        write_lakehouse_table(spark.createDataFrame(merged), gold_lakehouse_id, comparison_output, mode="overwrite")
        print(f"[budget] {len(merged)} comparison rows")
    else:
        print("[budget] Missing tons/budget_tons columns for comparison")
else:
    print("[budget] No common columns for merge")
print("[budget] Complete.")
