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

if "period" in fc_df.columns and "period" not in budget_df.columns and "period_date" in budget_df.columns:
    budget_df["period"] = pd.to_datetime(budget_df["period_date"]).dt.to_period("M").astype(str)

merge_keys = [c for c in grain_columns + ["period"] if c in fc_df.columns and c in budget_df.columns]
if not merge_keys:
    merge_keys = [c for c in grain_columns + ["period_date"] if c in fc_df.columns and c in budget_df.columns]

if merge_keys:
    fc_agg = fc_df[fc_df.get("version_type", "system") == "system"] if "version_type" in fc_df.columns else fc_df
    merged = fc_agg.merge(budget_df, on=merge_keys, how="inner", suffixes=("_fc", "_bgt"))
    fc_col = "forecast_tons" if "forecast_tons" in merged.columns else "tons"
    bgt_col = "budget_tons"
    if fc_col in merged.columns and bgt_col in merged.columns:
        merged["variance_pct"] = (merged[fc_col] - merged[bgt_col]) / merged[bgt_col].replace(0, float("nan"))
        merged["flag"] = merged["variance_pct"].apply(
            lambda v: "over" if v > over_thresh else ("under" if v < under_thresh else "ok"))
        write_lakehouse_table(spark.createDataFrame(merged), gold_lakehouse_id, comparison_output, mode="overwrite")
        print(f"[budget] {len(merged)} comparison rows")
    else:
        print(f"[budget] Missing forecast/budget cols. Have: {list(merged.columns[:15])}")
else:
    print("[budget] No common columns for merge")
print("[budget] Complete.")
