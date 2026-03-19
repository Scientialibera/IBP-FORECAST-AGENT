# Fabric Notebook
# 12_aggregate_gold.py

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

forecast_table = cfg("output_table")
hierarchy = cfg("hierarchy_levels")

print(f"[aggregate] Aggregating gold.{forecast_table} across {hierarchy}")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
pdf = fc_df.toPandas()

numeric_cols = pdf.select_dtypes(include="number").columns.tolist()
agg_frames = []
for level in hierarchy:
    if level in pdf.columns:
        agg = pdf.groupby(level, as_index=False)[numeric_cols].sum()
        agg["hierarchy_level"] = level
        agg_frames.append(agg)

if agg_frames:
    import pandas as pd
    combined = pd.concat(agg_frames, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), gold_lakehouse_id, "aggregated_forecast", mode="overwrite")
    print(f"[aggregate] {len(combined)} aggregated rows")
print("[aggregate] Complete.")
