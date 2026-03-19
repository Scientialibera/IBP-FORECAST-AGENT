# Fabric Notebook
# 10_consensus_build.py

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")

print("[consensus] Building consensus forecast.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
pdf = fc_df.toPandas()

grain_key = grain_columns + ["period_date"] if "period_date" in pdf.columns else grain_columns
numeric_cols = pdf.select_dtypes(include="number").columns.tolist()
consensus = pdf.groupby(grain_key, as_index=False)[numeric_cols].mean()
consensus["forecast_type"] = "consensus"

write_lakehouse_table(spark.createDataFrame(consensus), gold_lakehouse_id, "consensus_forecast", mode="overwrite")
print(f"[consensus] {len(consensus)} consensus rows written.")
print("[consensus] Complete.")
