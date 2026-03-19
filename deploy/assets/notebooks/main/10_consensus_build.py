# Fabric Notebook
# 10_consensus_build.py

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/override_module

forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")

print("[consensus] Building consensus forecast.")
all_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
print(f"[consensus] Total versioned rows: {len(all_df)}")

if all_df.empty:
    print("[consensus] WARNING: No forecast data.")
else:
    system_df = all_df[all_df["version_type"] == "system"] if "version_type" in all_df.columns else all_df
    sales_df = all_df[all_df["version_type"] == "sales_override"] if "version_type" in all_df.columns else None
    market_df = all_df[all_df["version_type"] == "market_adjusted"] if "version_type" in all_df.columns else None

    period_col = "period" if "period" in system_df.columns else "period_date"
    consensus = build_consensus(system_df, sales_df, market_df,
                                grain_columns=grain_columns, period_column=period_col)
    consensus["forecast_type"] = "consensus"

    write_lakehouse_table(spark.createDataFrame(consensus), gold_lakehouse_id, "consensus_forecast", mode="overwrite")
    print(f"[consensus] {len(consensus)} consensus rows written.")
print("[consensus] Complete.")
