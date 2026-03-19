# Fabric Notebook
# P2_01_external_signals.py

# @parameters
silver_lakehouse_id = ""
bronze_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
signal_columns = cfg("signal_columns")
signals_table = cfg("signals_table")

print("[signals] Enriching feature table with external signals.")
signals_df = read_lakehouse_table(spark, bronze_lakehouse_id, signals_table).toPandas()
feature_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table").toPandas()

if "period_date" in feature_df.columns and "period_date" in signals_df.columns:
    enriched = feature_df.merge(signals_df, on="period_date", how="left")
    write_lakehouse_table(spark.createDataFrame(enriched), gold_lakehouse_id, "enriched_features", mode="overwrite")
    print(f"[signals] {len(enriched)} enriched rows written to gold")
else:
    print("[signals] WARNING: period_date not found for merge")
print("[signals] Complete.")
