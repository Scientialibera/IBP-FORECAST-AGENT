# Fabric Notebook
# 06_version_snapshot.py

# @parameters
silver_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module

output_table = cfg("output_table")
keep_n = cfg("keep_n_snapshots")

print(f"[version] Creating snapshot in gold.{output_table}")
raw_df = read_lakehouse_table(spark, silver_lakehouse_id, "raw_forecasts").toPandas()
print(f"[version] Read {len(raw_df)} raw forecast rows from silver")

if raw_df.empty:
    print("[version] WARNING: No raw forecasts to snapshot.")
else:
    versioned, vid = stamp_forecast_version(raw_df, version_type="system")
    print(f"[version] Stamped version {vid}, {len(versioned)} rows")
    append_versioned_forecast(spark, gold_lakehouse_id, output_table, versioned)
    purge_old_snapshots(spark, gold_lakehouse_id, output_table, keep_n=keep_n)
print("[version] Complete.")
