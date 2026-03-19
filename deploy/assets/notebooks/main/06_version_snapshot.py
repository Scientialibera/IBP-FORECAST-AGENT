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
create_forecast_snapshot(spark, silver_lakehouse_id, gold_lakehouse_id,
                         output_table=output_table, keep_n=keep_n)
print("[version] Complete.")
