# Fabric Notebook
# 02_transform_bronze.py -- Landing -> Bronze cleansing and standardization
# Phase 1: Core Capability

# @parameters
landing_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

from pyspark.sql import functions as F

source_tables = cfg("source_tables")

if not landing_lakehouse_id:
    raise ValueError("landing_lakehouse_id is required.")
if not bronze_lakehouse_id:
    raise ValueError("bronze_lakehouse_id is required.")

for table_name in source_tables:
    print(f"\n[transform] Cleaning: {table_name}")
    df = read_lakehouse_table(spark, landing_lakehouse_id, table_name)

    original_count = df.count()
    df = df.dropDuplicates()
    dedup_count = df.count()
    df = df.dropna(how="all")
    clean_count = df.count()

    df = df.withColumn("_ingested_at", F.current_timestamp())

    write_lakehouse_table(df, bronze_lakehouse_id, table_name, mode="overwrite")
    print(f"[transform] {table_name}: {original_count} -> {dedup_count} (dedup) -> {clean_count} (clean)")

print("\n[transform] Complete.")
