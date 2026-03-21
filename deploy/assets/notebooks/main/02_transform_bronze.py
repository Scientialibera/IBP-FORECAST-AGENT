# Fabric Notebook
# 02_transform_bronze.py -- Landing -> Bronze cleansing and standardization
# Phase 1: Core Capability

# %run ../modules/ibp_config
# %run ../modules/config_module


landing_lakehouse_id = resolve_lakehouse_id("", "landing")
bronze_lakehouse_id = resolve_lakehouse_id("", "bronze")

from pyspark.sql import functions as F

source_tables = cfg("source_tables")


for table_name in source_tables:
    logger.info(f"\n[transform] Cleaning: {table_name}")
    df = read_lakehouse_table(spark, landing_lakehouse_id, table_name)

    original_count = df.count()
    df = df.dropDuplicates()
    dedup_count = df.count()
    df = df.dropna(how="all")
    clean_count = df.count()

    df = df.withColumn("_ingested_at", F.current_timestamp())

    write_lakehouse_table(df, bronze_lakehouse_id, table_name, mode="overwrite")
    logger.info(f"[transform] {table_name}: {original_count} -> {dedup_count} (dedup) -> {clean_count} (clean)")

logger.info("\n[transform] Complete.")
