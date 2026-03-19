# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# METADATA ********************

# CELL ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 02_transform_bronze.py -- Landing -> Bronze cleansing and standardization
# Phase 1: Core Capability

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

params = get_notebook_params()

landing_lakehouse_id = params["landing_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
source_tables = parse_list_param(params["source_tables"])

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
