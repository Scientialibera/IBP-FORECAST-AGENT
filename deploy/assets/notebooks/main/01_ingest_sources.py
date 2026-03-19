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

# 01_ingest_sources.py -- Ingest source tables into Landing lakehouse
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

params = get_notebook_params()

source_lakehouse_id = params["source_lakehouse_id"]
landing_lakehouse_id = params["landing_lakehouse_id"]
source_tables = parse_list_param(params["source_tables"])

if not source_lakehouse_id:
    raise ValueError("source_lakehouse_id is required.")
if not landing_lakehouse_id:
    raise ValueError("landing_lakehouse_id is required.")
if not source_tables:
    raise ValueError("source_tables list is empty.")

print(f"[ingest] Source lakehouse: {source_lakehouse_id}")
print(f"[ingest] Landing lakehouse: {landing_lakehouse_id}")
print(f"[ingest] Tables to ingest: {source_tables}")

for table_name in source_tables:
    print(f"\n[ingest] Reading: {table_name}")
    df = read_lakehouse_table(spark, source_lakehouse_id, table_name)
    row_count = df.count()
    print(f"[ingest] {table_name}: {row_count} rows")
    write_lakehouse_table(df, landing_lakehouse_id, table_name, mode="overwrite")
    print(f"[ingest] Wrote {table_name} to landing.")

print("\n[ingest] Complete.")

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
