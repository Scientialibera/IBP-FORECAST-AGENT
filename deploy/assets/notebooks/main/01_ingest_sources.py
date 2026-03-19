# Fabric Notebook
# 01_ingest_sources.py

# @parameters
source_lakehouse_id = ""
landing_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

source_tables = cfg("source_tables")

print(f"[ingest] Source: {source_lakehouse_id} → Landing: {landing_lakehouse_id}")
print(f"[ingest] Tables: {source_tables}")

for table_name in source_tables:
    print(f"  Reading: {table_name}")
    df = read_lakehouse_table(spark, source_lakehouse_id, table_name)
    row_count = df.count()
    print(f"  {table_name}: {row_count} rows")
    write_lakehouse_table(df, landing_lakehouse_id, table_name, mode="overwrite")

print("[ingest] Complete.")
