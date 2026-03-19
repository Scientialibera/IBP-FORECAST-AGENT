# Fabric Notebook
# 02_transform_bronze.py

# @parameters
landing_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

source_tables = cfg("source_tables")

print(f"[bronze] Landing → Bronze")
for table_name in source_tables:
    print(f"  Processing: {table_name}")
    df = read_lakehouse_table(spark, landing_lakehouse_id, table_name)
    df_clean = df.dropDuplicates().dropna(how="all")
    row_count = df_clean.count()
    write_lakehouse_table(df_clean, bronze_lakehouse_id, table_name, mode="overwrite")
    print(f"  {table_name}: {row_count} rows written to bronze")

print("[bronze] Complete.")
