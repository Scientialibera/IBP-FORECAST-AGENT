# Fabric Notebook
# 03_feature_engineering.py -- Bronze -> Silver feature table
# Phase 1: Core Capability

# @parameters
bronze_lakehouse_id = ""
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/feature_engineering_module

date_column = cfg("date_column")
frequency = cfg("frequency") or "M"
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
source_tables = cfg("source_tables")

if not bronze_lakehouse_id or not silver_lakehouse_id:
    raise ValueError("bronze_lakehouse_id and silver_lakehouse_id are required.")

print(f"[feature_eng] Grain: {grain_columns}, Target: {target_column}")
print(f"[feature_eng] Features: {feature_columns}, Frequency: {frequency}")

primary_table = source_tables[0] if source_tables else "orders"
print(f"[feature_eng] Reading primary table: {primary_table}")
spark_df = read_lakehouse_table(spark, bronze_lakehouse_id, primary_table)

available = set(spark_df.columns)
missing = [c for c in [date_column, target_column] + grain_columns + feature_columns if c not in available]
if missing:
    print(f"[feature_eng] WARNING: Missing columns {missing}")
    feature_columns = [c for c in feature_columns if c in available]

pdf = spark_df.toPandas()
print(f"[feature_eng] Loaded {len(pdf)} rows from bronze.")

feature_df = build_feature_table(pdf, date_column, grain_columns, target_column, feature_columns, frequency)
print(f"[feature_eng] Feature table: {len(feature_df)} rows, {len(feature_df.columns)} columns")

feature_spark = spark.createDataFrame(feature_df)
write_lakehouse_table(feature_spark, silver_lakehouse_id, "feature_table", mode="overwrite")

print("[feature_eng] Complete.")
