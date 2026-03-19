# Fabric Notebook
# 03_feature_engineering.py

# @parameters
bronze_lakehouse_id = ""
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/feature_engineering_module

date_column = cfg("date_column")
frequency = cfg("frequency")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
source_tables = cfg("source_tables")

print(f"[features] Bronze → Silver feature table")
orders_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders")
pdf = orders_df.toPandas()
print(f"[features] Loaded {len(pdf)} order rows")

feature_df = build_feature_table(
    pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, feature_columns=feature_columns, frequency=frequency,
)
print(f"[features] Feature table: {len(feature_df)} rows")

feature_spark = spark.createDataFrame(feature_df)
write_lakehouse_table(feature_spark, silver_lakehouse_id, "feature_table", mode="overwrite")
print("[features] Complete.")
