# Fabric Notebook
# 04_train_var.py

# @parameters
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/tuning_module
# %run ../modules/train_var_module

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
test_split_ratio = cfg("test_split_ratio")
maxlags = cfg("var_maxlags")
ic = cfg("var_ic")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")
tuning_enabled = cfg("tuning_enabled")
tuning_n_iter = cfg("tuning_n_iter")
tuning_n_splits = cfg("tuning_n_splits")
tuning_metric = cfg("tuning_metric")

print("[var] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[var] Loaded {len(pdf)} rows. Tuning: {tuning_enabled}")

results_df, agg_metrics = train_var_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, feature_columns=feature_columns,
    maxlags=maxlags, ic=ic, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_var",
    min_series_length=min_series_length,
    tuning_enabled=tuning_enabled, tuning_n_iter=tuning_n_iter,
    tuning_n_splits=tuning_n_splits, tuning_metric=tuning_metric,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "var_predictions", mode="overwrite")
print("[var] Complete.")
