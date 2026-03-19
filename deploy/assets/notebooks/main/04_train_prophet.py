# Fabric Notebook
# 04_train_prophet.py

# @parameters
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/train_prophet_module

date_column = cfg("date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
yearly = cfg("prophet_yearly_seasonality")
weekly = cfg("prophet_weekly_seasonality")
changepoint_prior = cfg("prophet_changepoint_prior")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")

print("[prophet] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[prophet] Loaded {len(pdf)} rows.")

results_df, agg_metrics = train_prophet_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, yearly_seasonality=yearly,
    weekly_seasonality=weekly, changepoint_prior=changepoint_prior,
    test_ratio=test_split_ratio, experiment_name=experiment_name,
    model_name=f"{model_prefix}_prophet", min_series_length=min_series_length,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "prophet_predictions", mode="overwrite")
print("[prophet] Complete.")
