# Fabric Notebook
# 04_train_exp_smoothing.py -- Train Holt-Winters per grain on Silver feature table
# Phase 1: Preferred model

# @parameters
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/train_exp_smoothing_module


silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
trend = cfg("exp_smoothing_trend")
seasonal = cfg("exp_smoothing_seasonal")
seasonal_periods = freq_params("seasonal_periods")
experiment_name = named(cfg("experiment_name"))
model_prefix = cfg("registered_model_prefix")
min_series_length = freq_params("min_train_periods")


logger.info("[exp_smoothing] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, cfg("feature_table"))
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
logger.info(f"[exp_smoothing] Loaded {len(pdf)} rows.")

results_df, agg_metrics = train_exp_smoothing_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, trend=trend, seasonal=seasonal,
    seasonal_periods=seasonal_periods, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_exp_smoothing",
    min_series_length=min_series_length,
)

if not results_df.empty:
    preds_spark = spark.createDataFrame(results_df)
    write_lakehouse_table(preds_spark, silver_lakehouse_id, prediction_table_for("exp_smoothing"), mode="overwrite")

logger.info("[exp_smoothing] Complete.")
