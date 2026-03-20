# Fabric Notebook
# 04_train_sarima.py -- Train SARIMA per grain on Silver feature table
# Phase 1: Required model

# @parameters
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/train_sarima_module

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
sarima_order = tuple(cfg("sarima_order"))
sarima_seasonal_order = tuple(cfg("sarima_seasonal_order"))
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")

if not silver_lakehouse_id:
    raise ValueError("silver_lakehouse_id is required.")

logger.info("[sarima] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
logger.info(f"[sarima] Loaded {len(pdf)} rows.")

results_df, agg_metrics = train_sarima_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, order=sarima_order,
    seasonal_order=sarima_seasonal_order, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_sarima",
    min_series_length=min_series_length,
)

if not results_df.empty:
    preds_spark = spark.createDataFrame(results_df)
    write_lakehouse_table(preds_spark, silver_lakehouse_id, "sarima_predictions", mode="overwrite")

logger.info("[sarima] Complete.")
