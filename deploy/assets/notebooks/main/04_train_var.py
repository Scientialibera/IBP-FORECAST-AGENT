# Fabric Notebook
# 04_train_var.py -- Train VAR (multivariate) per grain on Silver feature table
# Phase 1: Preferred model

# @parameters
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/train_var_module


silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
test_split_ratio = cfg("test_split_ratio")
var_maxlags = cfg("var_maxlags")
var_ic = cfg("var_ic")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = freq_params("min_train_periods")


logger.info("[var] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, cfg("feature_table"))
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
logger.info(f"[var] Loaded {len(pdf)} rows.")

results_df, agg_metrics = train_var_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, feature_columns=feature_columns,
    maxlags=var_maxlags, ic=var_ic, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_var",
    min_series_length=min_series_length,
)

if not results_df.empty:
    preds_spark = spark.createDataFrame(results_df)
    write_lakehouse_table(preds_spark, silver_lakehouse_id, cfg("prediction_tables")[2], mode="overwrite")

logger.info("[var] Complete.")
