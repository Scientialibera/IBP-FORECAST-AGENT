# Fabric Notebook
# 04_train_sarima.py -- Train SARIMA per grain on Silver feature table
# Phase 1: Required model

# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/train_sarima_module

params = get_notebook_params()

silver_lakehouse_id = params["silver_lakehouse_id"]
date_column = params["date_column"]
target_column = params["target_column"]
grain_columns = parse_list_param(params["grain_columns"])
test_split_ratio = float(params.get("test_split_ratio") or 0.2)
sarima_order = tuple(parse_int_list_param(params.get("sarima_order") or "[1,1,1]"))
sarima_seasonal_order = tuple(parse_int_list_param(params.get("sarima_seasonal_order") or "[1,1,1,12]"))
experiment_name = params.get("experiment_name") or "ibp_demand_forecast"
model_prefix = params.get("registered_model_prefix") or "ibp_model"
min_series_length = int(params.get("min_series_length") or 24)

if not silver_lakehouse_id:
    raise ValueError("silver_lakehouse_id is required.")

print("[sarima] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[sarima] Loaded {len(pdf)} rows.")

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

print("[sarima] Complete.")
