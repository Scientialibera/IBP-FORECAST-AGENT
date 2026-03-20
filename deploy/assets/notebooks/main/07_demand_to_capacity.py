# Fabric Notebook
# 07_demand_to_capacity.py -- Translate tons -> lineal feet -> production hours
# Phase 1: Core Capability

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module
# %run ../modules/capacity_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")
bronze_lakehouse_id = resolve_lakehouse_id(bronze_lakehouse_id, "bronze")

forecast_table = cfg("output_table")
capacity_output_table = cfg("capacity_output_table")
production_table = cfg("production_history_table")
grain_columns = cfg("grain_columns")
rolling_months = cfg("rolling_months")
tons_to_lf_factor = cfg("tons_to_lf_factor")
width_column = cfg("width_column")
speed_column = cfg("speed_column")
line_id_column = cfg("line_id_column")

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

logger.info("[capacity] Loading latest forecast from gold.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)

consensus = forecast_spark.filter(forecast_spark.version_type == "consensus")
if consensus.count() == 0:
    logger.warning("[capacity] No consensus found, using latest system forecast.")
    consensus = forecast_spark.filter(forecast_spark.version_type == "system")

forecast_pdf = consensus.toPandas()
if forecast_pdf.empty:
    logger.info("[capacity] No forecast data. Exiting.")
else:
    latest_vid = forecast_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    forecast_pdf = forecast_pdf[forecast_pdf["version_id"] == latest_vid]
    logger.info(f"[capacity] Using version {latest_vid[:8]}... ({len(forecast_pdf)} rows)")

    logger.info("[capacity] Loading production history from bronze.")
    prod_spark = read_lakehouse_table(spark, bronze_lakehouse_id, production_table)
    prod_pdf = prod_spark.toPandas()

    plant_col = "plant_id"
    sku_col = "sku_id"

    prod_avgs = compute_rolling_production_averages(
        prod_pdf, width_column=width_column, speed_column=speed_column,
        line_id_column=line_id_column, plant_column=plant_col,
        sku_column=sku_col, date_column=cfg("date_column"),
        rolling_months=rolling_months,
    )
    logger.info(f"[capacity] Computed rolling averages for {len(prod_avgs)} plant/sku/line combos")

    if "final_forecast_tons" in forecast_pdf.columns:
        forecast_pdf["forecast_tons"] = forecast_pdf["final_forecast_tons"]

    capacity_df = translate_demand_to_capacity(
        forecast_pdf, prod_avgs,
        plant_column=plant_col, sku_column=sku_col,
        line_id_column=line_id_column,
        tons_to_lf_factor=tons_to_lf_factor,
    )

    capacity_spark = spark.createDataFrame(capacity_df)
    write_lakehouse_table(capacity_spark, gold_lakehouse_id, capacity_output_table, mode="overwrite")
    logger.info(f"[capacity] Wrote {len(capacity_df)} rows to gold.{capacity_output_table}")

    plant_summary = capacity_df.groupby(plant_col).agg(
        total_tons=("forecast_tons", "sum"),
        total_lf=("lineal_feet", "sum"),
        total_hours=("production_hours", "sum"),
    ).reset_index()
    logger.info("\n[capacity] Plant Summary:")
    logger.info("\n%s", plant_summary.to_string(index=False))

logger.info("\n[capacity] Complete.")
