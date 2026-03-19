# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# METADATA ********************

# CELL ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 07_demand_to_capacity.py -- Translate tons -> lineal feet -> production hours
# Phase 1: Core Capability

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run versioning_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run capacity_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
capacity_output_table = params.get("capacity_output_table") or "capacity_translation"
production_table = params.get("production_history_table") or "production_history"
grain_columns = parse_list_param(params["grain_columns"])
rolling_months = int(params.get("rolling_months") or 3)
tons_to_lf_factor = float(params.get("tons_to_lf_factor") or 2000)
width_column = params.get("width_column") or "width_inches"
speed_column = params.get("speed_column") or "line_speed_fpm"
line_id_column = params.get("line_id_column") or "line_id"

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

# Get latest consensus or system forecast
print("[capacity] Loading latest forecast from gold.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)

# Prefer consensus, fall back to system
consensus = forecast_spark.filter(forecast_spark.version_type == "consensus")
if consensus.count() == 0:
    print("[capacity] No consensus found, using latest system forecast.")
    consensus = forecast_spark.filter(forecast_spark.version_type == "system")

forecast_pdf = consensus.toPandas()
if forecast_pdf.empty:
    print("[capacity] No forecast data. Exiting.")
else:
    # Get latest version
    latest_vid = forecast_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    forecast_pdf = forecast_pdf[forecast_pdf["version_id"] == latest_vid]
    print(f"[capacity] Using version {latest_vid[:8]}... ({len(forecast_pdf)} rows)")

    # Load production history for rolling averages
    print("[capacity] Loading production history from bronze.")
    prod_spark = read_lakehouse_table(spark, bronze_lakehouse_id, production_table)
    prod_pdf = prod_spark.toPandas()

    plant_col = "plant_id"
    sku_col = "sku_id"

    prod_avgs = compute_rolling_production_averages(
        prod_pdf, width_column=width_column, speed_column=speed_column,
        line_id_column=line_id_column, plant_column=plant_col,
        sku_column=sku_col, date_column="period_date",
        rolling_months=rolling_months,
    )
    print(f"[capacity] Computed rolling averages for {len(prod_avgs)} plant/sku/line combos")

    # Use final_forecast_tons if available, else forecast_tons
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
    print(f"[capacity] Wrote {len(capacity_df)} rows to gold.{capacity_output_table}")

    # Summary by plant
    plant_summary = capacity_df.groupby(plant_col).agg(
        total_tons=("forecast_tons", "sum"),
        total_lf=("lineal_feet", "sum"),
        total_hours=("production_hours", "sum"),
    ).reset_index()
    print("\n[capacity] Plant Summary:")
    print(plant_summary.to_string(index=False))

print("\n[capacity] Complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
