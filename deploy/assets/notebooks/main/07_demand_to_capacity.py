# Fabric Notebook
# 07_demand_to_capacity.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/capacity_module

forecast_table = cfg("output_table")
capacity_output = cfg("capacity_output_table")
prod_table = cfg("production_history_table")
width_col = cfg("width_column")
speed_col = cfg("speed_column")
line_id_col = cfg("line_id_column")
rolling_months = cfg("rolling_months")
tons_to_lf = cfg("tons_to_lf_factor")

print("[capacity] Loading forecast and production data.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
prod_df = read_lakehouse_table(spark, bronze_lakehouse_id, prod_table).toPandas()
print(f"[capacity] Forecast: {len(fc_df)} rows, Production: {len(prod_df)} rows")

prod_avgs = compute_rolling_production_averages(
    prod_df, width_column=width_col, speed_column=speed_col,
    line_id_column=line_id_col, plant_column="plant_id",
    sku_column="sku_id", date_column="period_date",
    rolling_months=rolling_months,
)
print(f"[capacity] Computed rolling averages: {len(prod_avgs)} rows")

capacity_df = translate_demand_to_capacity(
    fc_df, prod_avgs,
    plant_column="plant_id", sku_column="sku_id",
    line_id_column=line_id_col, tons_to_lf_factor=tons_to_lf,
)
write_lakehouse_table(spark.createDataFrame(capacity_df), gold_lakehouse_id, capacity_output, mode="overwrite")
print(f"[capacity] Wrote {len(capacity_df)} capacity rows")
print("[capacity] Complete.")
