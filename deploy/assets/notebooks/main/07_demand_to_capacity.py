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
grain_columns = cfg("grain_columns")

print("[capacity] Translating demand to capacity.")
translate_demand_to_capacity(
    spark, gold_lakehouse_id, bronze_lakehouse_id,
    forecast_table=forecast_table, capacity_output_table=capacity_output,
    production_table=prod_table, grain_columns=grain_columns,
    rolling_months=cfg("rolling_months"), tons_to_lf_factor=cfg("tons_to_lf_factor"),
    width_column=cfg("width_column"), speed_column=cfg("speed_column"),
    line_id_column=cfg("line_id_column"),
)
print("[capacity] Complete.")
