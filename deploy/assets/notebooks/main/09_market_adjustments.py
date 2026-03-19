# Fabric Notebook
# 09_market_adjustments.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/override_module

forecast_table = cfg("output_table")
adj_table = cfg("adjustments_table")
scale = cfg("default_scale_factor")
grain_columns = cfg("grain_columns")

print("[market] Applying market adjustments.")
apply_market_adjustments(spark, gold_lakehouse_id, bronze_lakehouse_id,
                         forecast_table=forecast_table, adjustments_table=adj_table,
                         default_scale_factor=scale, grain_columns=grain_columns)
print("[market] Complete.")
