# Fabric Notebook
# 08_sales_overrides.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/override_module

forecast_table = cfg("output_table")
overrides_table = cfg("overrides_table")
grain_columns = cfg("grain_columns")

print("[overrides] Applying sales overrides.")
apply_sales_overrides(spark, gold_lakehouse_id, bronze_lakehouse_id,
                      forecast_table=forecast_table, overrides_table=overrides_table,
                      grain_columns=grain_columns)
print("[overrides] Complete.")
