# Fabric Notebook
# 11_accuracy_tracking.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/accuracy_module

forecast_table = cfg("output_table")
accuracy_table = cfg("accuracy_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
date_column = cfg("date_column")

print("[accuracy] Tracking forecast accuracy.")
track_accuracy(spark, gold_lakehouse_id, bronze_lakehouse_id,
               forecast_table=forecast_table, accuracy_table=accuracy_table,
               target_column=target_column, grain_columns=grain_columns,
               date_column=date_column)
print("[accuracy] Complete.")
