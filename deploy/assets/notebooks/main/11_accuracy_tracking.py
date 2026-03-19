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
date_column = cfg("feature_date_column")

import pandas as pd

print("[accuracy] Loading forecast versions and actuals.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
actuals_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders").toPandas()
print(f"[accuracy] Forecast: {len(fc_df)} rows, Actuals: {len(actuals_df)} rows")

if "period" in fc_df.columns and "period" not in actuals_df.columns and "period_date" in actuals_df.columns:
    actuals_df["period"] = pd.to_datetime(actuals_df["period_date"]).dt.to_period("M").astype(str)
    actuals_agg = actuals_df.groupby(grain_columns + [date_column], as_index=False)[target_column].sum()
else:
    actuals_agg = actuals_df

if fc_df.empty or actuals_agg.empty:
    print("[accuracy] WARNING: No data for accuracy tracking.")
else:
    accuracy_df = evaluate_forecast_accuracy(
        fc_df, actuals_agg, grain_columns=grain_columns,
        period_column=date_column, forecast_col="forecast_tons",
        actual_col=target_column,
    )
    if not accuracy_df.empty:
        write_lakehouse_table(spark.createDataFrame(accuracy_df), gold_lakehouse_id,
                              accuracy_table, mode="overwrite")
        print(f"[accuracy] Wrote {len(accuracy_df)} accuracy records")
    else:
        print("[accuracy] No matching forecast-actual pairs found")
print("[accuracy] Complete.")
