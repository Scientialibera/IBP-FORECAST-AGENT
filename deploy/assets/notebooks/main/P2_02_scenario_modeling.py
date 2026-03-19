# Fabric Notebook
# P2_02_scenario_modeling.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

import pandas as pd

forecast_table = cfg("output_table")
scenarios_table = cfg("scenarios_table")
grain_columns = cfg("grain_columns")

print("[scenarios] Applying scenario multipliers.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
sc_df = read_lakehouse_table(spark, bronze_lakehouse_id, scenarios_table).toPandas()

scenario_results = []
for _, scenario in sc_df.iterrows():
    s = fc_df.copy()
    s["scenario_id"] = scenario.get("scenario_id", "unknown")
    s["scenario_name"] = scenario.get("scenario_name", "")
    vol_mult = float(scenario.get("volume_multiplier", 1.0))
    if "tons" in s.columns:
        s["tons"] = s["tons"] * vol_mult
    scenario_results.append(s)

if scenario_results:
    combined = pd.concat(scenario_results, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), gold_lakehouse_id, "scenario_forecasts", mode="overwrite")
    print(f"[scenarios] {len(combined)} scenario forecast rows")
print("[scenarios] Complete.")
