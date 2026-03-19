# Fabric Notebook
# P2_02_scenario_modeling.py -- NCCA-only vs NCCA+imports scenario comparison
# Phase 2: Advanced Capability

# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module

import pandas as pd
from pyspark.sql import functions as F

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
scenarios_table = params.get("scenarios_table") or "scenario_definitions"
grain_columns = parse_list_param(params["grain_columns"])

enabled = str(params.get("scenarios_enabled") or "false").lower() == "true"
if not enabled:
    print("[scenarios] Disabled in config. Set scenarios.enabled = true to run.")
else:
    # Load scenario definitions
    print("[scenarios] Loading scenario definitions.")
    try:
        scenarios_spark = read_lakehouse_table(spark, bronze_lakehouse_id, scenarios_table)
        scenarios_pdf = scenarios_spark.toPandas()
    except Exception:
        print("[scenarios] No scenario_definitions table. Creating default NCCA scenarios.")
        scenarios_pdf = pd.DataFrame([
            {"scenario_name": "ncca_only", "filter_type": "market_segment",
             "filter_value": "NCCA", "include": True},
            {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
             "filter_value": "NCCA", "include": True},
            {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
             "filter_value": "imports", "include": True},
        ])

    # Load latest system forecast
    print("[scenarios] Loading latest system forecast.")
    system_pdf = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

    if system_pdf.empty:
        print("[scenarios] No system forecast. Exiting.")
    else:
        scenario_names = scenarios_pdf["scenario_name"].unique()
        print(f"[scenarios] Running {len(scenario_names)} scenarios: {list(scenario_names)}")

        all_scenario_results = []

        for scenario_name in scenario_names:
            scenario_def = scenarios_pdf[scenarios_pdf["scenario_name"] == scenario_name]

            # Apply filters to get scenario subset
            scenario_data = system_pdf.copy()
            for _, rule in scenario_def.iterrows():
                filter_col = rule.get("filter_type", "market_segment")
                filter_val = rule.get("filter_value", "")
                if filter_col in scenario_data.columns and filter_val:
                    include = rule.get("include", True)
                    if include:
                        scenario_data = scenario_data[scenario_data[filter_col] == filter_val]
                    else:
                        scenario_data = scenario_data[scenario_data[filter_col] != filter_val]

            scenario_data["scenario_name"] = scenario_name
            all_scenario_results.append(scenario_data)
            print(f"[scenarios] {scenario_name}: {len(scenario_data)} rows, "
                  f"total={scenario_data['forecast_tons'].sum():,.0f} tons")

        if all_scenario_results:
            combined = pd.concat(all_scenario_results, ignore_index=True)
            combined_spark = spark.createDataFrame(combined)
            write_lakehouse_table(combined_spark, gold_lakehouse_id, "scenario_comparison", mode="overwrite")
            print(f"[scenarios] Wrote {len(combined)} scenario rows")

            # Summary comparison
            summary = combined.groupby("scenario_name").agg(
                total_tons=("forecast_tons", "sum"),
                n_rows=("forecast_tons", "count"),
            ).reset_index()
            print("\n[scenarios] Scenario Summary:")
            print(summary.to_string(index=False))

print("[scenarios] Complete.")
