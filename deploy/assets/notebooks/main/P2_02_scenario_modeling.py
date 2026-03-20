# Fabric Notebook
# P2_02_scenario_modeling.py -- NCCA-only vs NCCA+imports scenario comparison
# Phase 2: Advanced Capability

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module

import pandas as pd
from pyspark.sql import functions as F

forecast_table = cfg("output_table")
scenarios_table = cfg("scenarios_table")
grain_columns = cfg("grain_columns")

enabled = True
if not enabled:
    logger.info("[scenarios] Disabled in config. Set scenarios.enabled = true to run.")
else:
    logger.info("[scenarios] Loading scenario definitions.")
    try:
        scenarios_spark = read_lakehouse_table(spark, bronze_lakehouse_id, scenarios_table)
        scenarios_pdf = scenarios_spark.toPandas()
    except Exception:
        logger.info("[scenarios] No scenario_definitions table. Creating default NCCA scenarios.")
        scenarios_pdf = pd.DataFrame([
            {"scenario_name": "ncca_only", "filter_type": "market_segment",
             "filter_value": "NCCA", "include": True},
            {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
             "filter_value": "NCCA", "include": True},
            {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
             "filter_value": "imports", "include": True},
        ])

    logger.info("[scenarios] Loading latest system forecast.")
    system_pdf = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

    if system_pdf.empty:
        logger.info("[scenarios] No system forecast. Exiting.")
    else:
        scenario_names = scenarios_pdf["scenario_name"].unique()
        logger.info(f"[scenarios] Running {len(scenario_names)} scenarios: {list(scenario_names)}")

        all_scenario_results = []

        for scenario_name in scenario_names:
            scenario_def = scenarios_pdf[scenarios_pdf["scenario_name"] == scenario_name]

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
            logger.info(f"[scenarios] {scenario_name}: {len(scenario_data)} rows, "
                       f"total={scenario_data['forecast_tons'].sum():,.0f} tons")

        if all_scenario_results:
            combined = pd.concat(all_scenario_results, ignore_index=True)
            combined_spark = spark.createDataFrame(combined)
            write_lakehouse_table(combined_spark, gold_lakehouse_id, "scenario_comparison", mode="overwrite")
            logger.info(f"[scenarios] Wrote {len(combined)} scenario rows")

            summary = combined.groupby("scenario_name").agg(
                total_tons=("forecast_tons", "sum"),
                n_rows=("forecast_tons", "count"),
            ).reset_index()
            logger.info("\n[scenarios] Scenario Summary:")
            logger.info("\n%s", summary.to_string(index=False))

logger.info("[scenarios] Complete.")
