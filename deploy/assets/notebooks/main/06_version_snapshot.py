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

# 06_version_snapshot.py -- Stamp forecasts with version IDs and snapshot month
# Phase 1: Core Capability -- Forecast versioning

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

params = get_notebook_params()

silver_lakehouse_id = params["silver_lakehouse_id"]
gold_lakehouse_id = params["gold_lakehouse_id"]
output_table = params.get("output_table") or "forecast_versions"
keep_n_snapshots = int(params.get("keep_n_snapshots") or 24)

if not silver_lakehouse_id or not gold_lakehouse_id:
    raise ValueError("silver_lakehouse_id and gold_lakehouse_id are required.")

print("[version] Loading raw forecasts from silver.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "raw_forecasts")
raw_pdf = spark_df.toPandas()

if raw_pdf.empty:
    print("[version] No raw forecasts to version. Exiting.")
else:
    model_types = raw_pdf["model_type"].unique()
    print(f"[version] Models to version: {list(model_types)}")

    for model in model_types:
        model_df = raw_pdf[raw_pdf["model_type"] == model].copy()
        versioned_df, vid = stamp_forecast_version(
            model_df, version_type="system", model_type=model
        )
        append_versioned_forecast(spark, gold_lakehouse_id, output_table, versioned_df)
        print(f"[version] {model}: versioned {len(versioned_df)} rows, version_id={vid[:8]}...")

    # Purge old snapshots if configured
    purge_old_snapshots(spark, gold_lakehouse_id, output_table, keep_n=keep_n_snapshots)

print("[version] Complete.")

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
