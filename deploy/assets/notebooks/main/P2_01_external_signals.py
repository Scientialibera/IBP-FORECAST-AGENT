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

# P2_01_external_signals.py -- Ingest and correlate external market signals
# Phase 2: Advanced Capability

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

import pandas as pd
import numpy as np

params = get_notebook_params()

silver_lakehouse_id = params["silver_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
gold_lakehouse_id = params["gold_lakehouse_id"]
target_column = params["target_column"]
grain_columns = parse_list_param(params["grain_columns"])
signal_columns = parse_list_param(params.get("signal_columns") or '["construction_index","interest_rate","inflation_rate","tariff_rate"]')
signals_table = params.get("signals_table") or "external_signals"

enabled = str(params.get("external_signals_enabled") or "false").lower() == "true"
if not enabled:
    print("[external_signals] Disabled in config. Set external_signals.enabled = true to run.")
else:
    print("[external_signals] Loading external signals from bronze.")
    signals_spark = read_lakehouse_table(spark, bronze_lakehouse_id, signals_table)
    signals_pdf = signals_spark.toPandas()
    print(f"[external_signals] Loaded {len(signals_pdf)} signal rows")

    # Load feature table
    feature_spark = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
    feature_pdf = feature_spark.toPandas()

    # Merge signals with feature table on period
    if "period" in signals_pdf.columns:
        signals_pdf["period"] = pd.to_datetime(signals_pdf["period"])
        feature_pdf["period"] = pd.to_datetime(feature_pdf["period"])
        enriched = feature_pdf.merge(signals_pdf, on="period", how="left")
    else:
        enriched = feature_pdf.copy()
        print("[external_signals] WARNING: No 'period' column in signals table")

    # Compute correlation / feature importance per grain
    print("[external_signals] Computing signal correlations per grain...")
    importance_records = []

    for grain_key, group in enriched.groupby(grain_columns):
        if isinstance(grain_key, str):
            grain_key = (grain_key,)

        for signal in signal_columns:
            if signal not in group.columns:
                continue
            valid = group[[target_column, signal]].dropna()
            if len(valid) < 12:
                continue
            corr = valid[target_column].corr(valid[signal])
            record = {"signal": signal, "correlation": corr, "n_obs": len(valid)}
            for k, col in enumerate(grain_columns):
                record[col] = grain_key[k] if k < len(grain_key) else ""
            importance_records.append(record)

    if importance_records:
        imp_df = pd.DataFrame(importance_records)
        imp_df["abs_correlation"] = imp_df["correlation"].abs()
        imp_df = imp_df.sort_values("abs_correlation", ascending=False)

        imp_spark = spark.createDataFrame(imp_df)
        write_lakehouse_table(imp_spark, gold_lakehouse_id, "signal_importance", mode="overwrite")
        print(f"[external_signals] Wrote {len(imp_df)} importance rows")

        # Top signals summary
        print("\n[external_signals] Top 10 signal-grain correlations:")
        print(imp_df.head(10)[["signal"] + grain_columns + ["correlation"]].to_string(index=False))

    # Write enriched feature table for potential model use
    enriched_spark = spark.createDataFrame(enriched)
    write_lakehouse_table(enriched_spark, silver_lakehouse_id, "feature_table_enriched", mode="overwrite")
    print(f"[external_signals] Wrote enriched feature table ({len(enriched)} rows)")

print("[external_signals] Complete.")

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
