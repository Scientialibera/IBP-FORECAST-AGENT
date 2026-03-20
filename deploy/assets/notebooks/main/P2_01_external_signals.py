# Fabric Notebook
# P2_01_external_signals.py -- Ingest and correlate external market signals
# Phase 2: Advanced Capability

# @parameters
silver_lakehouse_id = ""
bronze_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

import pandas as pd
import numpy as np

target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
signal_columns = cfg("signal_columns")
signals_table = cfg("signals_table")

enabled = True
if not enabled:
    logger.info("[external_signals] Disabled in config. Set external_signals.enabled = true to run.")
else:
    logger.info("[external_signals] Loading external signals from bronze.")
    signals_spark = read_lakehouse_table(spark, bronze_lakehouse_id, signals_table)
    signals_pdf = signals_spark.toPandas()
    logger.info(f"[external_signals] Loaded {len(signals_pdf)} signal rows")

    feature_spark = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
    feature_pdf = feature_spark.toPandas()

    if "period" in signals_pdf.columns:
        signals_pdf["period"] = pd.to_datetime(signals_pdf["period"])
        feature_pdf["period"] = pd.to_datetime(feature_pdf["period"])
        enriched = feature_pdf.merge(signals_pdf, on="period", how="left")
    else:
        enriched = feature_pdf.copy()
        logger.warning("[external_signals] WARNING: No 'period' column in signals table")

    logger.info("[external_signals] Computing signal correlations per grain...")
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
        logger.info(f"[external_signals] Wrote {len(imp_df)} importance rows")

        logger.info("\n[external_signals] Top 10 signal-grain correlations:")
        logger.info("\n%s", imp_df.head(10)[["signal"] + grain_columns + ["correlation"]].to_string(index=False))

    enriched_spark = spark.createDataFrame(enriched)
    write_lakehouse_table(enriched_spark, silver_lakehouse_id, "feature_table_enriched", mode="overwrite")
    logger.info(f"[external_signals] Wrote enriched feature table ({len(enriched)} rows)")

logger.info("[external_signals] Complete.")
