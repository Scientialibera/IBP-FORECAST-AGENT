# Fabric Notebook
# P2_03_sku_classification.py -- ABC/XYZ and runner/repeater/stranger classification
# Phase 2: Advanced Capability

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module


silver_lakehouse_id = resolve_lakehouse_id("", "silver")
gold_lakehouse_id = resolve_lakehouse_id("", "gold")

import pandas as pd
import numpy as np

target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
sku_output_table = cfg("sku_classification_output_table")

runner_threshold = float(cfg("runner_threshold"))
repeater_threshold = float(cfg("repeater_threshold"))

xyz_x_threshold = float(cfg("xyz_cv_threshold_x"))
xyz_y_threshold = float(cfg("xyz_cv_threshold_y"))

enabled = cfg("sku_classification_enabled")
if not enabled:
    logger.info("[sku_class] Disabled in config. Set sku_classification.enabled = true to run.")
else:
    logger.info("[sku_class] Loading feature table.")
    feature_spark = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
    feature_pdf = feature_spark.toPandas()

    sku_col = cfg("grain_columns")[1]

    logger.info("[sku_class] Computing ABC classification...")
    sku_volume = feature_pdf.groupby(sku_col)[target_column].sum().reset_index()
    sku_volume.columns = [sku_col, "total_volume"]
    sku_volume = sku_volume.sort_values("total_volume", ascending=False)
    sku_volume["cumulative_pct"] = sku_volume["total_volume"].cumsum() / sku_volume["total_volume"].sum()

    def classify_abc(pct):
        if pct <= runner_threshold:
            return "A"
        elif pct <= repeater_threshold:
            return "B"
        return "C"

    sku_volume["abc_class"] = sku_volume["cumulative_pct"].apply(classify_abc)

    sku_volume["rrs_class"] = sku_volume["abc_class"].map(
        {"A": "runner", "B": "repeater", "C": "stranger"}
    )

    logger.info("[sku_class] Computing XYZ classification...")
    sku_cv = feature_pdf.groupby(sku_col)[target_column].agg(["mean", "std"]).reset_index()
    sku_cv.columns = [sku_col, "demand_mean", "demand_std"]
    sku_cv["cv"] = sku_cv["demand_std"] / sku_cv["demand_mean"].replace(0, np.nan)
    sku_cv["cv"] = sku_cv["cv"].fillna(999)

    def classify_xyz(cv):
        if cv <= xyz_x_threshold:
            return "X"
        elif cv <= xyz_y_threshold:
            return "Y"
        return "Z"

    sku_cv["xyz_class"] = sku_cv["cv"].apply(classify_xyz)

    sku_class = sku_volume.merge(sku_cv, on=sku_col, how="left")
    sku_class["combined_class"] = sku_class["abc_class"] + sku_class["xyz_class"]

    sku_freq = feature_pdf.groupby(sku_col)["period"].nunique().reset_index()
    sku_freq.columns = [sku_col, "n_active_periods"]
    total_periods = feature_pdf["period"].nunique()
    sku_freq["frequency_pct"] = sku_freq["n_active_periods"] / total_periods

    sku_class = sku_class.merge(sku_freq, on=sku_col, how="left")

    class_spark = spark.createDataFrame(sku_class)
    write_lakehouse_table(class_spark, gold_lakehouse_id, sku_output_table, mode="overwrite")
    logger.info(f"[sku_class] Wrote {len(sku_class)} SKU classifications")

    logger.info("\n[sku_class] ABC Distribution:")
    logger.info("\n%s", sku_class["abc_class"].value_counts().to_string())
    logger.info("\n[sku_class] XYZ Distribution:")
    logger.info("\n%s", sku_class["xyz_class"].value_counts().to_string())
    logger.info("\n[sku_class] Combined (top):")
    logger.info("\n%s", sku_class["combined_class"].value_counts().head(9).to_string())

logger.info("[sku_class] Complete.")
