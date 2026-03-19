# Fabric Notebook
# P2_03_sku_classification.py -- ABC/XYZ and runner/repeater/stranger classification
# Phase 2: Advanced Capability

# %run ../modules/config_module
# %run ../modules/utils_module

import pandas as pd
import numpy as np

params = get_notebook_params()

silver_lakehouse_id = params["silver_lakehouse_id"]
gold_lakehouse_id = params["gold_lakehouse_id"]
target_column = params["target_column"]
grain_columns = parse_list_param(params["grain_columns"])
sku_output_table = params.get("sku_classification_output_table") or "sku_classifications"

# ABC thresholds (cumulative % of volume)
runner_threshold = float(params.get("runner_threshold") or 0.8)
repeater_threshold = float(params.get("repeater_threshold") or 0.95)

# XYZ thresholds (coefficient of variation)
xyz_x_threshold = float(params.get("xyz_cv_threshold_x") or 0.5)
xyz_y_threshold = float(params.get("xyz_cv_threshold_y") or 1.0)

enabled = str(params.get("sku_classification_enabled") or "false").lower() == "true"
if not enabled:
    print("[sku_class] Disabled in config. Set sku_classification.enabled = true to run.")
else:
    print("[sku_class] Loading feature table.")
    feature_spark = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
    feature_pdf = feature_spark.toPandas()

    sku_col = "sku_id"

    # ABC Classification (by total volume)
    print("[sku_class] Computing ABC classification...")
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

    # Runner/Repeater/Stranger
    sku_volume["rrs_class"] = sku_volume["abc_class"].map(
        {"A": "runner", "B": "repeater", "C": "stranger"}
    )

    # XYZ Classification (by demand variability)
    print("[sku_class] Computing XYZ classification...")
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

    # Merge ABC and XYZ
    sku_class = sku_volume.merge(sku_cv, on=sku_col, how="left")
    sku_class["combined_class"] = sku_class["abc_class"] + sku_class["xyz_class"]

    # Frequency analysis
    sku_freq = feature_pdf.groupby(sku_col)["period"].nunique().reset_index()
    sku_freq.columns = [sku_col, "n_active_periods"]
    total_periods = feature_pdf["period"].nunique()
    sku_freq["frequency_pct"] = sku_freq["n_active_periods"] / total_periods

    sku_class = sku_class.merge(sku_freq, on=sku_col, how="left")

    # Write results
    class_spark = spark.createDataFrame(sku_class)
    write_lakehouse_table(class_spark, gold_lakehouse_id, sku_output_table, mode="overwrite")
    print(f"[sku_class] Wrote {len(sku_class)} SKU classifications")

    # Summary
    print("\n[sku_class] ABC Distribution:")
    print(sku_class["abc_class"].value_counts().to_string())
    print("\n[sku_class] XYZ Distribution:")
    print(sku_class["xyz_class"].value_counts().to_string())
    print("\n[sku_class] Combined (top):")
    print(sku_class["combined_class"].value_counts().head(9).to_string())

print("[sku_class] Complete.")
