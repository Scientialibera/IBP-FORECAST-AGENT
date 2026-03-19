# Fabric Notebook
# P2_03_sku_classification.py

# @parameters
silver_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

import pandas as pd
import numpy as np

target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
output_table = cfg("sku_classification_output_table")
runner_thresh = cfg("runner_threshold")
repeater_thresh = cfg("repeater_threshold")
xyz_x = cfg("xyz_cv_threshold_x")
xyz_y = cfg("xyz_cv_threshold_y")

print("[sku_class] Classifying SKUs (ABC-XYZ + Runner/Repeater/Stranger).")
feature_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table").toPandas()

if target_column in feature_df.columns and grain_columns[0] in feature_df.columns:
    sku_stats = feature_df.groupby(grain_columns).agg(
        total_volume=(target_column, "sum"),
        n_periods=(target_column, "count"),
        cv=(target_column, lambda x: x.std() / x.mean() if x.mean() > 0 else float("inf")),
    ).reset_index()

    total = sku_stats["total_volume"].sum()
    sku_stats = sku_stats.sort_values("total_volume", ascending=False)
    sku_stats["cumulative_pct"] = sku_stats["total_volume"].cumsum() / total

    sku_stats["abc_class"] = np.where(sku_stats["cumulative_pct"] <= 0.8, "A",
                             np.where(sku_stats["cumulative_pct"] <= 0.95, "B", "C"))
    sku_stats["xyz_class"] = np.where(sku_stats["cv"] <= xyz_x, "X",
                             np.where(sku_stats["cv"] <= xyz_y, "Y", "Z"))

    max_periods = sku_stats["n_periods"].max()
    sku_stats["frequency_pct"] = sku_stats["n_periods"] / max_periods
    sku_stats["rrs_class"] = np.where(sku_stats["frequency_pct"] >= repeater_thresh, "Repeater",
                             np.where(sku_stats["frequency_pct"] >= runner_thresh, "Runner", "Stranger"))

    write_lakehouse_table(spark.createDataFrame(sku_stats), gold_lakehouse_id, output_table, mode="overwrite")
    print(f"[sku_class] {len(sku_stats)} SKU classifications written")
print("[sku_class] Complete.")
