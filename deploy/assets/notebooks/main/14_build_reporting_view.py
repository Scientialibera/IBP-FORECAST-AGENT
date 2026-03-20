# Fabric Notebook
# 14_build_reporting_view.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
silver_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")
bronze_lakehouse_id = resolve_lakehouse_id(bronze_lakehouse_id, "bronze")
silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")

import pandas as pd
import numpy as np
from datetime import datetime

forecast_table = cfg("output_table")
reporting_table = cfg("reporting_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
primary_table = cfg("primary_table")

logger.info("[reporting] Copying dimension tables (master_sku, master_plant) to gold for semantic model...")
for dim_table in cfg("dimension_tables"):
    try:
        dim_df = read_lakehouse_table(spark, bronze_lakehouse_id, dim_table)
        write_lakehouse_table(dim_df, gold_lakehouse_id, dim_table, mode="overwrite")
        logger.info(f"  {dim_table}: {dim_df.count()} rows copied to gold")
    except Exception as e:
        logger.warning(f"  {dim_table}: WARN - {e}")

logger.info("[reporting] Building unified actuals-vs-forecast reporting view.")

fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
logger.info(f"[reporting] Forecast versions: {len(fc_df)} rows")

actuals_df = read_lakehouse_table(spark, bronze_lakehouse_id, primary_table).toPandas()
logger.info(f"[reporting] Actuals ({primary_table}): {len(actuals_df)} rows")

if "period_date" in actuals_df.columns and "period" not in actuals_df.columns:
    actuals_df["period"] = pd.to_datetime(actuals_df["period_date"]).dt.to_period("M").astype(str)

date_col = "period" if "period" in fc_df.columns else "period_date"
actuals_agg = actuals_df.groupby(grain_columns + [date_col], as_index=False)[target_column].sum()
actuals_agg = actuals_agg.rename(columns={target_column: "actual_tons"})
actuals_agg["record_type"] = "actual"

forecast_rows = []
for vtype in fc_df["version_type"].unique() if "version_type" in fc_df.columns else ["forecast"]:
    subset = fc_df[fc_df["version_type"] == vtype].copy() if "version_type" in fc_df.columns else fc_df.copy()
    for _, row in subset.iterrows():
        r = {c: row.get(c) for c in grain_columns if c in row.index}
        r[date_col] = row.get(date_col, row.get("period", row.get("period_date")))
        r["forecast_tons"] = row.get("forecast_tons", row.get("tons", None))
        r["model_type"] = row.get("model_type", "unknown")
        r["version_type"] = vtype
        r["version_id"] = row.get("version_id", "")
        forecast_rows.append(r)
forecast_clean = pd.DataFrame(forecast_rows)

merge_keys = grain_columns + [date_col]
valid_keys = [k for k in merge_keys if k in actuals_agg.columns and k in forecast_clean.columns]

if valid_keys:
    reporting = forecast_clean.merge(actuals_agg[valid_keys + ["actual_tons"]],
                                     on=valid_keys, how="outer")
else:
    reporting = forecast_clean.copy()
    reporting["actual_tons"] = np.nan

reporting["forecast_tons"] = pd.to_numeric(reporting.get("forecast_tons"), errors="coerce")
reporting["actual_tons"] = pd.to_numeric(reporting.get("actual_tons"), errors="coerce")

mask = reporting["forecast_tons"].notna() & reporting["actual_tons"].notna()
reporting.loc[mask, "abs_error"] = (reporting.loc[mask, "forecast_tons"] - reporting.loc[mask, "actual_tons"]).abs()
reporting.loc[mask, "pct_error"] = reporting.loc[mask, "abs_error"] / reporting.loc[mask, "actual_tons"].replace(0, np.nan)
reporting.loc[mask, "variance"] = reporting.loc[mask, "forecast_tons"] - reporting.loc[mask, "actual_tons"]

reporting["is_future"] = reporting["actual_tons"].isna() & reporting["forecast_tons"].notna()
reporting["snapshot_date"] = datetime.utcnow().strftime("%Y-%m-%d")

write_lakehouse_table(spark.createDataFrame(reporting), gold_lakehouse_id, reporting_table, mode="overwrite")
logger.info(f"[reporting] Wrote {len(reporting)} reporting rows to gold.{reporting_table}")

future_count = reporting["is_future"].sum()
historical_count = (~reporting["is_future"]).sum()
logger.info(f"  Historical (actual+forecast): {historical_count}")
logger.info(f"  Future (forecast only):       {future_count}")

# ── Union backtest predictions from silver into gold ─────────────
backtest_predictions_table = cfg("backtest_predictions_table")
logger.info("[reporting] Building backtest_predictions from silver prediction tables...")
prediction_tables = cfg("prediction_tables")
backtest_frames = []
for tbl in prediction_tables:
    try:
        df = read_lakehouse_table(spark, silver_lakehouse_id, tbl).toPandas()
        if not df.empty:
            backtest_frames.append(df)
            logger.info(f"  {tbl}: {len(df)} rows")
    except Exception as e:
        logger.warning(f"  {tbl}: not found or empty -- {e}")

if backtest_frames:
    backtest = pd.concat(backtest_frames, ignore_index=True)
    # Normalize period to string for consistency
    if "period" not in backtest.columns and "period_date" in backtest.columns:
        backtest["period"] = backtest["period_date"]
    backtest["period"] = pd.to_datetime(backtest["period"], format="mixed").dt.strftime("%Y-%m-%d")
    backtest["error"] = backtest["predicted"] - backtest["actual"]
    backtest["abs_error"] = backtest["error"].abs()
    backtest["pct_error"] = (backtest["abs_error"] / backtest["actual"].replace(0, np.nan))
    write_lakehouse_table(
        spark.createDataFrame(backtest), gold_lakehouse_id, backtest_predictions_table, mode="overwrite"
    )
    logger.info(f"[reporting] Wrote {len(backtest)} backtest rows to gold.{backtest_predictions_table}")
    for mt, grp in backtest.groupby("model_type"):
        mape = grp["pct_error"].mean() * 100
        logger.info(f"  {mt}: {len(grp)} rows, MAPE={mape:.1f}%")
else:
    logger.warning("[reporting] No backtest prediction tables found in silver.")

logger.info("[reporting] Complete.")
