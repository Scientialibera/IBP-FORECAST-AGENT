# Fabric Notebook -- Module
# versioning_module.py -- Forecast version management, snapshot creation

import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger("ibp")


def stamp_forecast_version(df: pd.DataFrame, version_type: str = "system",
                           model_type: str = "", parent_version_id: str = "",
                           created_by: str = "system") -> pd.DataFrame:
    """Add versioning columns to a forecast dataframe."""
    result = df.copy()
    vid = generate_version_id()
    result["version_id"] = vid
    result["snapshot_month"] = current_snapshot_label()
    result["version_type"] = version_type
    if "model_type" not in result.columns and model_type:
        result["model_type"] = model_type
    result["created_by"] = created_by
    result["created_at"] = datetime.utcnow().isoformat()
    result["parent_version_id"] = parent_version_id

    if "override_delta_tons" not in result.columns:
        result["override_delta_tons"] = 0.0
    if "market_scale_factor" not in result.columns:
        result["market_scale_factor"] = 1.0
    if "final_forecast_tons" not in result.columns:
        result["final_forecast_tons"] = result.get("forecast_tons", 0.0)

    return result, vid


def append_versioned_forecast(spark, gold_lakehouse_id: str, table_name: str,
                              forecast_df: pd.DataFrame) -> None:
    """Append (not overwrite) a versioned forecast to the gold table."""
    sdf = spark.createDataFrame(forecast_df)
    write_lakehouse_table(sdf, gold_lakehouse_id, table_name, mode="append")


def get_latest_system_version(spark, gold_lakehouse_id: str, table_name: str,
                              snapshot_month: str = "") -> pd.DataFrame:
    """Read the most recent system forecast version."""
    df = read_lakehouse_table(spark, gold_lakehouse_id, table_name)
    filtered = df.filter(df.version_type == "system")
    if snapshot_month:
        filtered = filtered.filter(filtered.snapshot_month == snapshot_month)
    else:
        filtered = filtered.orderBy(df.created_at.desc())

    pdf = filtered.toPandas()
    if pdf.empty:
        return pdf

    latest_vid = pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    return pdf[pdf["version_id"] == latest_vid].copy()


def purge_old_snapshots(spark, gold_lakehouse_id: str, table_name: str,
                        keep_n: int = 24) -> None:
    """Remove snapshots older than keep_n months. Preserves data governance."""
    df = read_lakehouse_table(spark, gold_lakehouse_id, table_name).toPandas()
    if df.empty:
        return

    unique_months = sorted(df["snapshot_month"].unique(), reverse=True)
    if len(unique_months) <= keep_n:
        return

    months_to_keep = set(unique_months[:keep_n])
    filtered = df[df["snapshot_month"].isin(months_to_keep)]
    sdf = spark.createDataFrame(filtered)
    write_lakehouse_table(sdf, gold_lakehouse_id, table_name, mode="overwrite")
    logger.info(f"[versioning] Purged {len(unique_months) - keep_n} old snapshots, kept {keep_n}")
