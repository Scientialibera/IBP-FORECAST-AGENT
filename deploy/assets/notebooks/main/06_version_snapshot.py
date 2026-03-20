# Fabric Notebook
# 06_version_snapshot.py

# @parameters
silver_lakehouse_id = ""
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module


silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")
gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")

import hashlib
import pandas as pd

output_table = cfg("output_table")
keep_n = cfg("keep_n_snapshots")

logger.info(f"[version] Creating snapshot in gold.{output_table}")
raw_df = read_lakehouse_table(spark, silver_lakehouse_id, cfg("raw_forecasts_table")).toPandas()
logger.info(f"[version] Read {len(raw_df)} raw forecast rows from silver")

if raw_df.empty:
    logger.warning("[version] WARNING: No raw forecasts to snapshot.")
else:
    payload_cols = sorted([c for c in raw_df.columns])
    raw_hash = hashlib.sha256(
        pd.util.hash_pandas_object(
            raw_df[payload_cols].sort_values(payload_cols).reset_index(drop=True)
        ).values.tobytes()
    ).hexdigest()[:16]
    logger.info(f"[version] Content hash: {raw_hash}")

    skip = False
    try:
        existing = read_lakehouse_table(spark, gold_lakehouse_id, output_table).toPandas()
        if not existing.empty and "version_id" in existing.columns:
            latest_vid = existing["version_id"].max()
            latest = existing[existing["version_id"] == latest_vid]
            meta_cols = {"version_id", "version_type", "snapshot_month"}
            compare_cols = sorted([c for c in latest.columns if c not in meta_cols and c in payload_cols])
            if compare_cols:
                latest_hash = hashlib.sha256(
                    pd.util.hash_pandas_object(
                        latest[compare_cols].sort_values(compare_cols).reset_index(drop=True)
                    ).values.tobytes()
                ).hexdigest()[:16]
                if raw_hash == latest_hash:
                    logger.warning(f"[version] CDC: no changes detected (hash={raw_hash}). Skipping snapshot.")
                    skip = True
                else:
                    logger.info(f"[version] CDC: data changed ({latest_hash} -> {raw_hash}). Creating new snapshot.")
    except Exception as e:
        logger.info(f"[version] No existing versions yet ({e}). Creating first snapshot.")

    if not skip:
        versioned, vid = stamp_forecast_version(raw_df, version_type="system")
        logger.info(f"[version] Stamped version {vid}, {len(versioned)} rows")
        append_versioned_forecast(spark, gold_lakehouse_id, output_table, versioned)
        purge_old_snapshots(spark, gold_lakehouse_id, output_table, keep_n=keep_n)

logger.info("[version] Complete.")
