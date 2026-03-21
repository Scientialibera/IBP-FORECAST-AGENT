# Fabric Notebook
# 08_sales_overrides.py -- Apply sales override layer on top of statistical baseline
# Phase 1: Core Capability -- Sales inputs are auditable

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module
# %run ../modules/override_module


gold_lakehouse_id = resolve_lakehouse_id("", "gold")
bronze_lakehouse_id = resolve_lakehouse_id("", "bronze")

forecast_table = cfg("output_table")
overrides_table = cfg("overrides_table")
grain_columns = cfg("grain_columns")


logger.info("[overrides] Loading latest system forecast.")
system_pdf = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

if system_pdf.empty:
    logger.warning("[overrides] No system forecast found. Run 05 + 06 first.")
else:
    system_version_id = system_pdf["version_id"].iloc[0]
    logger.info(f"[overrides] Baseline version: {system_version_id[:8]}... ({len(system_pdf)} rows)")

    # Load sales overrides table (may be empty if no overrides yet)
    overrides_pdf = None
    try:
        overrides_spark = read_lakehouse_table(spark, bronze_lakehouse_id, overrides_table)
        overrides_pdf = overrides_spark.toPandas()
        logger.info(f"[overrides] Loaded {len(overrides_pdf)} override rows")
    except Exception:
        logger.warning("[overrides] No sales_overrides table found -- creating sales version equal to baseline")

    # Apply overrides
    sales_df = apply_sales_overrides(system_pdf, overrides_pdf, grain_columns)

    # Version and append
    versioned_df, vid = stamp_forecast_version(
        sales_df, version_type="sales", model_type="override",
        parent_version_id=system_version_id, created_by="sales_team"
    )
    append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned_df)
    logger.info(f"[overrides] Sales version created: {vid[:8]}... ({len(versioned_df)} rows)")

    # Summary of overrides applied
    n_overrides = (versioned_df["override_delta_tons"] != 0).sum()
    total_delta = versioned_df["override_delta_tons"].sum()
    logger.info(f"[overrides] {n_overrides} overrides applied, total delta: {total_delta:.1f} tons")

logger.info("[overrides] Complete.")
