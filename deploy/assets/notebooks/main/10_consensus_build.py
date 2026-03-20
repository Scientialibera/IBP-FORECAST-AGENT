# Fabric Notebook
# 10_consensus_build.py -- Build final consensus forecast from all layers
# Phase 1: Core Capability -- system + sales_delta * market_factor

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module
# %run ../modules/override_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")

forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")


logger.info("[consensus] Loading all forecast layers.")
all_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
all_pdf = all_spark.toPandas()

if all_pdf.empty:
    logger.info("[consensus] No forecast data. Run notebooks 04-09 first.")
else:
    # Get latest of each version type
    system_pdf = all_pdf[all_pdf["version_type"] == "system"]
    sales_pdf = all_pdf[all_pdf["version_type"] == "sales"]
    market_pdf = all_pdf[all_pdf["version_type"] == "market_adjusted"]

    if system_pdf.empty:
        logger.warning("[consensus] No system baseline found. Exiting.")
    else:
        latest_sys_vid = system_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
        system_latest = system_pdf[system_pdf["version_id"] == latest_sys_vid]

        sales_latest = None
        if not sales_pdf.empty:
            latest_sales_vid = sales_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
            sales_latest = sales_pdf[sales_pdf["version_id"] == latest_sales_vid]

        market_latest = None
        if not market_pdf.empty:
            latest_mkt_vid = market_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
            market_latest = market_pdf[market_pdf["version_id"] == latest_mkt_vid]

        logger.info(f"[consensus] System: {len(system_latest)} rows")
        logger.info(f"[consensus] Sales: {len(sales_latest) if sales_latest is not None else 0} rows")
        logger.info(f"[consensus] Market: {len(market_latest) if market_latest is not None else 0} rows")

        consensus_df = build_consensus(system_latest, sales_latest, market_latest, grain_columns)

        versioned_df, vid = stamp_forecast_version(
            consensus_df, version_type="consensus", model_type="consensus",
            parent_version_id=latest_sys_vid, created_by="system"
        )
        append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned_df)
        logger.info(f"[consensus] Consensus version: {vid[:8]}... ({len(versioned_df)} rows)")

        total_forecast = versioned_df["final_forecast_tons"].sum()
        logger.info(f"[consensus] Total consensus forecast: {total_forecast:,.1f} tons")

logger.info("[consensus] Complete.")
