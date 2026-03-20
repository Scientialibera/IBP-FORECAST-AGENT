# Fabric Notebook
# 09_market_adjustments.py -- Apply market-level ±X% scaling as transparent layer
# Phase 1: Core Capability

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module
# %run ../modules/override_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")
bronze_lakehouse_id = resolve_lakehouse_id(bronze_lakehouse_id, "bronze")

forecast_table = cfg("output_table")
adjustments_table = cfg("adjustments_table")
default_scale_factor = float(cfg("default_scale_factor"))
grain_columns = cfg("grain_columns")


# Get latest sales version (or system if no sales version)
logger.info("[market_adj] Loading latest sales or system forecast.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)

sales = forecast_spark.filter(forecast_spark.version_type == "sales").toPandas()
if sales.empty:
    logger.warning("[market_adj] No sales version found, using system baseline.")
    sales = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

if sales.empty:
    logger.warning("[market_adj] No forecast found. Exiting.")
else:
    parent_vid = sales["version_id"].iloc[0]
    logger.info(f"[market_adj] Parent version: {parent_vid[:8]}... ({len(sales)} rows)")

    # Forecast grain is (plant_id, sku_id) -- join market_id from orders/bronze
    if "market_id" not in sales.columns:
        logger.info("[market_adj] Joining market_id from bronze orders...")
        orders_spark = read_lakehouse_table(spark, bronze_lakehouse_id, "orders")
        market_map = (orders_spark.select("sku_id", "market_id")
                      .distinct()
                      .dropDuplicates(["sku_id"])
                      .toPandas())
        sales = sales.merge(market_map, on="sku_id", how="left")
        sales["market_id"] = sales["market_id"].fillna("UNKNOWN")
        logger.info(f"[market_adj] Mapped {sales['market_id'].nunique()} markets")

    # Load market adjustments table
    adjustments_pdf = None
    try:
        adj_spark = read_lakehouse_table(spark, bronze_lakehouse_id, adjustments_table)
        adjustments_pdf = adj_spark.toPandas()
        logger.info(f"[market_adj] Loaded {len(adjustments_pdf)} adjustment rows")
    except Exception:
        logger.info("[market_adj] No market_adjustments table -- applying default factor")

    # Apply market adjustments
    adjusted_df = apply_market_adjustments(
        sales, adjustments_pdf,
        market_column="market_id", period_column="period",
        default_factor=default_scale_factor,
    )

    # Version and append
    versioned_df, vid = stamp_forecast_version(
        adjusted_df, version_type="market_adjusted", model_type="market_adjustment",
        parent_version_id=parent_vid, created_by="market_planner"
    )
    append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned_df)
    logger.info(f"[market_adj] Market-adjusted version: {vid[:8]}...")

    # Summary
    n_adjusted = (versioned_df["market_scale_factor"] != 1.0).sum()
    logger.info(f"[market_adj] {n_adjusted} rows with non-default scale factors")

logger.info("[market_adj] Complete.")
