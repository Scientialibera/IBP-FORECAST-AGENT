# Fabric Notebook
# 09_market_adjustments.py -- Apply market-level ±X% scaling as transparent layer
# Phase 1: Core Capability

# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module
# %run ../modules/override_module

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
adjustments_table = params.get("adjustments_table") or "market_adjustments"
default_scale_factor = float(params.get("default_scale_factor") or 1.0)
grain_columns = parse_list_param(params["grain_columns"])

if not gold_lakehouse_id:
    raise ValueError("gold_lakehouse_id is required.")

# Get latest sales version (or system if no sales version)
print("[market_adj] Loading latest sales or system forecast.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)

sales = forecast_spark.filter(forecast_spark.version_type == "sales").toPandas()
if sales.empty:
    print("[market_adj] No sales version found, using system baseline.")
    sales = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

if sales.empty:
    print("[market_adj] No forecast found. Exiting.")
else:
    parent_vid = sales["version_id"].iloc[0]
    print(f"[market_adj] Parent version: {parent_vid[:8]}... ({len(sales)} rows)")

    # Load market adjustments table
    adjustments_pdf = None
    try:
        adj_spark = read_lakehouse_table(spark, bronze_lakehouse_id, adjustments_table)
        adjustments_pdf = adj_spark.toPandas()
        print(f"[market_adj] Loaded {len(adjustments_pdf)} adjustment rows")
    except Exception:
        print("[market_adj] No market_adjustments table -- applying default factor")

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
    print(f"[market_adj] Market-adjusted version: {vid[:8]}...")

    # Summary
    n_adjusted = (versioned_df["market_scale_factor"] != 1.0).sum()
    print(f"[market_adj] {n_adjusted} rows with non-default scale factors")

print("[market_adj] Complete.")
