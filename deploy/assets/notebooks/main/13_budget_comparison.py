# Fabric Notebook
# 13_budget_comparison.py -- Compare consensus forecast to budget, flag over/under
# Phase 1: Core Capability

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module

from pyspark.sql import functions as F

forecast_table = cfg("output_table")
budget_table = cfg("budget_table")
comparison_table = cfg("comparison_output_table")
over_threshold = float(cfg("over_forecast_threshold"))
under_threshold = float(cfg("under_forecast_threshold"))
hierarchy_levels = cfg("hierarchy_levels")

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

logger.info("[budget] Loading consensus forecast.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
consensus = forecast_spark.filter(F.col("version_type") == "consensus")

if consensus.count() == 0:
    logger.info("[budget] No consensus forecast. Using system baseline.")
    consensus = forecast_spark.filter(F.col("version_type") == "system")

consensus_pdf = consensus.toPandas()
if consensus_pdf.empty:
    logger.info("[budget] No forecast data. Exiting.")
else:
    latest_vid = consensus_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    consensus_pdf = consensus_pdf[consensus_pdf["version_id"] == latest_vid]

    logger.info("[budget] Loading budget volumes.")
    try:
        budget_spark = read_lakehouse_table(spark, bronze_lakehouse_id, budget_table)
        budget_pdf = budget_spark.toPandas()
    except Exception:
        logger.warning("[budget] No budget_volumes table found. Exiting.")
        budget_pdf = None

    if budget_pdf is not None and not budget_pdf.empty:
        for level_cols in [hierarchy_levels[:i+1] for i in range(len(hierarchy_levels))]:
            valid_forecast = [c for c in level_cols + ["period"] if c in consensus_pdf.columns]
            valid_budget = [c for c in level_cols + ["period"] if c in budget_pdf.columns]

            if not all(c in consensus_pdf.columns for c in valid_forecast):
                continue
            if not all(c in budget_pdf.columns for c in valid_budget):
                continue

            fc_agg = consensus_pdf.groupby(valid_forecast).agg(
                forecast_tons=("final_forecast_tons", "sum")
            ).reset_index()

            budget_col = "budget_tons" if "budget_tons" in budget_pdf.columns else "tons"
            bgt_agg = budget_pdf.groupby(valid_budget).agg(
                budget_tons=(budget_col, "sum")
            ).reset_index()

            merged = fc_agg.merge(bgt_agg, on=valid_forecast, how="outer")
            merged["forecast_tons"] = merged["forecast_tons"].fillna(0)
            merged["budget_tons"] = merged["budget_tons"].fillna(0)

            merged["variance_tons"] = merged["forecast_tons"] - merged["budget_tons"]
            merged["variance_pct"] = merged.apply(
                lambda r: r["variance_tons"] / r["budget_tons"] if r["budget_tons"] != 0 else 0, axis=1
            )
            merged["flag"] = merged["variance_pct"].apply(
                lambda v: "over_forecast" if v > over_threshold
                else ("under_forecast" if v < under_threshold else "on_track")
            )
            merged["threshold_pct"] = merged["flag"].map({
                "over_forecast": over_threshold,
                "under_forecast": under_threshold,
                "on_track": 0,
            })

            level_label = "_".join(level_cols)
            table_name = f"budget_comparison_{level_label}"
            comp_spark = spark.createDataFrame(merged)
            write_lakehouse_table(comp_spark, gold_lakehouse_id, table_name, mode="overwrite")
            logger.info(f"[budget] {table_name}: {len(merged)} rows")

            n_over = (merged["flag"] == "over_forecast").sum()
            n_under = (merged["flag"] == "under_forecast").sum()
            logger.info(f"[budget]   Over: {n_over}, Under: {n_under}, On-track: {len(merged) - n_over - n_under}")

logger.info("[budget] Complete.")
