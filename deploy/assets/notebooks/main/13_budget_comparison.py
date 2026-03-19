# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# METADATA ********************

# CELL ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 13_budget_comparison.py -- Compare consensus forecast to budget, flag over/under
# Phase 1: Core Capability

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run versioning_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
budget_table = params.get("budget_table") or "budget_volumes"
comparison_table = params.get("comparison_output_table") or "budget_comparison"
over_threshold = float(params.get("over_forecast_threshold") or 0.10)
under_threshold = float(params.get("under_forecast_threshold") or -0.10)
hierarchy_levels = parse_list_param(params.get("hierarchy_levels") or '["market_id","plant_id","sku_group"]')

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

# Load consensus forecast
print("[budget] Loading consensus forecast.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
consensus = forecast_spark.filter(F.col("version_type") == "consensus")

if consensus.count() == 0:
    print("[budget] No consensus forecast. Using system baseline.")
    consensus = forecast_spark.filter(F.col("version_type") == "system")

consensus_pdf = consensus.toPandas()
if consensus_pdf.empty:
    print("[budget] No forecast data. Exiting.")
else:
    latest_vid = consensus_pdf.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    consensus_pdf = consensus_pdf[consensus_pdf["version_id"] == latest_vid]

    # Load budget
    print("[budget] Loading budget volumes.")
    try:
        budget_spark = read_lakehouse_table(spark, bronze_lakehouse_id, budget_table)
        budget_pdf = budget_spark.toPandas()
    except Exception:
        print("[budget] No budget_volumes table found. Exiting.")
        budget_pdf = None

    if budget_pdf is not None and not budget_pdf.empty:
        # Compare at each hierarchy level
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
            print(f"[budget] {table_name}: {len(merged)} rows")

            # Flag summary
            n_over = (merged["flag"] == "over_forecast").sum()
            n_under = (merged["flag"] == "under_forecast").sum()
            print(f"[budget]   Over: {n_over}, Under: {n_under}, On-track: {len(merged) - n_over - n_under}")

print("[budget] Complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
