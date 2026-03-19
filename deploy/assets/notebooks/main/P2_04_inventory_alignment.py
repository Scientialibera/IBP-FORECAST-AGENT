# Fabric Notebook
# P2_04_inventory_alignment.py -- FG inventory vs forecast demand alignment
# Phase 2: Advanced Capability

# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/versioning_module

import pandas as pd
import numpy as np

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
inventory_table = "inventory_finished_goods"
grain_columns = parse_list_param(params["grain_columns"])

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

# Load latest consensus/system forecast
print("[inventory] Loading forecast.")
system_pdf = get_latest_system_version(spark, gold_lakehouse_id, forecast_table)

# Prefer consensus
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
consensus = forecast_spark.filter(forecast_spark.version_type == "consensus").toPandas()
if not consensus.empty:
    latest_vid = consensus.sort_values("created_at", ascending=False)["version_id"].iloc[0]
    forecast_pdf = consensus[consensus["version_id"] == latest_vid]
else:
    forecast_pdf = system_pdf

if forecast_pdf.empty:
    print("[inventory] No forecast data. Exiting.")
else:
    # Load current FG inventory
    print("[inventory] Loading finished goods inventory.")
    try:
        inv_spark = read_lakehouse_table(spark, bronze_lakehouse_id, inventory_table)
        inv_pdf = inv_spark.toPandas()
    except Exception:
        print("[inventory] No inventory table found. Exiting.")
        inv_pdf = None

    if inv_pdf is not None and not inv_pdf.empty:
        # Aggregate inventory by grain
        inv_col = "on_hand_tons" if "on_hand_tons" in inv_pdf.columns else "quantity"
        valid_grain = [c for c in grain_columns if c in inv_pdf.columns]

        inv_agg = inv_pdf.groupby(valid_grain).agg(
            current_inventory=(inv_col, "sum")
        ).reset_index()

        # Aggregate near-term forecast demand (next 3 months)
        forecast_pdf["period_dt"] = pd.to_datetime(forecast_pdf["period"])
        cutoff = forecast_pdf["period_dt"].min() + pd.DateOffset(months=3)
        near_term = forecast_pdf[forecast_pdf["period_dt"] <= cutoff]

        fc_col = "final_forecast_tons" if "final_forecast_tons" in near_term.columns else "forecast_tons"
        valid_grain_fc = [c for c in grain_columns if c in near_term.columns]
        demand_agg = near_term.groupby(valid_grain_fc).agg(
            demand_3m=(fc_col, "sum")
        ).reset_index()

        # Merge and compute coverage
        merge_keys = [c for c in valid_grain if c in valid_grain_fc]
        alignment = inv_agg.merge(demand_agg, on=merge_keys, how="outer")
        alignment["current_inventory"] = alignment["current_inventory"].fillna(0)
        alignment["demand_3m"] = alignment["demand_3m"].fillna(0)

        alignment["coverage_months"] = np.where(
            alignment["demand_3m"] > 0,
            (alignment["current_inventory"] / alignment["demand_3m"]) * 3,
            np.where(alignment["current_inventory"] > 0, 99, 0)
        )

        alignment["risk_flag"] = alignment.apply(
            lambda r: "stock_out_risk" if r["coverage_months"] < 1
            else ("overbuild" if r["coverage_months"] > 6 else "healthy"),
            axis=1
        )

        alignment["excess_tons"] = np.maximum(0, alignment["current_inventory"] - alignment["demand_3m"])
        alignment["shortfall_tons"] = np.maximum(0, alignment["demand_3m"] - alignment["current_inventory"])

        # Write results
        align_spark = spark.createDataFrame(alignment)
        write_lakehouse_table(align_spark, gold_lakehouse_id, "inventory_alignment", mode="overwrite")
        print(f"[inventory] Wrote {len(alignment)} alignment rows")

        # Summary
        n_risk = (alignment["risk_flag"] == "stock_out_risk").sum()
        n_over = (alignment["risk_flag"] == "overbuild").sum()
        n_healthy = (alignment["risk_flag"] == "healthy").sum()
        total_excess = alignment["excess_tons"].sum()
        total_short = alignment["shortfall_tons"].sum()
        print(f"\n[inventory] Risk Summary:")
        print(f"  Stock-out risk: {n_risk} items")
        print(f"  Overbuild: {n_over} items ({total_excess:,.0f} excess tons)")
        print(f"  Healthy: {n_healthy} items")
        print(f"  Total shortfall: {total_short:,.0f} tons")

print("[inventory] Complete.")
