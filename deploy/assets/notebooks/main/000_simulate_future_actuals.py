# Fabric Notebook
# 000_simulate_future_actuals.py -- Inject synthetic "future actuals" for testing accuracy tracking
#
# Purpose: After running the full pipeline (train → score), this notebook reads the forward
# forecasts and generates realistic fake actuals for a configurable number of future weeks.
# These are appended to the source orders table. Re-running the scoring pipeline after this
# will cause accuracy_tracking (notebook 11) to compare the original predictions against
# these newly available actuals -- demonstrating the continuous monitoring loop.
#
# Usage:
#   1. Run pl_ibp_train → pl_ibp_score_and_refresh  (produces forward forecasts)
#   2. Run this notebook                              (injects fake actuals)
#   3. Run pl_ibp_score_and_refresh again             (accuracy tracking picks up the overlap)
#   4. Check accuracy_tracking table + reporting view in the semantic model

# @parameters
source_lakehouse_id = ""
silver_lakehouse_id = ""
simulate_weeks = 12
noise_pct = 0.10
seed = 99
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/schemas_module


source_lakehouse_id = resolve_lakehouse_id(source_lakehouse_id, "source")
silver_lakehouse_id = resolve_lakehouse_id(silver_lakehouse_id, "silver")

import pandas as pd
import numpy as np

np.random.seed(seed)
simulate_weeks = int(simulate_weeks)
noise_pct = float(noise_pct)

logger.info(f"[simulate] Generating fake actuals for {simulate_weeks} future weeks "
            f"(noise ±{noise_pct*100:.0f}%)")

# ── Load raw forecasts from silver ───────────────────────────────
logger.info("[simulate] Loading raw_forecasts from silver...")
raw_fc = read_lakehouse_table(spark, silver_lakehouse_id, cfg("raw_forecasts_table")).toPandas()

if raw_fc.empty:
    raise ValueError("raw_forecasts is empty. Run the scoring pipeline first (pl_ibp_score).")

logger.info(f"[simulate] Raw forecasts: {len(raw_fc)} rows, "
            f"models: {raw_fc['model_type'].unique().tolist()}, "
            f"periods: {raw_fc['period'].nunique()}")

# Use one model's forecasts as the "truth" baseline (pick the first enabled model)
# All models forecast the same periods, so we just need one set of grain+period values
baseline_model = raw_fc["model_type"].iloc[0]
baseline = raw_fc[raw_fc["model_type"] == baseline_model].copy()
logger.info(f"[simulate] Using '{baseline_model}' as baseline ({len(baseline)} rows)")

# Sort periods and take only the first N weeks
baseline["period_dt"] = pd.to_datetime(baseline["period"])
all_periods = sorted(baseline["period_dt"].unique())

if simulate_weeks > len(all_periods):
    simulate_weeks = len(all_periods)
    logger.info(f"[simulate] Capped to {simulate_weeks} weeks (all available)")

selected_periods = all_periods[:simulate_weeks]
baseline = baseline[baseline["period_dt"].isin(selected_periods)]
logger.info(f"[simulate] Simulating actuals for {len(selected_periods)} periods: "
            f"{selected_periods[0].date()} to {selected_periods[-1].date()}")

# ── Load existing orders to match schema ─────────────────────────
existing_orders = read_lakehouse_table(spark, source_lakehouse_id, "orders").toPandas()
logger.info(f"[simulate] Existing orders: {len(existing_orders)} rows, "
            f"columns: {list(existing_orders.columns)}")

# ── Load master data for enrichment ──────────────────────────────
master_sku = read_lakehouse_table(spark, source_lakehouse_id, "master_sku").toPandas()
sku_to_group = dict(zip(master_sku["sku_id"], master_sku["sku_group"]))

# ── Generate fake actuals ────────────────────────────────────────
grain_columns = cfg("grain_columns")
rng = np.random.RandomState(seed)

fake_rows = []
for _, row in baseline.iterrows():
    forecast_tons = float(row["forecast_tons"])
    noise = rng.normal(0, noise_pct)
    actual_tons = max(0, round(forecast_tons * (1 + noise), 2))

    fake_row = {
        "period_date": str(row["period_dt"].date()),
        "plant_id": row["plant_id"],
        "sku_id": row["sku_id"],
        "sku_group": sku_to_group.get(row["sku_id"], "Unknown"),
        "customer_id": f"CUST-{rng.randint(1, 21):03d}",
        "market_id": f"MKT-{chr(65 + hash(row['sku_id']) % 4)}",
        "tons": actual_tons,
        "price_per_ton": round(rng.uniform(900, 2400), 2),
        "lead_time_days": rng.randint(2, 20),
        "promo_flag": int(rng.random() < 0.12),
        "safety_stock_tons": round(actual_tons * rng.uniform(0.15, 0.30), 1),
    }
    fake_rows.append(fake_row)

fake_df = pd.DataFrame(fake_rows)
logger.info(f"[simulate] Generated {len(fake_df)} fake actual rows")

# ── Append to source orders table ────────────────────────────────
# Remove any previously simulated rows for these periods to allow re-runs
existing_periods = set(fake_df["period_date"].unique())
clean_existing = existing_orders[~existing_orders["period_date"].isin(existing_periods)]

combined = pd.concat([clean_existing, fake_df], ignore_index=True)
expected_cols = column_names("orders")
present_cols = [c for c in expected_cols if c in combined.columns]
combined_spark = spark.createDataFrame(combined[present_cols])
write_lakehouse_table(combined_spark, source_lakehouse_id, "orders", mode="overwrite")

new_count = len(combined) - len(clean_existing)
logger.info(f"[simulate] Wrote {len(combined)} total order rows "
            f"({new_count} new future actuals appended)")

# ── Summary ──────────────────────────────────────────────────────
logger.info(f"\n{'='*60}")
logger.info("[simulate] SIMULATION COMPLETE")
logger.info(f"  Future weeks injected:  {simulate_weeks}")
logger.info(f"  Period range:           {selected_periods[0].date()} → {selected_periods[-1].date()}")
logger.info(f"  Fake actual rows:       {len(fake_df)}")
logger.info(f"  Noise level:            ±{noise_pct*100:.0f}%")
logger.info(f"  Baseline model used:    {baseline_model}")
logger.info(f"  Grains (plant×sku):     {fake_df.groupby(grain_columns).ngroups}")
logger.info(f"")
logger.info("[simulate] NEXT STEPS:")
logger.info("  1. Run pl_ibp_score_and_refresh (or at minimum: 01 → 02 → 11 → 14 → 15)")
logger.info("  2. Check 'accuracy_tracking' table in gold for prediction-vs-actual metrics")
logger.info("  3. Check 'reporting_actuals_vs_forecast' — previously future rows now have actuals")
logger.info("  4. Open the Power BI report to see accuracy metrics populated")
logger.info(f"{'='*60}")
