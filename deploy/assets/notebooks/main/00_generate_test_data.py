# Fabric Notebook
# 00_generate_test_data.py -- Generate realistic synthetic data for end-to-end testing
# Writes all source tables to the source lakehouse so notebooks 01-13 + P2 can run.

# %run ../modules/config_module

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

params = get_notebook_params()
source_lakehouse_id = params.get("source_lakehouse_id")
if not source_lakehouse_id:
    raise ValueError("source_lakehouse_id is required. Set it in notebook parameters.")

# ── Config ──────────────────────────────────────────────────────
N_SKUS = int(params.get("n_skus") or 50)
N_PLANTS = int(params.get("n_plants") or 5)
N_CUSTOMERS = int(params.get("n_customers") or 20)
N_MARKETS = int(params.get("n_markets") or 4)
N_LINES = int(params.get("n_production_lines") or 10)
HISTORY_MONTHS = int(params.get("history_months") or 42)
SEED = int(params.get("seed") or 42)

np.random.seed(SEED)
print(f"[test_data] Generating {HISTORY_MONTHS} months of history for "
      f"{N_SKUS} SKUs, {N_PLANTS} plants, {N_CUSTOMERS} customers, {N_MARKETS} markets")

# ── Date range ──────────────────────────────────────────────────
end_date = pd.Timestamp(datetime.utcnow().replace(day=1))
dates = pd.date_range(end=end_date, periods=HISTORY_MONTHS, freq="MS")

# ── Master Data ─────────────────────────────────────────────────
markets = [f"MKT-{chr(65+i)}" for i in range(N_MARKETS)]
market_names = ["Residential", "Commercial", "Industrial", "Specialty"][:N_MARKETS]
market_segments = ["NCCA", "NCCA", "imports", "NCCA"][:N_MARKETS]

plants = [f"PLT-{i+1:02d}" for i in range(N_PLANTS)]
plant_names = [f"Plant {chr(65+i)}" for i in range(N_PLANTS)]
plant_regions = ["East", "West", "Central", "South", "North"][:N_PLANTS]

sku_groups = ["Coated Steel", "Galvanized", "Aluminum", "Prepainted", "Specialty Alloy"]
skus = []
for i in range(N_SKUS):
    skus.append({
        "sku_id": f"SKU-{i+1:04d}",
        "sku_name": f"Product {i+1}",
        "sku_group": sku_groups[i % len(sku_groups)],
        "base_width_inches": round(np.random.uniform(12, 72), 1),
        "base_weight_lbs": round(np.random.uniform(50, 500), 0),
        "unit_of_measure": "tons",
    })

customers = []
for i in range(N_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST-{i+1:03d}",
        "customer_name": f"Customer {i+1}",
        "market_id": markets[i % N_MARKETS],
        "region": plant_regions[i % N_PLANTS],
        "tier": np.random.choice(["A", "B", "C"], p=[0.2, 0.5, 0.3]),
    })

lines = []
for i in range(N_LINES):
    lines.append({
        "line_id": f"LINE-{i+1:02d}",
        "plant_id": plants[i % N_PLANTS],
        "line_name": f"Line {i+1}",
        "max_speed_fpm": round(np.random.uniform(100, 500), 0),
        "max_width_inches": round(np.random.uniform(48, 72), 0),
    })

# Write master tables
print("[test_data] Writing master tables...")

master_sku_df = spark.createDataFrame(pd.DataFrame(skus))
write_lakehouse_table(master_sku_df, source_lakehouse_id, "master_sku", mode="overwrite")

master_plant_df = spark.createDataFrame(pd.DataFrame([
    {"plant_id": p, "plant_name": n, "region": r, "capacity_tons_month": round(np.random.uniform(5000, 20000), 0)}
    for p, n, r in zip(plants, plant_names, plant_regions)
]))
write_lakehouse_table(master_plant_df, source_lakehouse_id, "master_plant", mode="overwrite")

master_customer_df = spark.createDataFrame(pd.DataFrame(customers))
write_lakehouse_table(master_customer_df, source_lakehouse_id, "master_customer", mode="overwrite")

master_market_df = spark.createDataFrame(pd.DataFrame([
    {"market_id": m, "market_name": n, "market_segment": s}
    for m, n, s in zip(markets, market_names, market_segments)
]))
write_lakehouse_table(master_market_df, source_lakehouse_id, "master_market", mode="overwrite")

prod_lines_df = spark.createDataFrame(pd.DataFrame(lines))
write_lakehouse_table(prod_lines_df, source_lakehouse_id, "production_lines", mode="overwrite")

print(f"  master_sku: {len(skus)}, master_plant: {N_PLANTS}, master_customer: {N_CUSTOMERS}, "
      f"master_market: {N_MARKETS}, production_lines: {N_LINES}")

# ── Orders (main demand signal) ─────────────────────────────────
print("[test_data] Generating orders with seasonal patterns...")

# Each SKU gets a base demand level + seasonality + noise
orders_rows = []
for sku in skus:
    sku_id = sku["sku_id"]
    base_tons = np.random.uniform(10, 200)
    trend = np.random.uniform(-0.5, 1.5)
    seasonality_amp = np.random.uniform(0.1, 0.4) * base_tons
    phase = np.random.uniform(0, 2 * np.pi)

    assigned_plant = plants[hash(sku_id) % N_PLANTS]
    assigned_customers = np.random.choice([c["customer_id"] for c in customers],
                                          size=min(5, N_CUSTOMERS), replace=False)
    assigned_market = markets[hash(sku_id) % N_MARKETS]

    for t, date in enumerate(dates):
        month_idx = date.month
        seasonal = seasonality_amp * np.sin(2 * np.pi * month_idx / 12 + phase)
        trend_val = trend * t
        noise = np.random.normal(0, base_tons * 0.1)
        tons = max(0.5, base_tons + seasonal + trend_val + noise)

        cust = np.random.choice(assigned_customers)
        price = round(np.random.uniform(800, 2500) + seasonal * 2, 2)
        lead_time = max(1, int(np.random.normal(14, 5)))
        promo = int(np.random.random() < 0.15)
        safety_stock = round(base_tons * np.random.uniform(0.1, 0.3), 1)

        orders_rows.append({
            "period_date": str(date.date()),
            "plant_id": assigned_plant,
            "sku_id": sku_id,
            "sku_group": sku["sku_group"],
            "customer_id": cust,
            "market_id": assigned_market,
            "tons": round(tons, 2),
            "price_per_ton": price,
            "lead_time_days": lead_time,
            "promo_flag": promo,
            "safety_stock_tons": safety_stock,
        })

orders_df = pd.DataFrame(orders_rows)
orders_spark = spark.createDataFrame(orders_df)
write_lakehouse_table(orders_spark, source_lakehouse_id, "orders", mode="overwrite")
print(f"  orders: {len(orders_df)} rows")

# ── Shipments (similar to orders, slightly lagged) ──────────────
print("[test_data] Generating shipments...")
shipments_df = orders_df.copy()
shipments_df["tons"] = (shipments_df["tons"] * np.random.uniform(0.85, 1.05, len(shipments_df))).round(2)
shipments_df["shipped_date"] = pd.to_datetime(shipments_df["period_date"]) + pd.Timedelta(days=7)
shipments_df["shipped_date"] = shipments_df["shipped_date"].astype(str)
shipments_spark = spark.createDataFrame(shipments_df)
write_lakehouse_table(shipments_spark, source_lakehouse_id, "shipments", mode="overwrite")
print(f"  shipments: {len(shipments_df)} rows")

# ── Production History (width, line speed per plant/sku/line) ───
print("[test_data] Generating production history...")
prod_rows = []
for sku in skus:
    sku_id = sku["sku_id"]
    plant_id = plants[hash(sku_id) % N_PLANTS]
    line_id = f"LINE-{(hash(sku_id) % N_LINES) + 1:02d}"
    base_width = sku["base_width_inches"]
    base_speed = np.random.uniform(150, 400)

    for date in dates:
        width = round(base_width + np.random.normal(0, 1.5), 1)
        speed = round(base_speed + np.random.normal(0, 15), 1)
        prod_tons = round(np.random.uniform(5, 150), 2)

        prod_rows.append({
            "period_date": str(date.date()),
            "plant_id": plant_id,
            "sku_id": sku_id,
            "line_id": line_id,
            "width_inches": max(6, width),
            "line_speed_fpm": max(50, speed),
            "produced_tons": prod_tons,
        })

prod_history_df = pd.DataFrame(prod_rows)
prod_spark = spark.createDataFrame(prod_history_df)
write_lakehouse_table(prod_spark, source_lakehouse_id, "production_history", mode="overwrite")
print(f"  production_history: {len(prod_history_df)} rows")

# ── Budget Volumes ──────────────────────────────────────────────
print("[test_data] Generating budget volumes...")
budget_rows = []
future_dates = pd.date_range(start=end_date, periods=12, freq="MS")
all_budget_dates = list(dates[-6:]) + list(future_dates)

for sku in skus:
    sku_id = sku["sku_id"]
    plant_id = plants[hash(sku_id) % N_PLANTS]
    market_id = markets[hash(sku_id) % N_MARKETS]
    base = np.random.uniform(20, 180)

    for date in all_budget_dates:
        budget_tons = round(base + np.random.normal(0, base * 0.15), 2)
        budget_rows.append({
            "period": str(date.date()),
            "plant_id": plant_id,
            "sku_id": sku_id,
            "sku_group": sku["sku_group"],
            "market_id": market_id,
            "budget_tons": max(1, budget_tons),
        })

budget_df = pd.DataFrame(budget_rows)
budget_spark = spark.createDataFrame(budget_df)
write_lakehouse_table(budget_spark, source_lakehouse_id, "budget_volumes", mode="overwrite")
print(f"  budget_volumes: {len(budget_df)} rows")

# ── Finished Goods Inventory ────────────────────────────────────
print("[test_data] Generating FG inventory...")
inv_rows = []
for sku in skus:
    sku_id = sku["sku_id"]
    plant_id = plants[hash(sku_id) % N_PLANTS]
    on_hand = round(np.random.uniform(5, 300), 2)
    inv_rows.append({
        "sku_id": sku_id,
        "plant_id": plant_id,
        "sku_group": sku["sku_group"],
        "on_hand_tons": on_hand,
        "snapshot_date": str(end_date.date()),
    })

inv_df = pd.DataFrame(inv_rows)
inv_spark = spark.createDataFrame(inv_df)
write_lakehouse_table(inv_spark, source_lakehouse_id, "inventory_finished_goods", mode="overwrite")
print(f"  inventory_finished_goods: {len(inv_df)} rows")

# ── Sales Overrides (a few manual adjustments) ──────────────────
print("[test_data] Generating sales overrides...")
override_rows = []
for i in range(30):
    sku = skus[i % N_SKUS]
    plant_id = plants[hash(sku["sku_id"]) % N_PLANTS]
    period = future_dates[i % len(future_dates)]
    delta = round(np.random.uniform(-50, 80), 2)
    override_rows.append({
        "period": str(period.date()),
        "plant_id": plant_id,
        "sku_id": sku["sku_id"],
        "override_delta_tons": delta,
        "override_reason": np.random.choice(["large_order", "customer_request", "seasonal_push", "market_intelligence"]),
        "created_by": "sales_test_user",
    })

override_df = pd.DataFrame(override_rows)
override_spark = spark.createDataFrame(override_df)
write_lakehouse_table(override_spark, source_lakehouse_id, "sales_overrides", mode="overwrite")
print(f"  sales_overrides: {len(override_df)} rows")

# ── Market Adjustments (±% by market) ───────────────────────────
print("[test_data] Generating market adjustments...")
adj_rows = []
for market_id in markets:
    for period in future_dates:
        factor = round(np.random.uniform(0.90, 1.15), 3)
        adj_rows.append({
            "market_id": market_id,
            "period": str(period.date()),
            "scale_factor": factor,
            "adjustment_reason": np.random.choice(["growth_outlook", "downturn_risk", "tariff_impact", "stable"]),
        })

adj_df = pd.DataFrame(adj_rows)
adj_spark = spark.createDataFrame(adj_df)
write_lakehouse_table(adj_spark, source_lakehouse_id, "market_adjustments", mode="overwrite")
print(f"  market_adjustments: {len(adj_df)} rows")

# ── External Signals (Phase 2) ──────────────────────────────────
print("[test_data] Generating external signals...")
signal_rows = []
for date in dates:
    month = date.month
    signal_rows.append({
        "period": str(date.date()),
        "construction_index": round(100 + 15 * np.sin(2 * np.pi * month / 12) + np.random.normal(0, 5), 2),
        "interest_rate": round(np.random.uniform(3.0, 7.5) + 0.02 * (date.year - 2022), 2),
        "inflation_rate": round(np.random.uniform(1.5, 8.0), 2),
        "tariff_rate": round(np.random.choice([0, 5, 10, 15, 25], p=[0.3, 0.25, 0.2, 0.15, 0.1]), 1),
    })

signal_df = pd.DataFrame(signal_rows)
signal_spark = spark.createDataFrame(signal_df)
write_lakehouse_table(signal_spark, source_lakehouse_id, "external_signals", mode="overwrite")
print(f"  external_signals: {len(signal_df)} rows")

# ── Scenario Definitions (Phase 2) ──────────────────────────────
print("[test_data] Generating scenario definitions...")
scenario_df = pd.DataFrame([
    {"scenario_name": "ncca_only", "filter_type": "market_segment", "filter_value": "NCCA", "include": True},
    {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment", "filter_value": "NCCA", "include": True},
    {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment", "filter_value": "imports", "include": True},
])
scenario_spark = spark.createDataFrame(scenario_df)
write_lakehouse_table(scenario_spark, source_lakehouse_id, "scenario_definitions", mode="overwrite")
print(f"  scenario_definitions: {len(scenario_df)} rows")

# ── Summary ─────────────────────────────────────────────────────
total_rows = (len(orders_df) + len(shipments_df) + len(prod_history_df) +
              len(budget_df) + len(inv_df) + len(override_df) + len(adj_df) +
              len(signal_df) + len(scenario_df) + len(skus) + N_PLANTS +
              N_CUSTOMERS + N_MARKETS + N_LINES)

print(f"\n{'='*60}")
print(f"[test_data] COMPLETE -- {total_rows:,} total rows across all tables")
print(f"  Date range: {dates[0].date()} to {dates[-1].date()} ({HISTORY_MONTHS} months)")
print(f"  SKUs: {N_SKUS} ({len(sku_groups)} groups)")
print(f"  Plants: {N_PLANTS}, Lines: {N_LINES}")
print(f"  Customers: {N_CUSTOMERS}, Markets: {N_MARKETS}")
print(f"  Source lakehouse: {source_lakehouse_id}")
print(f"{'='*60}")
print(f"\nNext: Run 01_ingest_sources with source_lakehouse_id={source_lakehouse_id}")
