# Fabric Notebook
# 00_generate_test_data.py

# @parameters
source_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

N_SKUS = cfg("n_skus")
N_PLANTS = cfg("n_plants")
N_CUSTOMERS = cfg("n_customers")
N_MARKETS = cfg("n_markets")
N_LINES = cfg("n_production_lines")
HISTORY_MONTHS = cfg("history_months")
SEED = cfg("seed")

np.random.seed(SEED)
print(f"[test_data] Generating {HISTORY_MONTHS} months of history for "
      f"{N_SKUS} SKUs, {N_PLANTS} plants, {N_CUSTOMERS} customers, {N_MARKETS} markets")

end_date = pd.Timestamp(datetime.utcnow().replace(day=1))
dates = pd.date_range(end=end_date, periods=HISTORY_MONTHS, freq="MS")

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
        "sku_id": f"SKU-{i+1:04d}", "sku_name": f"Product {i+1}",
        "sku_group": sku_groups[i % len(sku_groups)],
        "base_width_inches": round(np.random.uniform(12, 72), 1),
        "base_weight_lbs": round(np.random.uniform(50, 500), 0),
        "unit_of_measure": "tons",
    })

customers = []
for i in range(N_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST-{i+1:03d}", "customer_name": f"Customer {i+1}",
        "market_id": markets[i % N_MARKETS], "region": plant_regions[i % N_PLANTS],
        "tier": np.random.choice(["A", "B", "C"], p=[0.2, 0.5, 0.3]),
    })

lines_data = []
for i in range(N_LINES):
    lines_data.append({
        "line_id": f"LINE-{i+1:02d}", "plant_id": plants[i % N_PLANTS],
        "line_name": f"Line {i+1}",
        "max_speed_fpm": round(np.random.uniform(100, 500), 0),
        "max_width_inches": round(np.random.uniform(48, 72), 0),
    })

print("[test_data] Writing master tables...")
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(skus)), source_lakehouse_id, "master_sku", mode="overwrite")
write_lakehouse_table(spark.createDataFrame(pd.DataFrame([
    {"plant_id": p, "plant_name": n, "region": r, "capacity_tons_month": round(np.random.uniform(5000, 20000), 0)}
    for p, n, r in zip(plants, plant_names, plant_regions)
])), source_lakehouse_id, "master_plant", mode="overwrite")
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(customers)), source_lakehouse_id, "master_customer", mode="overwrite")
write_lakehouse_table(spark.createDataFrame(pd.DataFrame([
    {"market_id": m, "market_name": n, "segment": s} for m, n, s in zip(markets, market_names, market_segments)
])), source_lakehouse_id, "master_market", mode="overwrite")
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(lines_data)), source_lakehouse_id, "production_lines", mode="overwrite")

orders_rows = []
for sku in skus:
    base_tons = np.random.uniform(10, 200)
    trend = np.random.uniform(-0.5, 1.5)
    seasonality_amp = np.random.uniform(0.1, 0.4) * base_tons
    phase = np.random.uniform(0, 2 * np.pi)
    assigned_plant = plants[hash(sku["sku_id"]) % N_PLANTS]
    assigned_customers = [customers[j] for j in range(N_CUSTOMERS) if j % N_SKUS == (int(sku["sku_id"].split("-")[1]) - 1) % N_CUSTOMERS]
    if not assigned_customers:
        assigned_customers = [customers[hash(sku["sku_id"]) % N_CUSTOMERS]]
    for t, date in enumerate(dates):
        seasonal = seasonality_amp * np.sin(2 * np.pi * (date.month - 1) / 12 + phase)
        tons = max(1, base_tons + trend * t + seasonal + np.random.normal(0, base_tons * 0.1))
        cust = assigned_customers[t % len(assigned_customers)]
        orders_rows.append({
            "period_date": str(date.date()), "plant_id": assigned_plant,
            "sku_id": sku["sku_id"], "sku_group": sku["sku_group"],
            "customer_id": cust["customer_id"], "market_id": cust["market_id"],
            "tons": round(tons, 2),
            "price_per_ton": round(np.random.uniform(500, 2000), 2),
            "lead_time_days": int(np.random.uniform(3, 30)),
            "promo_flag": int(np.random.random() < 0.15),
            "safety_stock_tons": round(base_tons * 0.1, 2),
        })
orders_spark = spark.createDataFrame(pd.DataFrame(orders_rows))
write_lakehouse_table(orders_spark, source_lakehouse_id, "orders", mode="overwrite")
print(f"  orders: {len(orders_rows)} rows")

shipments_rows = [dict(r, shipped_tons=round(r["tons"] * np.random.uniform(0.85, 1.0), 2),
                       ship_date=r["period_date"]) for r in orders_rows if np.random.random() < 0.9]
shipments_spark = spark.createDataFrame(pd.DataFrame(shipments_rows))
write_lakehouse_table(shipments_spark, source_lakehouse_id, "shipments", mode="overwrite")
print(f"  shipments: {len(shipments_rows)} rows")

prod_rows = []
for line in lines_data:
    for date in dates:
        prod_rows.append({
            "period_date": str(date.date()), "line_id": line["line_id"],
            "plant_id": line["plant_id"],
            "produced_tons": round(np.random.uniform(500, 3000), 2),
            "line_speed_fpm": round(line["max_speed_fpm"] * np.random.uniform(0.7, 1.0), 1),
            "width_inches": round(line["max_width_inches"] * np.random.uniform(0.8, 1.0), 1),
            "downtime_hours": round(np.random.exponential(5), 1),
        })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(prod_rows)), source_lakehouse_id, "production_history", mode="overwrite")
print(f"  production_history: {len(prod_rows)} rows")

budget_rows = []
for sku in skus:
    assigned_plant = plants[hash(sku["sku_id"]) % N_PLANTS]
    for date in dates[-12:]:
        budget_rows.append({
            "period_date": str(date.date()), "plant_id": assigned_plant,
            "sku_id": sku["sku_id"], "sku_group": sku["sku_group"],
            "budget_tons": round(np.random.uniform(10, 200) * (1 + np.random.normal(0, 0.1)), 2),
            "budget_revenue": round(np.random.uniform(5000, 200000), 2),
        })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(budget_rows)), source_lakehouse_id, "budget_volumes", mode="overwrite")
print(f"  budget_volumes: {len(budget_rows)} rows")

inv_rows = []
for sku in skus:
    assigned_plant = plants[hash(sku["sku_id"]) % N_PLANTS]
    for date in dates[-6:]:
        inv_rows.append({
            "period_date": str(date.date()), "plant_id": assigned_plant,
            "sku_id": sku["sku_id"],
            "on_hand_tons": round(np.random.uniform(5, 300), 2),
            "in_transit_tons": round(np.random.uniform(0, 50), 2),
            "safety_stock_tons": round(np.random.uniform(5, 50), 2),
        })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(inv_rows)), source_lakehouse_id, "inventory_finished_goods", mode="overwrite")
print(f"  inventory_finished_goods: {len(inv_rows)} rows")

override_rows = []
for sku in skus[:10]:
    for date in dates[-3:]:
        override_rows.append({
            "period_date": str(date.date()), "plant_id": plants[0],
            "sku_id": sku["sku_id"], "override_tons": round(np.random.uniform(50, 300), 2),
            "override_reason": np.random.choice(["promo", "contract", "seasonal"]),
            "submitted_by": "sales_team",
        })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(override_rows)), source_lakehouse_id, "sales_overrides", mode="overwrite")
print(f"  sales_overrides: {len(override_rows)} rows")

adj_rows = []
for mkt in markets:
    for date in dates[-6:]:
        adj_rows.append({
            "period_date": str(date.date()), "market_id": mkt,
            "scale_factor": round(np.random.uniform(0.8, 1.2), 3),
            "adjustment_reason": np.random.choice(["tariff", "demand_shift", "competition"]),
        })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(adj_rows)), source_lakehouse_id, "market_adjustments", mode="overwrite")
print(f"  market_adjustments: {len(adj_rows)} rows")

signal_rows = []
for date in dates:
    signal_rows.append({
        "period_date": str(date.date()),
        "construction_index": round(100 + np.random.normal(0, 10), 2),
        "interest_rate": round(np.random.uniform(3, 7), 2),
        "inflation_rate": round(np.random.uniform(1, 5), 2),
        "tariff_rate": round(np.random.uniform(0, 15), 2),
    })
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(signal_rows)), source_lakehouse_id, "external_signals", mode="overwrite")
print(f"  external_signals: {len(signal_rows)} rows")

scenario_rows = [
    {"scenario_id": "base", "scenario_name": "Base Case", "volume_multiplier": 1.0, "price_multiplier": 1.0},
    {"scenario_id": "bull", "scenario_name": "Bull Case", "volume_multiplier": 1.15, "price_multiplier": 1.05},
    {"scenario_id": "bear", "scenario_name": "Bear Case", "volume_multiplier": 0.85, "price_multiplier": 0.95},
    {"scenario_id": "tariff", "scenario_name": "Tariff Impact", "volume_multiplier": 0.90, "price_multiplier": 1.10},
]
write_lakehouse_table(spark.createDataFrame(pd.DataFrame(scenario_rows)), source_lakehouse_id, "scenario_definitions", mode="overwrite")
print(f"  scenario_definitions: {len(scenario_rows)} rows")

print(f"\n{'='*60}")
print(f"[test_data] COMPLETE - All tables written to source lakehouse")
print(f"  SKUs: {N_SKUS}, Plants: {N_PLANTS}")
print(f"  Customers: {N_CUSTOMERS}, Markets: {N_MARKETS}")
print(f"  Source lakehouse: {source_lakehouse_id}")
print(f"{'='*60}")
