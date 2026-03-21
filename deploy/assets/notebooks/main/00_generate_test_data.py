# Fabric Notebook
# 00_generate_test_data.py -- Generate realistic synthetic data for end-to-end testing
# Writes all source tables to the source lakehouse so notebooks 01-16 + P2 can run.
#
# Realism features:
#   - Multi-regime demand (structural breaks, slope changes)
#   - Construction-driven seasonality (Q2-Q3 peaks, 20-50% amplitude)
#   - Correlated features (price elasticity, promo lifts, demand-driven lead times)
#   - Demand shocks (supply-chain disruptions, large-order spikes)
#   - Intermittent demand for specialty SKUs
#   - Cross-SKU cannibalization within same group
#   - Frequency-aware: works at M / W / D grain via freq_params()

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/schemas_module


source_lakehouse_id = resolve_lakehouse_id("", "source")

import pandas as pd
import numpy as np
from datetime import datetime


def _validated_write(df, table_name, lid=None):
    """Select only the columns defined in schemas_module, then write."""
    lid = lid or source_lakehouse_id
    expected = column_names(table_name)
    present = [c for c in expected if c in df.columns]
    sdf = spark.createDataFrame(df[present])
    write_lakehouse_table(sdf, lid, table_name, mode="overwrite")
    return len(df)

# ── Config ──────────────────────────────────────────────────────
N_SKUS = int(cfg("n_skus") or 50)
N_PLANTS = int(cfg("n_plants") or 5)
N_CUSTOMERS = int(cfg("n_customers") or 20)
N_MARKETS = int(cfg("n_markets") or 4)
N_LINES = int(cfg("n_production_lines") or 10)
HISTORY_PERIODS = int(cfg("history_periods") or 36)
SEED = int(cfg("seed") or 42)

FREQ = cfg("frequency")
FREQ_CODE = freq_params("code")
PPY = freq_params("periods_per_year")
OFFSET_KW = freq_params("offset_kwarg")

SHOCK_PROB = cfg("demand_shock_prob") or 0.06
INTERMITTENT_PCT = cfg("intermittent_pct") or 0.10
ELAST_RANGE = cfg("price_elasticity_range") or [-0.8, -0.3]
PROMO_RANGE = cfg("promo_lift_range") or [0.15, 0.40]

np.random.seed(SEED)
logger.info(f"[test_data] Generating {HISTORY_PERIODS} periods ({FREQ}) for "
            f"{N_SKUS} SKUs, {N_PLANTS} plants, {N_CUSTOMERS} customers, {N_MARKETS} markets")

# ── Date range ──────────────────────────────────────────────────
end_date = pd.Timestamp(datetime.utcnow().replace(day=1))
dates = pd.date_range(end=end_date, periods=HISTORY_PERIODS, freq=FREQ_CODE)

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
logger.info("[test_data] Writing master tables...")
_validated_write(pd.DataFrame(skus), "master_sku")

_validated_write(pd.DataFrame([
    {"plant_id": p, "plant_name": n, "region": r,
     "capacity_tons_month": round(np.random.uniform(5000, 20000), 0)}
    for p, n, r in zip(plants, plant_names, plant_regions)
]), "master_plant")

_validated_write(pd.DataFrame(customers), "master_customer")

_validated_write(pd.DataFrame([
    {"market_id": m, "market_name": n, "market_segment": s}
    for m, n, s in zip(markets, market_names, market_segments)
]), "master_market")

_validated_write(pd.DataFrame(lines), "production_lines")

logger.info(f"  master_sku: {len(skus)}, master_plant: {N_PLANTS}, "
            f"master_customer: {N_CUSTOMERS}, master_market: {N_MARKETS}, "
            f"production_lines: {N_LINES}")


# ═══════════════════════════════════════════════════════════════════
# DEMAND GENERATION ENGINE
# ═══════════════════════════════════════════════════════════════════

def _period_of_year(dt):
    """Return the period-within-year index for any frequency."""
    if FREQ == "D":
        return dt.dayofyear
    elif FREQ == "W":
        return dt.isocalendar()[1]
    else:
        return dt.month


def _seasonal_factor(dt, amplitude, phase):
    """Construction-driven seasonality: peaks in spring/summer (Q2-Q3)."""
    p = _period_of_year(dt)
    return amplitude * np.sin(2 * np.pi * p / PPY + phase)


def _generate_regimes(n_periods, rng):
    """Create 2-3 demand regimes with different trend slopes."""
    n_regimes = rng.choice([2, 3])
    breakpoints = sorted(rng.choice(range(n_periods // 4, 3 * n_periods // 4),
                                    size=n_regimes - 1, replace=False))
    breakpoints = [0] + list(breakpoints) + [n_periods]
    slopes = rng.uniform(-1.0, 2.5, size=n_regimes)
    trend = np.zeros(n_periods)
    for i in range(n_regimes):
        start, end = breakpoints[i], breakpoints[i + 1]
        segment_len = end - start
        trend[start:end] = slopes[i] * np.arange(segment_len)
        if i > 0:
            trend[start:end] += trend[start - 1]
    return trend


def _generate_shocks(n_periods, rng, shock_prob):
    """Random demand shocks: +/- 30-60% disruptions."""
    shocks = np.ones(n_periods)
    for t in range(n_periods):
        if rng.random() < shock_prob:
            shocks[t] = rng.uniform(0.4, 1.6)
    return shocks


logger.info("[test_data] Generating demand with realistic patterns...")

sku_rng = np.random.RandomState(SEED)
n_intermittent = int(N_SKUS * INTERMITTENT_PCT)

# Pre-compute per-SKU demand profiles
sku_demand_profiles = {}
for idx, sku in enumerate(skus):
    sid = sku["sku_id"]
    is_intermittent = idx >= (N_SKUS - n_intermittent)
    base_tons = sku_rng.uniform(30, 200) if not is_intermittent else sku_rng.uniform(5, 40)

    seasonality_amp = sku_rng.uniform(0.20, 0.50) * base_tons
    phase = sku_rng.uniform(-0.5, 0.5)

    trend = _generate_regimes(HISTORY_PERIODS, sku_rng)
    shocks = _generate_shocks(HISTORY_PERIODS, sku_rng, SHOCK_PROB)

    elasticity = sku_rng.uniform(ELAST_RANGE[0], ELAST_RANGE[1])
    promo_lift = sku_rng.uniform(PROMO_RANGE[0], PROMO_RANGE[1])

    base_price = sku_rng.uniform(900, 2400)
    price_group_premium = {"Specialty Alloy": 400, "Prepainted": 200,
                           "Aluminum": 150, "Galvanized": 50,
                           "Coated Steel": 0}.get(sku["sku_group"], 0)
    base_price += price_group_premium

    zero_mask = np.ones(HISTORY_PERIODS, dtype=bool)
    if is_intermittent:
        zero_frac = sku_rng.uniform(0.20, 0.40)
        zero_idx = sku_rng.choice(HISTORY_PERIODS,
                                  size=int(HISTORY_PERIODS * zero_frac), replace=False)
        zero_mask[zero_idx] = False

    sku_demand_profiles[sid] = {
        "base_tons": base_tons, "trend": trend, "shocks": shocks,
        "seasonality_amp": seasonality_amp, "phase": phase,
        "elasticity": elasticity, "promo_lift": promo_lift,
        "base_price": base_price, "zero_mask": zero_mask,
        "is_intermittent": is_intermittent,
    }


# ── Orders (main demand signal) ─────────────────────────────────
orders_rows = []
for sku in skus:
    sid = sku["sku_id"]
    profile = sku_demand_profiles[sid]

    assigned_plant = plants[hash(sid) % N_PLANTS]
    assigned_customers = sku_rng.choice(
        [c["customer_id"] for c in customers],
        size=min(5, N_CUSTOMERS), replace=False)
    assigned_market = markets[hash(sid) % N_MARKETS]

    trailing_demand = []

    for t, date in enumerate(dates):
        seasonal = _seasonal_factor(date, profile["seasonality_amp"], profile["phase"])
        trend_val = profile["trend"][t]
        shock = profile["shocks"][t]

        price_seasonal = profile["base_price"] * (1 + 0.05 * np.sin(
            2 * np.pi * _period_of_year(date) / PPY + 1.0))
        inflation_drift = profile["base_price"] * 0.003 * t
        price = round(price_seasonal + inflation_drift + sku_rng.normal(0, 30), 2)
        price = max(price, profile["base_price"] * 0.5)

        price_deviation = (price - profile["base_price"]) / profile["base_price"]
        price_effect = profile["elasticity"] * price_deviation * profile["base_tons"]

        is_promo = int(sku_rng.random() < 0.12)
        promo_effect = is_promo * profile["promo_lift"] * profile["base_tons"]

        noise = sku_rng.normal(0, profile["base_tons"] * 0.08)

        raw_tons = (profile["base_tons"] + seasonal + trend_val + price_effect
                    + promo_effect + noise) * shock

        if not profile["zero_mask"][t]:
            raw_tons = 0.0

        tons = round(max(0.0, raw_tons), 2)
        trailing_demand.append(tons)

        lead_base = 14 if FREQ == "M" else (3 if FREQ == "W" else 1)
        demand_pressure = tons / max(profile["base_tons"], 1)
        lead_time = max(1, int(sku_rng.normal(lead_base * demand_pressure, lead_base * 0.2)))

        window = min(3, len(trailing_demand))
        safety_stock = round(np.mean(trailing_demand[-window:]) * sku_rng.uniform(0.15, 0.30), 1)

        orders_rows.append({
            "period_date": str(date.date()),
            "plant_id": assigned_plant,
            "sku_id": sid,
            "sku_group": sku["sku_group"],
            "customer_id": sku_rng.choice(assigned_customers),
            "market_id": assigned_market,
            "tons": tons,
            "price_per_ton": price,
            "lead_time_days": lead_time,
            "promo_flag": is_promo,
            "safety_stock_tons": safety_stock,
        })

orders_df = pd.DataFrame(orders_rows)

# Cross-SKU cannibalization within same group
logger.info("[test_data] Applying cross-SKU cannibalization...")
for group in sku_groups:
    group_skus = [s["sku_id"] for s in skus if s["sku_group"] == group]
    if len(group_skus) < 2:
        continue
    for date in dates:
        mask = (orders_df["sku_group"] == group) & (orders_df["period_date"] == str(date.date()))
        group_tons = orders_df.loc[mask, "tons"]
        if group_tons.sum() == 0:
            continue
        group_mean = group_tons.mean()
        deviations = group_tons - group_mean
        cannib_factor = 0.05
        adjustment = -cannib_factor * deviations
        orders_df.loc[mask, "tons"] = np.maximum(0, group_tons + adjustment).round(2)

_validated_write(orders_df, "orders")
logger.info(f"  orders: {len(orders_df)} rows")


# ── Shipments (lagged, fulfillment rate varies) ─────────────────
logger.info("[test_data] Generating shipments...")
shipments_df = orders_df.copy()
fulfillment = np.where(
    orders_df["tons"] > orders_df.groupby("sku_id")["tons"].transform("quantile", 0.8),
    sku_rng.uniform(0.80, 0.95, len(orders_df)),
    sku_rng.uniform(0.92, 1.02, len(orders_df)),
)
shipments_df["tons"] = (shipments_df["tons"] * fulfillment).round(2)
lag_days = 7 if FREQ == "M" else (2 if FREQ == "W" else 1)
shipments_df["shipped_date"] = (pd.to_datetime(shipments_df["period_date"])
                                 + pd.Timedelta(days=lag_days)).astype(str)
_validated_write(shipments_df, "shipments")
logger.info(f"  shipments: {len(shipments_df)} rows")


# ── Production History (tied to orders with lag + efficiency) ───
logger.info("[test_data] Generating production history...")
prod_rows = []
for sku in skus:
    sid = sku["sku_id"]
    plant_id = plants[hash(sid) % N_PLANTS]
    line_id = f"LINE-{(hash(sid) % N_LINES) + 1:02d}"
    base_width = sku["base_width_inches"]
    base_speed = sku_rng.uniform(150, 400)

    sku_orders = orders_df[orders_df["sku_id"] == sid].sort_values("period_date")["tons"].values

    for t, date in enumerate(dates):
        lagged_demand = sku_orders[max(0, t - 1)] if t > 0 else sku_orders[0]
        efficiency = sku_rng.uniform(0.85, 1.10)
        prod_tons = round(lagged_demand * efficiency, 2)

        width = round(base_width + sku_rng.normal(0, 1.0), 1)
        speed = round(base_speed + sku_rng.normal(0, 10), 1)

        prod_rows.append({
            "period_date": str(date.date()),
            "plant_id": plant_id,
            "sku_id": sid,
            "line_id": line_id,
            "width_inches": max(6, width),
            "line_speed_fpm": max(50, speed),
            "produced_tons": max(0, prod_tons),
        })

prod_history_df = pd.DataFrame(prod_rows)
_validated_write(prod_history_df, "production_history")
logger.info(f"  production_history: {len(prod_history_df)} rows")


# ── Budget Volumes ──────────────────────────────────────────────
logger.info("[test_data] Generating budget volumes...")
budget_rows = []
budget_future = pd.date_range(start=end_date, periods=PPY, freq=FREQ_CODE)
all_budget_dates = list(dates[-max(6, PPY // 2):]) + list(budget_future)

for sku in skus:
    sid = sku["sku_id"]
    plant_id = plants[hash(sid) % N_PLANTS]
    market_id = markets[hash(sid) % N_MARKETS]
    profile = sku_demand_profiles[sid]
    base = profile["base_tons"]

    for date in all_budget_dates:
        seasonal = _seasonal_factor(date, profile["seasonality_amp"] * 0.8, profile["phase"])
        budget_tons = round(max(1, base + seasonal + sku_rng.normal(0, base * 0.12)), 2)
        budget_rows.append({
            "period": str(date.date()),
            "plant_id": plant_id,
            "sku_id": sid,
            "sku_group": sku["sku_group"],
            "market_id": market_id,
            "budget_tons": budget_tons,
        })

budget_df = pd.DataFrame(budget_rows)
_validated_write(budget_df, "budget_volumes")
logger.info(f"  budget_volumes: {len(budget_df)} rows")


# ── Finished Goods Inventory ────────────────────────────────────
logger.info("[test_data] Generating FG inventory...")
inv_rows = []
for sku in skus:
    sid = sku["sku_id"]
    plant_id = plants[hash(sid) % N_PLANTS]
    recent = orders_df[(orders_df["sku_id"] == sid)].tail(3)["tons"].mean()
    coverage_periods = sku_rng.uniform(0.5, 4.0)
    on_hand = round(recent * coverage_periods, 2)
    inv_rows.append({
        "sku_id": sid,
        "plant_id": plant_id,
        "sku_group": sku["sku_group"],
        "on_hand_tons": max(0, on_hand),
        "snapshot_date": str(end_date.date()),
    })

inv_df = pd.DataFrame(inv_rows)
_validated_write(inv_df, "inventory_finished_goods")
logger.info(f"  inventory_finished_goods: {len(inv_df)} rows")


# ── Sales Overrides ─────────────────────────────────────────────
logger.info("[test_data] Generating sales overrides...")
override_rows = []
future_dates = pd.date_range(start=end_date, periods=PPY, freq=FREQ_CODE)
for i in range(30):
    sku = skus[i % N_SKUS]
    plant_id = plants[hash(sku["sku_id"]) % N_PLANTS]
    period = future_dates[i % len(future_dates)]
    delta = round(sku_rng.uniform(-50, 80), 2)
    override_rows.append({
        "period": str(period.date()),
        "plant_id": plant_id,
        "sku_id": sku["sku_id"],
        "override_delta_tons": delta,
        "override_reason": sku_rng.choice(["large_order", "customer_request",
                                           "seasonal_push", "market_intelligence"]),
        "created_by": "sales_test_user",
    })

override_df = pd.DataFrame(override_rows)
_validated_write(override_df, "sales_overrides")
logger.info(f"  sales_overrides: {len(override_df)} rows")


# ── Market Adjustments ──────────────────────────────────────────
logger.info("[test_data] Generating market adjustments...")
adj_rows = []
for market_id in markets:
    for period in future_dates:
        factor = round(sku_rng.uniform(0.90, 1.15), 3)
        adj_rows.append({
            "market_id": market_id,
            "period": str(period.date()),
            "scale_factor": factor,
            "adjustment_reason": sku_rng.choice(["growth_outlook", "downturn_risk",
                                                 "tariff_impact", "stable"]),
        })

adj_df = pd.DataFrame(adj_rows)
_validated_write(adj_df, "market_adjustments")
logger.info(f"  market_adjustments: {len(adj_df)} rows")


# ── External Signals (Phase 2) ──────────────────────────────────
logger.info("[test_data] Generating external signals...")
signal_rows = []
for date in dates:
    p = _period_of_year(date)
    signal_rows.append({
        "period": str(date.date()),
        "construction_index": round(100 + 20 * np.sin(2 * np.pi * p / PPY - 0.3)
                                    + sku_rng.normal(0, 4), 2),
        "interest_rate": round(sku_rng.uniform(3.0, 7.5) + 0.002 * (date.year - 2023), 2),
        "inflation_rate": round(sku_rng.uniform(1.5, 6.0), 2),
        "tariff_rate": round(float(sku_rng.choice([0, 5, 10, 15, 25],
                                                   p=[0.3, 0.25, 0.2, 0.15, 0.1])), 1),
    })

signal_df = pd.DataFrame(signal_rows)
_validated_write(signal_df, "external_signals")
logger.info(f"  external_signals: {len(signal_df)} rows")


# ── Scenario Definitions (Phase 2) ──────────────────────────────
logger.info("[test_data] Generating scenario definitions...")
scenario_df = pd.DataFrame([
    {"scenario_name": "ncca_only", "filter_type": "market_segment",
     "filter_value": "NCCA", "include": True},
    {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
     "filter_value": "NCCA", "include": True},
    {"scenario_name": "ncca_plus_imports", "filter_type": "market_segment",
     "filter_value": "imports", "include": True},
])
_validated_write(scenario_df, "scenario_definitions")
logger.info(f"  scenario_definitions: {len(scenario_df)} rows")


# ── Summary ─────────────────────────────────────────────────────
total_rows = (len(orders_df) + len(shipments_df) + len(prod_history_df)
              + len(budget_df) + len(inv_df) + len(override_df) + len(adj_df)
              + len(signal_df) + len(scenario_df) + len(skus) + N_PLANTS
              + N_CUSTOMERS + N_MARKETS + N_LINES)

logger.info(f"\n{'='*60}")
logger.info(f"[test_data] COMPLETE -- {total_rows:,} total rows across all tables")
logger.info(f"  Frequency: {FREQ} ({FREQ_CODE}), Periods: {HISTORY_PERIODS}")
logger.info(f"  Date range: {dates[0].date()} to {dates[-1].date()}")
logger.info(f"  SKUs: {N_SKUS} ({len(sku_groups)} groups), "
            f"Intermittent: {n_intermittent}")
logger.info(f"  Plants: {N_PLANTS}, Lines: {N_LINES}")
logger.info(f"  Customers: {N_CUSTOMERS}, Markets: {N_MARKETS}")
logger.info(f"  Source lakehouse: {source_lakehouse_id}")
logger.info(f"{'='*60}")
