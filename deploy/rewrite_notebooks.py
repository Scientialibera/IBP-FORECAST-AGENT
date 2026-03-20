"""
Rewrite all main notebooks with enterprise-grade parameter handling.

- Parameter cell: ONLY lakehouse IDs (with __PLACEHOLDER__ for converter to inject)
- All other config: from centralized ibp_config via cfg()
- No hardcoded defaults scattered across notebooks
"""

import pathlib

MAIN_DIR = pathlib.Path(__file__).parent / "assets" / "notebooks" / "main"

# Each notebook definition: (filename, params, runs, code)
# params = list of lakehouse param names for the parameter cell
# runs = list of module names to %run
# code = the notebook body

NOTEBOOKS = {}


def nb(name, lakehouse_params, runs, code):
    NOTEBOOKS[name] = {
        "params": lakehouse_params,
        "runs": runs,
        "code": code,
    }


# ─────────────────────────────────────────────────────────────────
# 00 - Generate Test Data
# ─────────────────────────────────────────────────────────────────
nb("00_generate_test_data",
   ["source_lakehouse_id"],
   ["ibp_config", "config_module"],
   r'''
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
    plant_skus = [s for s in skus if plants[hash(s["sku_id"]) % N_PLANTS] == line["plant_id"]]
    if not plant_skus:
        plant_skus = skus[:3]
    for date in dates:
        sku = plant_skus[hash(str(date)) % len(plant_skus)]
        prod_rows.append({
            "period_date": str(date.date()), "line_id": line["line_id"],
            "plant_id": line["plant_id"], "sku_id": sku["sku_id"],
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
''')


# ─────────────────────────────────────────────────────────────────
# 01 - Ingest Sources
# ─────────────────────────────────────────────────────────────────
nb("01_ingest_sources",
   ["source_lakehouse_id", "landing_lakehouse_id"],
   ["ibp_config", "config_module"],
   '''
source_tables = cfg("source_tables")

print(f"[ingest] Source: {source_lakehouse_id} → Landing: {landing_lakehouse_id}")
print(f"[ingest] Tables: {source_tables}")

for table_name in source_tables:
    print(f"  Reading: {table_name}")
    df = read_lakehouse_table(spark, source_lakehouse_id, table_name)
    row_count = df.count()
    print(f"  {table_name}: {row_count} rows")
    write_lakehouse_table(df, landing_lakehouse_id, table_name, mode="overwrite")

print("[ingest] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 02 - Transform Bronze
# ─────────────────────────────────────────────────────────────────
nb("02_transform_bronze",
   ["landing_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module"],
   '''
source_tables = cfg("source_tables")

print(f"[bronze] Landing → Bronze")
for table_name in source_tables:
    print(f"  Processing: {table_name}")
    df = read_lakehouse_table(spark, landing_lakehouse_id, table_name)
    df_clean = df.dropDuplicates().dropna(how="all")
    row_count = df_clean.count()
    write_lakehouse_table(df_clean, bronze_lakehouse_id, table_name, mode="overwrite")
    print(f"  {table_name}: {row_count} rows written to bronze")

print("[bronze] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 03 - Feature Engineering
# ─────────────────────────────────────────────────────────────────
nb("03_feature_engineering",
   ["bronze_lakehouse_id", "silver_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "feature_engineering_module"],
   '''
date_column = cfg("date_column")
frequency = cfg("frequency")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
source_tables = cfg("source_tables")

print(f"[features] Bronze → Silver feature table")
orders_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders")
pdf = orders_df.toPandas()
print(f"[features] Loaded {len(pdf)} order rows")

feature_df = build_feature_table(
    pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, feature_columns=feature_columns, frequency=frequency,
)
print(f"[features] Feature table: {len(feature_df)} rows")

feature_spark = spark.createDataFrame(feature_df)
write_lakehouse_table(feature_spark, silver_lakehouse_id, "feature_table", mode="overwrite")
print("[features] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 04 - Train SARIMA
# ─────────────────────────────────────────────────────────────────
nb("04_train_sarima",
   ["silver_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "tuning_module", "train_sarima_module"],
   '''
date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
sarima_order = tuple(cfg("sarima_order"))
sarima_seasonal_order = tuple(cfg("sarima_seasonal_order"))
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")
tuning_enabled = cfg("tuning_enabled")
tuning_n_iter = cfg("tuning_n_iter")
tuning_n_splits = cfg("tuning_n_splits")
tuning_metric = cfg("tuning_metric")

print("[sarima] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[sarima] Loaded {len(pdf)} rows. Tuning: {tuning_enabled}")

results_df, agg_metrics = train_sarima_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, order=sarima_order,
    seasonal_order=sarima_seasonal_order, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_sarima",
    min_series_length=min_series_length,
    tuning_enabled=tuning_enabled, tuning_n_iter=tuning_n_iter,
    tuning_n_splits=tuning_n_splits, tuning_metric=tuning_metric,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "sarima_predictions", mode="overwrite")
print("[sarima] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 04 - Train Prophet
# ─────────────────────────────────────────────────────────────────
nb("04_train_prophet",
   ["silver_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "tuning_module", "train_prophet_module"],
   '''
date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
yearly = cfg("prophet_yearly_seasonality")
weekly = cfg("prophet_weekly_seasonality")
changepoint_prior = cfg("prophet_changepoint_prior")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")
tuning_enabled = cfg("tuning_enabled")
tuning_n_iter = cfg("tuning_n_iter")
tuning_n_splits = cfg("tuning_n_splits")
tuning_metric = cfg("tuning_metric")

print("[prophet] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[prophet] Loaded {len(pdf)} rows. Tuning: {tuning_enabled}")

results_df, agg_metrics = train_prophet_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, yearly_seasonality=yearly,
    weekly_seasonality=weekly, changepoint_prior=changepoint_prior,
    test_ratio=test_split_ratio, experiment_name=experiment_name,
    model_name=f"{model_prefix}_prophet", min_series_length=min_series_length,
    tuning_enabled=tuning_enabled, tuning_n_iter=tuning_n_iter,
    tuning_n_splits=tuning_n_splits, tuning_metric=tuning_metric,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "prophet_predictions", mode="overwrite")
print("[prophet] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 04 - Train VAR
# ─────────────────────────────────────────────────────────────────
nb("04_train_var",
   ["silver_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "tuning_module", "train_var_module"],
   '''
date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
test_split_ratio = cfg("test_split_ratio")
maxlags = cfg("var_maxlags")
ic = cfg("var_ic")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")
tuning_enabled = cfg("tuning_enabled")
tuning_n_iter = cfg("tuning_n_iter")
tuning_n_splits = cfg("tuning_n_splits")
tuning_metric = cfg("tuning_metric")

print("[var] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[var] Loaded {len(pdf)} rows. Tuning: {tuning_enabled}")

results_df, agg_metrics = train_var_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, feature_columns=feature_columns,
    maxlags=maxlags, ic=ic, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_var",
    min_series_length=min_series_length,
    tuning_enabled=tuning_enabled, tuning_n_iter=tuning_n_iter,
    tuning_n_splits=tuning_n_splits, tuning_metric=tuning_metric,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "var_predictions", mode="overwrite")
print("[var] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 04 - Train Exp Smoothing
# ─────────────────────────────────────────────────────────────────
nb("04_train_exp_smoothing",
   ["silver_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "tuning_module", "train_exp_smoothing_module"],
   '''
date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
test_split_ratio = cfg("test_split_ratio")
trend = cfg("exp_smoothing_trend")
seasonal = cfg("exp_smoothing_seasonal")
seasonal_periods = cfg("exp_smoothing_seasonal_periods")
experiment_name = cfg("experiment_name")
model_prefix = cfg("registered_model_prefix")
min_series_length = cfg("min_series_length")
tuning_enabled = cfg("tuning_enabled")
tuning_n_iter = cfg("tuning_n_iter")
tuning_n_splits = cfg("tuning_n_splits")
tuning_metric = cfg("tuning_metric")

print("[ets] Loading feature table.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[ets] Loaded {len(pdf)} rows. Tuning: {tuning_enabled}")

results_df, agg_metrics = train_exp_smoothing_per_grain(
    df=pdf, date_column=date_column, grain_columns=grain_columns,
    target_column=target_column, trend=trend, seasonal=seasonal,
    seasonal_periods=seasonal_periods, test_ratio=test_split_ratio,
    experiment_name=experiment_name, model_name=f"{model_prefix}_ets",
    min_series_length=min_series_length,
    tuning_enabled=tuning_enabled, tuning_n_iter=tuning_n_iter,
    tuning_n_splits=tuning_n_splits, tuning_metric=tuning_metric,
)

if not results_df.empty:
    write_lakehouse_table(spark.createDataFrame(results_df), silver_lakehouse_id, "ets_predictions", mode="overwrite")
print("[ets] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 05 - Score Forecast
# ─────────────────────────────────────────────────────────────────
nb("05_score_forecast",
   ["silver_lakehouse_id", "gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "scoring_module"],
   '''
import pandas as pd

date_column = cfg("feature_date_column")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
feature_columns = cfg("feature_columns")
forecast_horizon = cfg("forecast_horizon")
experiment_name = cfg("experiment_name")

print("[score] Loading feature table from silver.")
spark_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table")
pdf = spark_df.toPandas().dropna(subset=[target_column]).reset_index(drop=True)
print(f"[score] Loaded {len(pdf)} rows. Forecasting {forecast_horizon} periods ahead.")
print(f"[score] Loading trained models from MLflow experiment: {experiment_name}")

all_forecasts = []

print("[score] Forecasting with SARIMA (MLflow models)...")
try:
    sarima_fc = forecast_sarima_forward(pdf, date_column, grain_columns, target_column,
                                        forecast_horizon, tuple(cfg("sarima_order")),
                                        tuple(cfg("sarima_seasonal_order")),
                                        experiment_name=experiment_name)
    if not sarima_fc.empty:
        all_forecasts.append(sarima_fc)
        print(f"  SARIMA: {len(sarima_fc)} rows")
except Exception as e:
    print(f"  SARIMA failed: {e}")

print("[score] Forecasting with Prophet (MLflow models)...")
try:
    prophet_fc = forecast_prophet_forward(pdf, date_column, grain_columns, target_column,
                                          forecast_horizon, cfg("prophet_yearly_seasonality"),
                                          cfg("prophet_weekly_seasonality"),
                                          cfg("prophet_changepoint_prior"),
                                          experiment_name=experiment_name)
    if not prophet_fc.empty:
        all_forecasts.append(prophet_fc)
        print(f"  Prophet: {len(prophet_fc)} rows")
except Exception as e:
    print(f"  Prophet failed: {e}")

print("[score] Forecasting with VAR (MLflow models)...")
try:
    var_fc = forecast_var_forward(pdf, date_column, grain_columns, target_column,
                                  feature_columns, forecast_horizon,
                                  cfg("var_maxlags"), cfg("var_ic"),
                                  experiment_name=experiment_name)
    if not var_fc.empty:
        all_forecasts.append(var_fc)
        print(f"  VAR: {len(var_fc)} rows")
except Exception as e:
    print(f"  VAR failed: {e}")

print("[score] Forecasting with Exp Smoothing (MLflow models)...")
try:
    ets_fc = forecast_ets_forward(pdf, date_column, grain_columns, target_column,
                                   forecast_horizon, cfg("exp_smoothing_trend"),
                                   cfg("exp_smoothing_seasonal"),
                                   cfg("exp_smoothing_seasonal_periods"),
                                   experiment_name=experiment_name)
    if not ets_fc.empty:
        all_forecasts.append(ets_fc)
        print(f"  Exp Smoothing: {len(ets_fc)} rows")
except Exception as e:
    print(f"  Exp Smoothing failed: {e}")

if all_forecasts:
    combined = pd.concat(all_forecasts, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), silver_lakehouse_id, "raw_forecasts", mode="overwrite")
    print(f"[score] Wrote {len(combined)} raw forecast rows")
else:
    print("[score] WARNING: No forecasts produced.")
print("[score] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 06 - Version Snapshot
# ─────────────────────────────────────────────────────────────────
nb("06_version_snapshot",
   ["silver_lakehouse_id", "gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "versioning_module"],
   '''
output_table = cfg("output_table")
keep_n = cfg("keep_n_snapshots")

print(f"[version] Creating snapshot in gold.{output_table}")
raw_df = read_lakehouse_table(spark, silver_lakehouse_id, "raw_forecasts").toPandas()
print(f"[version] Read {len(raw_df)} raw forecast rows from silver")

if raw_df.empty:
    print("[version] WARNING: No raw forecasts to snapshot.")
else:
    versioned, vid = stamp_forecast_version(raw_df, version_type="system")
    print(f"[version] Stamped version {vid}, {len(versioned)} rows")
    append_versioned_forecast(spark, gold_lakehouse_id, output_table, versioned)
    purge_old_snapshots(spark, gold_lakehouse_id, output_table, keep_n=keep_n)
print("[version] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 07 - Demand to Capacity
# ─────────────────────────────────────────────────────────────────
nb("07_demand_to_capacity",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "capacity_module"],
   '''
forecast_table = cfg("output_table")
capacity_output = cfg("capacity_output_table")
prod_table = cfg("production_history_table")
width_col = cfg("width_column")
speed_col = cfg("speed_column")
line_id_col = cfg("line_id_column")
rolling_months = cfg("rolling_months")
tons_to_lf = cfg("tons_to_lf_factor")

print("[capacity] Loading forecast and production data.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
prod_df = read_lakehouse_table(spark, bronze_lakehouse_id, prod_table).toPandas()
print(f"[capacity] Forecast: {len(fc_df)} rows, Production: {len(prod_df)} rows")

prod_avgs = compute_rolling_production_averages(
    prod_df, width_column=width_col, speed_column=speed_col,
    line_id_column=line_id_col, plant_column="plant_id",
    sku_column="sku_id", date_column="period_date",
    rolling_months=rolling_months,
)
print(f"[capacity] Computed rolling averages: {len(prod_avgs)} rows")

capacity_df = translate_demand_to_capacity(
    fc_df, prod_avgs,
    plant_column="plant_id", sku_column="sku_id",
    line_id_column=line_id_col, tons_to_lf_factor=tons_to_lf,
)
write_lakehouse_table(spark.createDataFrame(capacity_df), gold_lakehouse_id, capacity_output, mode="overwrite")
print(f"[capacity] Wrote {len(capacity_df)} capacity rows")
print("[capacity] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 08 - Sales Overrides
# ─────────────────────────────────────────────────────────────────
nb("08_sales_overrides",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "override_module", "versioning_module"],
   '''
forecast_table = cfg("output_table")
overrides_table = cfg("overrides_table")
grain_columns = cfg("grain_columns")

import pandas as pd

print("[overrides] Loading forecast and overrides data.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
overrides_df = read_lakehouse_table(spark, bronze_lakehouse_id, overrides_table).toPandas()
print(f"[overrides] Forecast: {len(fc_df)} rows, Overrides: {len(overrides_df)} rows")

period_col = "period" if "period" in fc_df.columns else "period_date"
if period_col == "period" and "period" not in overrides_df.columns and "period_date" in overrides_df.columns:
    overrides_df["period"] = pd.to_datetime(overrides_df["period_date"]).dt.to_period("M").astype(str)
result = apply_sales_overrides(fc_df, overrides_df, grain_columns=grain_columns, period_column=period_col)

versioned, vid = stamp_forecast_version(result, version_type="sales_override")
append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned)
print(f"[overrides] Wrote {len(versioned)} override rows (version {vid})")
print("[overrides] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 09 - Market Adjustments
# ─────────────────────────────────────────────────────────────────
nb("09_market_adjustments",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "override_module", "versioning_module"],
   '''
forecast_table = cfg("output_table")
adj_table = cfg("adjustments_table")
scale = cfg("default_scale_factor")

import pandas as pd

print("[market] Loading forecast, orders (for market mapping), and adjustments.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
adj_df = read_lakehouse_table(spark, bronze_lakehouse_id, adj_table).toPandas()
print(f"[market] Forecast: {len(fc_df)} rows, Adjustments: {len(adj_df)} rows")

if "market_id" not in fc_df.columns:
    orders_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders").toPandas()
    market_map = orders_df[["plant_id", "sku_id", "market_id"]].drop_duplicates()
    market_map = market_map.groupby(["plant_id", "sku_id"])["market_id"].first().reset_index()
    fc_df = fc_df.merge(market_map, on=["plant_id", "sku_id"], how="left")
    print(f"[market] Enriched forecast with market_id from orders")

period_col = "period" if "period" in fc_df.columns else "period_date"
if period_col == "period" and "period" not in adj_df.columns and "period_date" in adj_df.columns:
    adj_df["period"] = pd.to_datetime(adj_df["period_date"]).dt.to_period("M").astype(str)
result = apply_market_adjustments(fc_df, adj_df, market_column="market_id",
                                  period_column=period_col, default_factor=scale)

versioned, vid = stamp_forecast_version(result, version_type="market_adjusted")
append_versioned_forecast(spark, gold_lakehouse_id, forecast_table, versioned)
print(f"[market] Wrote {len(versioned)} adjusted rows (version {vid})")
print("[market] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 10 - Consensus Build
# ─────────────────────────────────────────────────────────────────
nb("10_consensus_build",
   ["gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "override_module"],
   '''
forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")

print("[consensus] Building consensus forecast.")
all_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
print(f"[consensus] Total versioned rows: {len(all_df)}")

if all_df.empty:
    print("[consensus] WARNING: No forecast data.")
else:
    system_df = all_df[all_df["version_type"] == "system"] if "version_type" in all_df.columns else all_df
    sales_df = all_df[all_df["version_type"] == "sales_override"] if "version_type" in all_df.columns else None
    market_df = all_df[all_df["version_type"] == "market_adjusted"] if "version_type" in all_df.columns else None

    period_col = "period" if "period" in system_df.columns else "period_date"
    consensus = build_consensus(system_df, sales_df, market_df,
                                grain_columns=grain_columns, period_column=period_col)
    consensus["forecast_type"] = "consensus"

    write_lakehouse_table(spark.createDataFrame(consensus), gold_lakehouse_id, "consensus_forecast", mode="overwrite")
    print(f"[consensus] {len(consensus)} consensus rows written.")
print("[consensus] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 11 - Accuracy Tracking
# ─────────────────────────────────────────────────────────────────
nb("11_accuracy_tracking",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module", "accuracy_module"],
   '''
forecast_table = cfg("output_table")
accuracy_table = cfg("accuracy_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
date_column = cfg("feature_date_column")

import pandas as pd

print("[accuracy] Loading forecast versions and actuals.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
actuals_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders").toPandas()
print(f"[accuracy] Forecast: {len(fc_df)} rows, Actuals: {len(actuals_df)} rows")

if "period" in fc_df.columns and "period" not in actuals_df.columns and "period_date" in actuals_df.columns:
    actuals_df["period"] = pd.to_datetime(actuals_df["period_date"]).dt.to_period("M").astype(str)
    actuals_agg = actuals_df.groupby(grain_columns + [date_column], as_index=False)[target_column].sum()
else:
    actuals_agg = actuals_df

if fc_df.empty or actuals_agg.empty:
    print("[accuracy] WARNING: No data for accuracy tracking.")
else:
    accuracy_df = evaluate_forecast_accuracy(
        fc_df, actuals_agg, grain_columns=grain_columns,
        period_column=date_column, forecast_col="forecast_tons",
        actual_col=target_column,
    )
    if not accuracy_df.empty:
        write_lakehouse_table(spark.createDataFrame(accuracy_df), gold_lakehouse_id,
                              accuracy_table, mode="overwrite")
        print(f"[accuracy] Wrote {len(accuracy_df)} accuracy records")
    else:
        print("[accuracy] No matching forecast-actual pairs found")
print("[accuracy] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 12 - Aggregate Gold
# ─────────────────────────────────────────────────────────────────
nb("12_aggregate_gold",
   ["gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
forecast_table = cfg("output_table")
hierarchy = cfg("hierarchy_levels")

print(f"[aggregate] Aggregating gold.{forecast_table} across {hierarchy}")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
pdf = fc_df.toPandas()

numeric_cols = pdf.select_dtypes(include="number").columns.tolist()
agg_frames = []
for level in hierarchy:
    if level in pdf.columns:
        agg = pdf.groupby(level, as_index=False)[numeric_cols].sum()
        agg["hierarchy_level"] = level
        agg_frames.append(agg)

if agg_frames:
    import pandas as pd
    combined = pd.concat(agg_frames, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), gold_lakehouse_id, "aggregated_forecast", mode="overwrite")
    print(f"[aggregate] {len(combined)} aggregated rows")
print("[aggregate] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 13 - Budget Comparison
# ─────────────────────────────────────────────────────────────────
nb("13_budget_comparison",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
import pandas as pd

forecast_table = cfg("output_table")
budget_table = cfg("budget_table")
comparison_output = cfg("comparison_output_table")
over_thresh = cfg("over_forecast_threshold")
under_thresh = cfg("under_forecast_threshold")
grain_columns = cfg("grain_columns")

print("[budget] Comparing forecast vs budget.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
budget_df = read_lakehouse_table(spark, bronze_lakehouse_id, budget_table).toPandas()
print(f"[budget] Forecast: {len(fc_df)} rows ({list(fc_df.columns[:8])})")
print(f"[budget] Budget:   {len(budget_df)} rows ({list(budget_df.columns[:8])})")

fc_system = fc_df[fc_df["version_type"] == "system"].copy() if "version_type" in fc_df.columns else fc_df.copy()

if "period" in fc_system.columns and "period" not in budget_df.columns and "period_date" in budget_df.columns:
    budget_df["period"] = pd.to_datetime(budget_df["period_date"]).dt.to_period("M").astype(str)

merge_keys = [c for c in grain_columns if c in fc_system.columns and c in budget_df.columns]
date_key = "period" if "period" in fc_system.columns and "period" in budget_df.columns else (
    "period_date" if "period_date" in fc_system.columns and "period_date" in budget_df.columns else None)
if date_key:
    merge_keys.append(date_key)
print(f"[budget] Merge keys: {merge_keys}")

if merge_keys:
    merged = fc_system.merge(budget_df, on=merge_keys, how="inner", suffixes=("_fc", "_bgt"))
    print(f"[budget] Merged: {len(merged)} rows")
    fc_col = "forecast_tons" if "forecast_tons" in merged.columns else "tons"
    bgt_col = "budget_tons"
    if len(merged) > 0 and fc_col in merged.columns and bgt_col in merged.columns:
        merged["variance_pct"] = (merged[fc_col] - merged[bgt_col]) / merged[bgt_col].replace(0, float("nan"))
        merged["flag"] = merged["variance_pct"].apply(
            lambda v: "over" if v > over_thresh else ("under" if v < under_thresh else "ok"))
        write_lakehouse_table(spark.createDataFrame(merged), gold_lakehouse_id, comparison_output, mode="overwrite")
        print(f"[budget] {len(merged)} comparison rows written")
    else:
        print(f"[budget] No matching data or missing columns. fc_col={fc_col}, bgt_col={bgt_col}")
else:
    print("[budget] No common columns for merge")
print("[budget] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 14 - Build Reporting View (Actuals vs Forecast for Semantic Model)
# ─────────────────────────────────────────────────────────────────
nb("14_build_reporting_view",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
import pandas as pd
import numpy as np
from datetime import datetime

forecast_table = cfg("output_table")
reporting_table = cfg("reporting_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")

print("[reporting] Building unified actuals-vs-forecast reporting view.")

fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
print(f"[reporting] Forecast versions: {len(fc_df)} rows")

actuals_df = read_lakehouse_table(spark, bronze_lakehouse_id, "orders").toPandas()
print(f"[reporting] Actuals (orders): {len(actuals_df)} rows")

if "period_date" in actuals_df.columns and "period" not in actuals_df.columns:
    actuals_df["period"] = pd.to_datetime(actuals_df["period_date"]).dt.to_period("M").astype(str)

date_col = "period" if "period" in fc_df.columns else "period_date"
actuals_agg = actuals_df.groupby(grain_columns + [date_col], as_index=False)[target_column].sum()
actuals_agg = actuals_agg.rename(columns={target_column: "actual_tons"})
actuals_agg["record_type"] = "actual"

forecast_rows = []
for vtype in fc_df["version_type"].unique() if "version_type" in fc_df.columns else ["forecast"]:
    subset = fc_df[fc_df["version_type"] == vtype].copy() if "version_type" in fc_df.columns else fc_df.copy()
    for _, row in subset.iterrows():
        r = {c: row.get(c) for c in grain_columns if c in row.index}
        r[date_col] = row.get(date_col, row.get("period", row.get("period_date")))
        r["forecast_tons"] = row.get("forecast_tons", row.get("tons", None))
        r["model_type"] = row.get("model_type", "unknown")
        r["version_type"] = vtype
        r["version_id"] = row.get("version_id", "")
        forecast_rows.append(r)
forecast_clean = pd.DataFrame(forecast_rows)

merge_keys = grain_columns + [date_col]
valid_keys = [k for k in merge_keys if k in actuals_agg.columns and k in forecast_clean.columns]

if valid_keys:
    reporting = forecast_clean.merge(actuals_agg[valid_keys + ["actual_tons"]],
                                     on=valid_keys, how="outer")
else:
    reporting = forecast_clean.copy()
    reporting["actual_tons"] = np.nan

reporting["forecast_tons"] = pd.to_numeric(reporting.get("forecast_tons"), errors="coerce")
reporting["actual_tons"] = pd.to_numeric(reporting.get("actual_tons"), errors="coerce")

mask = reporting["forecast_tons"].notna() & reporting["actual_tons"].notna()
reporting.loc[mask, "abs_error"] = (reporting.loc[mask, "forecast_tons"] - reporting.loc[mask, "actual_tons"]).abs()
reporting.loc[mask, "pct_error"] = reporting.loc[mask, "abs_error"] / reporting.loc[mask, "actual_tons"].replace(0, np.nan)
reporting.loc[mask, "variance"] = reporting.loc[mask, "forecast_tons"] - reporting.loc[mask, "actual_tons"]

reporting["is_future"] = reporting["actual_tons"].isna() & reporting["forecast_tons"].notna()
reporting["snapshot_date"] = datetime.utcnow().strftime("%Y-%m-%d")

write_lakehouse_table(spark.createDataFrame(reporting), gold_lakehouse_id, reporting_table, mode="overwrite")
print(f"[reporting] Wrote {len(reporting)} reporting rows to gold.{reporting_table}")

future_count = reporting["is_future"].sum()
historical_count = (~reporting["is_future"]).sum()
print(f"  Historical (actual+forecast): {historical_count}")
print(f"  Future (forecast only):       {future_count}")
print("[reporting] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# 15 - Create / Update Semantic Model (DirectLake over gold)
# ─────────────────────────────────────────────────────────────────
nb("15_create_semantic_model",
   ["gold_lakehouse_id"],
   ["ibp_config", "config_module"],
   '''
import json

semantic_model_name = cfg("semantic_model_name")
reporting_table = cfg("reporting_table")
print(f"[semantic] Creating/updating semantic model: {semantic_model_name}")
print(f"[semantic] Gold lakehouse: {gold_lakehouse_id}")

bim = {
    "compatibilityLevel": 1604,
    "model": {
        "name": semantic_model_name,
        "culture": "en-US",
        "defaultMode": "directLake",
        "tables": [
            {
                "name": "Reporting Actuals vs Forecast",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons"},
                    {"name": "actual_tons",    "dataType": "double",  "sourceColumn": "actual_tons"},
                    {"name": "abs_error",      "dataType": "double",  "sourceColumn": "abs_error"},
                    {"name": "pct_error",      "dataType": "double",  "sourceColumn": "pct_error"},
                    {"name": "variance",       "dataType": "double",  "sourceColumn": "variance"},
                    {"name": "model_type",     "dataType": "string",  "sourceColumn": "model_type"},
                    {"name": "version_type",   "dataType": "string",  "sourceColumn": "version_type"},
                    {"name": "version_id",     "dataType": "string",  "sourceColumn": "version_id"},
                    {"name": "is_future",      "dataType": "boolean", "sourceColumn": "is_future"},
                    {"name": "snapshot_date",  "dataType": "string",  "sourceColumn": "snapshot_date"},
                ],
                "measures": [
                    {"name": "Total Forecast Tons",   "expression": "SUM(\'Reporting Actuals vs Forecast\'[forecast_tons])"},
                    {"name": "Total Actual Tons",     "expression": "SUM(\'Reporting Actuals vs Forecast\'[actual_tons])"},
                    {"name": "Total Variance",        "expression": "[Total Forecast Tons] - [Total Actual Tons]"},
                    {"name": "MAPE %",                "expression": "DIVIDE(SUM(\'Reporting Actuals vs Forecast\'[abs_error]), SUM(\'Reporting Actuals vs Forecast\'[actual_tons]), BLANK()) * 100", "formatString": "0.0"},
                    {"name": "Bias %",                "expression": "DIVIDE([Total Variance], [Total Actual Tons], BLANK()) * 100", "formatString": "0.0"},
                    {"name": "Forecast Accuracy %",   "expression": "100 - [MAPE %]", "formatString": "0.0"},
                    {"name": "Future Forecast Tons",  "expression": "CALCULATE(SUM(\'Reporting Actuals vs Forecast\'[forecast_tons]), \'Reporting Actuals vs Forecast\'[is_future] = TRUE())"},
                    {"name": "Historical Forecast Tons", "expression": "CALCULATE(SUM(\'Reporting Actuals vs Forecast\'[forecast_tons]), \'Reporting Actuals vs Forecast\'[is_future] = FALSE())"},
                ],
                "partitions": [{"name": "reporting_actuals_vs_forecast", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "reporting_actuals_vs_forecast",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Forecast Versions",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons"},
                    {"name": "model_type",     "dataType": "string",  "sourceColumn": "model_type"},
                    {"name": "version_type",   "dataType": "string",  "sourceColumn": "version_type"},
                    {"name": "version_id",     "dataType": "string",  "sourceColumn": "version_id"},
                    {"name": "snapshot_month", "dataType": "string",  "sourceColumn": "snapshot_month"},
                ],
                "partitions": [{"name": "forecast_versions", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "forecast_versions",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Accuracy Tracking",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons"},
                    {"name": "actual_tons",    "dataType": "double",  "sourceColumn": "actual_tons"},
                    {"name": "abs_error",      "dataType": "double",  "sourceColumn": "abs_error"},
                    {"name": "pct_error",      "dataType": "double",  "sourceColumn": "pct_error"},
                ],
                "partitions": [{"name": "accuracy_tracking", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "accuracy_tracking",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Consensus Forecast",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons"},
                    {"name": "forecast_type",  "dataType": "string",  "sourceColumn": "forecast_type"},
                ],
                "partitions": [{"name": "consensus_forecast", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "consensus_forecast",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Master SKU",
                "columns": [
                    {"name": "sku_id",    "dataType": "string", "sourceColumn": "sku_id"},
                    {"name": "sku_name",  "dataType": "string", "sourceColumn": "sku_name"},
                    {"name": "sku_group", "dataType": "string", "sourceColumn": "sku_group"},
                ],
                "partitions": [{"name": "master_sku", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "master_sku",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Master Plant",
                "columns": [
                    {"name": "plant_id",   "dataType": "string", "sourceColumn": "plant_id"},
                    {"name": "plant_name", "dataType": "string", "sourceColumn": "plant_name"},
                    {"name": "region",     "dataType": "string", "sourceColumn": "region"},
                ],
                "partitions": [{"name": "master_plant", "mode": "directLake",
                                "source": {"type": "entity", "entityName": "master_plant",
                                           "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
        ],
        "relationships": [
            {"name": "FK_Reporting_SKU",   "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_Reporting_Plant", "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
            {"name": "FK_FV_SKU",          "fromTable": "Forecast Versions",             "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_FV_Plant",        "fromTable": "Forecast Versions",             "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
            {"name": "FK_Consensus_SKU",   "fromTable": "Consensus Forecast",            "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_Consensus_Plant", "fromTable": "Consensus Forecast",            "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
        ],
        "expressions": [
            {
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": "let\\n    database = Sql.Database(\\\"__GOLD_SQL_ENDPOINT__\\\", \\\"__GOLD_LAKEHOUSE_NAME__\\\")\\nin\\n    database",
            }
        ],
    },
}

try:
    import sempy.fabric as fabric

    gold_properties = fabric.resolve_item_id(gold_lakehouse_id) if hasattr(fabric, "resolve_item_id") else None
    print(f"[semantic] Creating semantic model via sempy...")
    try:
        fabric.create_semantic_model_from_bim(dataset=semantic_model_name, bim_file=bim)
        print(f"[semantic] Created semantic model: {semantic_model_name}")
    except Exception as e1:
        if "already exists" in str(e1).lower():
            print(f"[semantic] Model exists, updating...")
            fabric.update_semantic_model_from_bim(dataset=semantic_model_name, bim_file=bim)
            print(f"[semantic] Updated semantic model: {semantic_model_name}")
        else:
            raise e1
except ImportError:
    print("[semantic] WARNING: sempy not available. Saving BIM to lakehouse for manual import.")
    bim_json = json.dumps(bim, indent=2)
    bim_path = lakehouse_table_path(gold_lakehouse_id, "_semantic_model_bim").replace("/Tables/", "/Files/") + ".json"
    dbutils.fs.put(bim_path, bim_json, overwrite=True)
    print(f"[semantic] BIM saved to: {bim_path}")
except Exception as e:
    print(f"[semantic] WARNING: Semantic model creation failed: {e}")
    print("[semantic] You can manually create it from the gold lakehouse in the Fabric UI.")
    bim_json = json.dumps(bim, indent=2)
    bim_path = lakehouse_table_path(gold_lakehouse_id, "_semantic_model_bim").replace("/Tables/", "/Files/") + ".json"
    try:
        dbutils.fs.put(bim_path, bim_json, overwrite=True)
        print(f"[semantic] BIM definition saved to: {bim_path}")
    except Exception:
        pass

print("[semantic] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# P2_01 - External Signals
# ─────────────────────────────────────────────────────────────────
nb("P2_01_external_signals",
   ["silver_lakehouse_id", "bronze_lakehouse_id", "gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
signal_columns = cfg("signal_columns")
signals_table = cfg("signals_table")

print("[signals] Enriching feature table with external signals.")
signals_df = read_lakehouse_table(spark, bronze_lakehouse_id, signals_table).toPandas()
feature_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table").toPandas()

if "period_date" in feature_df.columns and "period_date" in signals_df.columns:
    enriched = feature_df.merge(signals_df, on="period_date", how="left")
    write_lakehouse_table(spark.createDataFrame(enriched), gold_lakehouse_id, "enriched_features", mode="overwrite")
    print(f"[signals] {len(enriched)} enriched rows written to gold")
else:
    print("[signals] WARNING: period_date not found for merge")
print("[signals] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# P2_02 - Scenario Modeling
# ─────────────────────────────────────────────────────────────────
nb("P2_02_scenario_modeling",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
import pandas as pd

forecast_table = cfg("output_table")
scenarios_table = cfg("scenarios_table")
grain_columns = cfg("grain_columns")

print("[scenarios] Applying scenario multipliers.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
sc_df = read_lakehouse_table(spark, bronze_lakehouse_id, scenarios_table).toPandas()

scenario_results = []
for _, scenario in sc_df.iterrows():
    s = fc_df.copy()
    s["scenario_id"] = scenario.get("scenario_id", "unknown")
    s["scenario_name"] = scenario.get("scenario_name", "")
    vol_mult = float(scenario.get("volume_multiplier", 1.0))
    if "tons" in s.columns:
        s["tons"] = s["tons"] * vol_mult
    scenario_results.append(s)

if scenario_results:
    combined = pd.concat(scenario_results, ignore_index=True)
    write_lakehouse_table(spark.createDataFrame(combined), gold_lakehouse_id, "scenario_forecasts", mode="overwrite")
    print(f"[scenarios] {len(combined)} scenario forecast rows")
print("[scenarios] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# P2_03 - SKU Classification
# ─────────────────────────────────────────────────────────────────
nb("P2_03_sku_classification",
   ["silver_lakehouse_id", "gold_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
import pandas as pd
import numpy as np

target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
output_table = cfg("sku_classification_output_table")
runner_thresh = cfg("runner_threshold")
repeater_thresh = cfg("repeater_threshold")
xyz_x = cfg("xyz_cv_threshold_x")
xyz_y = cfg("xyz_cv_threshold_y")

print("[sku_class] Classifying SKUs (ABC-XYZ + Runner/Repeater/Stranger).")
feature_df = read_lakehouse_table(spark, silver_lakehouse_id, "feature_table").toPandas()

if target_column in feature_df.columns and grain_columns[0] in feature_df.columns:
    sku_stats = feature_df.groupby(grain_columns).agg(
        total_volume=(target_column, "sum"),
        n_periods=(target_column, "count"),
        cv=(target_column, lambda x: x.std() / x.mean() if x.mean() > 0 else float("inf")),
    ).reset_index()

    total = sku_stats["total_volume"].sum()
    sku_stats = sku_stats.sort_values("total_volume", ascending=False)
    sku_stats["cumulative_pct"] = sku_stats["total_volume"].cumsum() / total

    sku_stats["abc_class"] = np.where(sku_stats["cumulative_pct"] <= 0.8, "A",
                             np.where(sku_stats["cumulative_pct"] <= 0.95, "B", "C"))
    sku_stats["xyz_class"] = np.where(sku_stats["cv"] <= xyz_x, "X",
                             np.where(sku_stats["cv"] <= xyz_y, "Y", "Z"))

    max_periods = sku_stats["n_periods"].max()
    sku_stats["frequency_pct"] = sku_stats["n_periods"] / max_periods
    sku_stats["rrs_class"] = np.where(sku_stats["frequency_pct"] >= repeater_thresh, "Repeater",
                             np.where(sku_stats["frequency_pct"] >= runner_thresh, "Runner", "Stranger"))

    write_lakehouse_table(spark.createDataFrame(sku_stats), gold_lakehouse_id, output_table, mode="overwrite")
    print(f"[sku_class] {len(sku_stats)} SKU classifications written")
print("[sku_class] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# P2_04 - Inventory Alignment
# ─────────────────────────────────────────────────────────────────
nb("P2_04_inventory_alignment",
   ["gold_lakehouse_id", "bronze_lakehouse_id"],
   ["ibp_config", "config_module", "utils_module"],
   '''
forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")

print("[inventory] Aligning forecast with inventory positions.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
inv_df = read_lakehouse_table(spark, bronze_lakehouse_id, "inventory_finished_goods").toPandas()

common_cols = [c for c in grain_columns if c in fc_df.columns and c in inv_df.columns]
if common_cols:
    inv_latest = inv_df.sort_values("period_date").groupby(common_cols).last().reset_index()
    aligned = fc_df.merge(inv_latest[common_cols + ["on_hand_tons", "safety_stock_tons"]],
                          on=common_cols, how="left")
    if "tons" in aligned.columns and "on_hand_tons" in aligned.columns:
        aligned["net_requirement"] = aligned["tons"] - aligned["on_hand_tons"].fillna(0)
        aligned["stock_coverage_flag"] = (aligned["on_hand_tons"].fillna(0) >= aligned["safety_stock_tons"].fillna(0)).astype(int)

    write_lakehouse_table(spark.createDataFrame(aligned), gold_lakehouse_id, "inventory_aligned_forecast", mode="overwrite")
    print(f"[inventory] {len(aligned)} aligned rows")
else:
    print("[inventory] WARNING: No common grain columns for merge")
print("[inventory] Complete.")
''')


# ─────────────────────────────────────────────────────────────────
# GENERATE FILES
# ─────────────────────────────────────────────────────────────────

def generate_notebook(name: str, spec: dict) -> str:
    """Generate clean Python source for a notebook."""
    lines = [
        f"# Fabric Notebook",
        f"# {name}.py",
        "",
    ]

    # Parameter cell with lakehouse IDs only
    lines.append("# @parameters")
    for p in spec["params"]:
        lines.append(f'{p} = ""')
    lines.append("# @end_parameters")
    lines.append("")

    # %run directives
    for module in spec["runs"]:
        lines.append(f"# %run ../modules/{module}")
    lines.append("")

    # Code body
    code = spec["code"].strip()
    lines.append(code)
    lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    print("Rewriting all main notebooks...\n")
    for name, spec in NOTEBOOKS.items():
        content = generate_notebook(name, spec)
        path = MAIN_DIR / f"{name}.py"
        path.write_text(content, encoding="utf-8")
        print(f"  {name}.py  ({len(spec['params'])} params, {len(spec['runs'])} runs, {len(content.splitlines())} lines)")
    print(f"\nRewrote {len(NOTEBOOKS)} notebooks.")
