# Fabric Notebook -- Module
# ibp_config.py -- Centralized IBP Forecast configuration
# Single source of truth for all pipeline parameters, thresholds, and table names.
# Lakehouse IDs are NOT here -- they are injected per-notebook at deploy time.

IBP_CONFIG = {
    # ── Data Schema ──────────────────────────────────────────────
    "date_column":              "period_date",
    "feature_date_column":      "period",
    "frequency":                "M",
    "target_column":     "tons",
    "grain_columns":     ["plant_id", "sku_id"],
    "extended_grains":   ["plant_id", "sku_group", "customer_id", "market_id"],
    "feature_columns":   ["price_per_ton", "lead_time_days", "promo_flag", "safety_stock_tons"],
    "source_tables":     ["orders", "shipments", "production_history",
                          "master_sku", "master_plant", "master_customer",
                          "master_market", "budget_volumes",
                          "inventory_finished_goods", "production_lines",
                          "sales_overrides", "market_adjustments",
                          "external_signals", "scenario_definitions"],

    # ── Forecasting ──────────────────────────────────────────────
    "forecast_horizon":    6,
    "test_split_ratio":    0.2,
    "min_series_length":   24,

    # ── SARIMA ───────────────────────────────────────────────────
    "sarima_order":            [1, 1, 1],
    "sarima_seasonal_order":   [1, 1, 1, 12],

    # ── Prophet ──────────────────────────────────────────────────
    "prophet_yearly_seasonality":  True,
    "prophet_weekly_seasonality":  False,
    "prophet_changepoint_prior":   0.05,

    # ── VAR ──────────────────────────────────────────────────────
    "var_maxlags":  12,
    "var_ic":       "aic",

    # ── Exponential Smoothing ────────────────────────────────────
    "exp_smoothing_trend":            "add",
    "exp_smoothing_seasonal":         "add",
    "exp_smoothing_seasonal_periods": 12,

    # ── Hyperparameter Tuning ──────────────────────────────────────
    "tuning_enabled":       True,
    "tuning_n_iter":        10,
    "tuning_n_splits":      3,
    "tuning_metric":        "rmse",

    # ── MLflow / Experiment Tracking ─────────────────────────────
    "experiment_name":          "ibp_demand_forecast",
    "registered_model_prefix":  "ibp_model",

    # ── Versioning ───────────────────────────────────────────────
    "output_table":       "forecast_versions",
    "keep_n_snapshots":   24,

    # ── Capacity Translation ─────────────────────────────────────
    "capacity_output_table":      "capacity_translation",
    "production_history_table":   "production_history",
    "rolling_months":             3,
    "tons_to_lf_factor":          2000,
    "width_column":               "width_inches",
    "speed_column":               "line_speed_fpm",
    "line_id_column":             "line_id",

    # ── Sales Overrides & Market Adjustments ─────────────────────
    "overrides_table":        "sales_overrides",
    "adjustments_table":      "market_adjustments",
    "default_scale_factor":   1.0,

    # ── Accuracy Tracking ────────────────────────────────────────
    "accuracy_table":  "accuracy_tracking",

    # ── Hierarchy / Aggregation ──────────────────────────────────
    "hierarchy_levels": ["market_id", "plant_id", "sku_group", "sku_id", "customer_id"],

    # ── Budget Comparison ────────────────────────────────────────
    "budget_table":               "budget_volumes",
    "comparison_output_table":    "budget_comparison",
    "over_forecast_threshold":    0.10,
    "under_forecast_threshold":  -0.10,

    # ── Phase 2: External Signals ────────────────────────────────
    "signal_columns":   ["construction_index", "interest_rate", "inflation_rate", "tariff_rate"],
    "signals_table":    "external_signals",

    # ── Phase 2: Scenario Modeling ───────────────────────────────
    "scenarios_table":  "scenario_definitions",

    # ── Phase 2: SKU Classification ──────────────────────────────
    "sku_classification_output_table":  "sku_classifications",
    "runner_threshold":                 0.8,
    "repeater_threshold":               0.95,
    "xyz_cv_threshold_x":               0.5,
    "xyz_cv_threshold_y":               1.0,

    # ── Semantic Model / Reporting ─────────────────────────────────
    "reporting_table":      "reporting_actuals_vs_forecast",
    "semantic_model_name":  "IBP Forecast Model",

    # ── Test Data Generation ─────────────────────────────────────
    "n_skus":               50,
    "n_plants":             5,
    "n_customers":          20,
    "n_markets":            4,
    "n_production_lines":   10,
    "history_months":       42,
    "seed":                 42,
}


def cfg(key: str, override=None):
    """Get a config value. Pipeline override takes priority over default."""
    if override is not None and override != "" and override != "None":
        return override
    return IBP_CONFIG.get(key)
