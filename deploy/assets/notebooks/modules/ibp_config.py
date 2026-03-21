# Fabric Notebook -- Module
# ibp_config.py -- Centralized IBP Forecast configuration
# Single source of truth for all pipeline parameters, thresholds, and table names.
# Lakehouse IDs are NOT here -- they are injected per-notebook at deploy time.

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ibp")
logger.setLevel(logging.INFO)

# ═════════════════════════════════════════════════════════════════════
# Frequency-derived parameters.  Change "frequency" in IBP_CONFIG to
# "W" or "D" and every seasonal period, lag window, date offset, etc.
# adapts automatically.  Individual keys in IBP_CONFIG can still
# override any derived value.
# ═════════════════════════════════════════════════════════════════════

FREQ_MAP = {
    "M": {
        "code":              "MS",
        "seasonal_periods":  12,
        "periods_per_year":  12,
        "default_lags":      [1, 2, 3, 6, 12],
        "default_rolling":   [3, 6, 12],
        "offset_kwarg":      "months",
        "snapshot_fmt":      "%Y-%m",
        "min_train_periods": 24,
        "var_maxlags":       12,
        "sarima_seasonal_s": 12,
        "tuning_grid_maxlags": [4, 6, 8, 12],
        "prophet_freq":      "MS",
    },
    "W": {
        "code":              "W-MON",
        "seasonal_periods":  52,
        "periods_per_year":  52,
        "default_lags":      [1, 2, 4, 13, 52],
        "default_rolling":   [4, 13, 52],
        "offset_kwarg":      "weeks",
        "snapshot_fmt":      "%Y-W%W",
        "min_train_periods": 104,
        "var_maxlags":       13,
        "sarima_seasonal_s": 52,
        "tuning_grid_maxlags": [4, 8, 13, 26],
        "prophet_freq":      "W",
    },
    "D": {
        "code":              "D",
        "seasonal_periods":  365,
        "periods_per_year":  365,
        "default_lags":      [1, 7, 14, 30, 365],
        "default_rolling":   [7, 30, 90],
        "offset_kwarg":      "days",
        "snapshot_fmt":      "%Y-%m-%d",
        "min_train_periods": 365,
        "var_maxlags":       30,
        "sarima_seasonal_s": 7,
        "tuning_grid_maxlags": [7, 14, 30],
        "prophet_freq":      "D",
    },
}

IBP_CONFIG = {
    # ── Naming Convention ────────────────────────────────────────
    # prefix/suffix applied to lakehouses, experiments, semantic models, reports.
    # Set suffix="_dev" / "_staging" / "_prod" to isolate environments.
    "naming_prefix":  "",
    "naming_suffix":  "",

    # ── Data Schema ──────────────────────────────────────────────
    "date_column":              "period_date",
    "feature_date_column":      "period",
    "frequency":                "W",
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

    # ── Enabled Models ────────────────────────────────────────────
    # Controls which 04_train_* notebooks run, which models are scored,
    # and which prediction tables are unioned for reporting.
    "models_enabled": ["sarima", "prophet", "var", "exp_smoothing", "lightgbm"],

    # ── Forecasting ──────────────────────────────────────────────
    "forecast_horizon":    52,
    "test_split_ratio":    0.2,

    # ── SARIMA ───────────────────────────────────────────────────
    "sarima_order":            [1, 1, 1],

    # ── Prophet ──────────────────────────────────────────────────
    "prophet_yearly_seasonality":  True,
    "prophet_weekly_seasonality":  False,
    "prophet_changepoint_prior":   0.30,
    "prophet_seasonality_mode":    "multiplicative",
    "prophet_yearly_fourier_order": 5,

    # ── VAR ──────────────────────────────────────────────────────
    "var_ic":       "aic",

    # ── Exponential Smoothing ────────────────────────────────────
    "exp_smoothing_trend":            "add",
    "exp_smoothing_seasonal":         "add",

    # ── LightGBM (global pooled) ─────────────────────────────────
    "lightgbm_n_estimators":      800,
    "lightgbm_max_depth":         7,
    "lightgbm_learning_rate":     0.02,
    "lightgbm_num_leaves":        40,
    "lightgbm_min_child_samples": 10,

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
    "rolling_periods":            3,
    "tons_to_lf_factor":          2000,
    "width_column":               "width_inches",
    "speed_column":               "line_speed_fpm",
    "line_id_column":             "line_id",

    # ── Sales Overrides & Market Adjustments ─────────────────────
    "overrides_table":        "sales_overrides",
    "adjustments_table":      "market_adjustments",
    "default_scale_factor":   1.0,

    # ── Tables: Intermediate / Pipeline ───────────────────────────
    "feature_table":            "feature_table",
    "raw_forecasts_table":      "raw_forecasts",
    "primary_table":            "orders",
    "prediction_tables":        [],  # derived from models_enabled after dict init
    "backtest_predictions_table": "backtest_predictions",
    "dimension_tables":         ["master_sku", "master_plant"],
    "model_recommendations_table": "model_recommendations",
    "agg_grand_total_table":    "agg_grand_total",
    "version_types":            ["system", "sales", "consensus"],

    # ── Accuracy Tracking ────────────────────────────────────────
    "accuracy_table":                   "accuracy_tracking",
    "accuracy_recommendation_metric":   "mape",

    # ── Hierarchy / Aggregation ──────────────────────────────────
    "hierarchy_levels": ["market_id", "plant_id", "sku_group", "sku_id", "customer_id"],

    # ── Budget Comparison ────────────────────────────────────────
    "budget_table":               "budget_volumes",
    "comparison_output_table":    "budget_comparison",
    "over_forecast_threshold":    0.10,
    "under_forecast_threshold":  -0.10,

    # ── Phase 2: External Signals ────────────────────────────────
    "external_signals_enabled":   False,
    "signal_columns":   ["construction_index", "interest_rate", "inflation_rate", "tariff_rate"],
    "signals_table":    "external_signals",
    "signal_importance_table":    "signal_importance",
    "feature_table_enriched":     "feature_table_enriched",

    # ── Phase 2: Scenario Modeling ───────────────────────────────
    "scenarios_enabled":          False,
    "scenarios_table":            "scenario_definitions",
    "scenario_comparison_table":  "scenario_comparison",

    # ── Phase 2: SKU Classification ──────────────────────────────
    "sku_classification_enabled":       False,
    "sku_classification_output_table":  "sku_classifications",
    "runner_threshold":                 0.8,
    "repeater_threshold":               0.95,
    "xyz_cv_threshold_x":               0.5,
    "xyz_cv_threshold_y":               1.0,

    # ── Phase 2: Inventory Alignment ──────────────────────────────
    "inventory_table":                       "inventory_finished_goods",
    "inventory_alignment_table":             "inventory_alignment",
    "inventory_near_term_periods":           3,
    "inventory_stockout_threshold_periods":  1,
    "inventory_overbuild_threshold_periods": 6,

    # ── Semantic Model / Reporting ─────────────────────────────────
    "reporting_table":      "reporting_actuals_vs_forecast",
    "semantic_model_name":  "IBP Forecast Model",
    "refresh_schedule_enabled": True,
    "refresh_schedule_time":    "06:00",
    "refresh_schedule_timezone": "UTC",
    "semantic_models_folder":   "semantic_models",
    "reports_folder":           "reports",
    "report_name":              "IBP Backtest - Actual vs Predicted",
    "report_description":       "Generated PBIR-Legacy report for IBP backtest analysis.",

    # ── Lakehouse Names (used by resolve_lakehouse_id for manual runs) ──
    "lakehouse_names": {
        "source":  "lh_ibp_source",
        "landing": "lh_ibp_landing",
        "bronze":  "lh_ibp_bronze",
        "silver":  "lh_ibp_silver",
        "gold":    "lh_ibp_gold",
    },

    # ── Test Data Generation ─────────────────────────────────────
    "n_skus":               50,
    "n_plants":             5,
    "n_customers":          20,
    "n_markets":            4,
    "n_production_lines":   10,
    "history_periods":      156,
    "seed":                 42,
    "demand_shock_prob":    0.06,
    "intermittent_pct":     0.10,
    "price_elasticity_range": [-0.8, -0.3],
    "promo_lift_range":     [0.15, 0.40],
}


# Derive prediction_tables from models_enabled
IBP_CONFIG["prediction_tables"] = [f"{m}_predictions" for m in IBP_CONFIG["models_enabled"]]


def prediction_table_for(model_type: str) -> str:
    """Return the silver prediction table name for a given model type."""
    return f"{model_type}_predictions"


def cfg(key: str, override=None):
    """Get a config value. Pipeline override takes priority over default."""
    if override is not None and override != "" and override != "None":
        return override
    return IBP_CONFIG.get(key)


def freq_params(key=None):
    """Return frequency-derived params, or a single key from the map.

    All seasonal periods, lag defaults, date-offset keywords, etc. are
    derived from IBP_CONFIG["frequency"].  Explicit overrides in
    IBP_CONFIG take precedence for the keys that exist in both places.
    """
    fp = FREQ_MAP[IBP_CONFIG["frequency"]]
    if key:
        return fp[key]
    return fp


def named(base: str) -> str:
    """Apply naming_prefix + base + naming_suffix.

    Example: named("lh_ibp_source") -> "lh_ibp_source_dev"
    """
    pfx = IBP_CONFIG.get("naming_prefix") or ""
    sfx = IBP_CONFIG.get("naming_suffix") or ""
    return f"{pfx}{base}{sfx}"
