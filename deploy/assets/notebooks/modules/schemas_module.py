# Fabric Notebook -- Module
# schemas_module.py -- Single source of truth for ALL table schemas
#
# Every table schema in the system is defined here once. Notebooks import
# from this module to create empty tables, build the semantic model BIM,
# or validate DataFrame columns.
#
# Convention:
#   - All tables live in TABLES dict, keyed by lakehouse table name.
#   - Columns that appear in the Power BI BIM have a "summarize" field.
#   - Columns without "summarize" exist in Delta but not in the BIM.
#   - Semantic model tables additionally have: display_name, entity, measures.

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType,
    TimestampType,
)

_SPARK_TYPE_MAP = {
    "string":    StringType(),
    "double":    DoubleType(),
    "int64":     LongType(),
    "boolean":   BooleanType(),
    "timestamp": TimestampType(),
}


# ═══════════════════════════════════════════════════════════════════
# TABLE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════

TABLES = {

    # ── Source Tables ─────────────────────────────────────────────

    "orders": {
        "columns": [
            {"name": "period_date",       "type": "string"},
            {"name": "plant_id",          "type": "string"},
            {"name": "sku_id",            "type": "string"},
            {"name": "sku_group",         "type": "string"},
            {"name": "customer_id",       "type": "string"},
            {"name": "market_id",         "type": "string"},
            {"name": "tons",              "type": "double"},
            {"name": "price_per_ton",     "type": "double"},
            {"name": "lead_time_days",    "type": "int64"},
            {"name": "promo_flag",        "type": "int64"},
            {"name": "safety_stock_tons", "type": "double"},
        ],
    },

    "shipments": {
        "columns": [
            {"name": "period_date",       "type": "string"},
            {"name": "plant_id",          "type": "string"},
            {"name": "sku_id",            "type": "string"},
            {"name": "sku_group",         "type": "string"},
            {"name": "customer_id",       "type": "string"},
            {"name": "market_id",         "type": "string"},
            {"name": "tons",              "type": "double"},
            {"name": "price_per_ton",     "type": "double"},
            {"name": "lead_time_days",    "type": "int64"},
            {"name": "promo_flag",        "type": "int64"},
            {"name": "safety_stock_tons", "type": "double"},
            {"name": "shipped_date",      "type": "string"},
        ],
    },

    "production_history": {
        "columns": [
            {"name": "period_date",    "type": "string"},
            {"name": "plant_id",       "type": "string"},
            {"name": "sku_id",         "type": "string"},
            {"name": "line_id",        "type": "string"},
            {"name": "width_inches",   "type": "double"},
            {"name": "line_speed_fpm", "type": "double"},
            {"name": "produced_tons",  "type": "double"},
        ],
    },

    "budget_volumes": {
        "columns": [
            {"name": "period",      "type": "string"},
            {"name": "plant_id",    "type": "string"},
            {"name": "sku_id",      "type": "string"},
            {"name": "sku_group",   "type": "string"},
            {"name": "market_id",   "type": "string"},
            {"name": "budget_tons", "type": "double"},
        ],
    },

    "inventory_finished_goods": {
        "columns": [
            {"name": "sku_id",        "type": "string"},
            {"name": "plant_id",      "type": "string"},
            {"name": "sku_group",     "type": "string"},
            {"name": "on_hand_tons",  "type": "double"},
            {"name": "snapshot_date", "type": "string"},
        ],
    },

    "sales_overrides": {
        "columns": [
            {"name": "period",              "type": "string"},
            {"name": "plant_id",            "type": "string"},
            {"name": "sku_id",              "type": "string"},
            {"name": "override_delta_tons", "type": "double"},
            {"name": "override_reason",     "type": "string"},
            {"name": "created_by",          "type": "string"},
        ],
    },

    "market_adjustments": {
        "columns": [
            {"name": "market_id",         "type": "string"},
            {"name": "period",            "type": "string"},
            {"name": "scale_factor",      "type": "double"},
            {"name": "adjustment_reason", "type": "string"},
        ],
    },

    "external_signals": {
        "columns": [
            {"name": "period",             "type": "string"},
            {"name": "construction_index", "type": "double"},
            {"name": "interest_rate",      "type": "double"},
            {"name": "inflation_rate",     "type": "double"},
            {"name": "tariff_rate",        "type": "double"},
        ],
    },

    "scenario_definitions": {
        "columns": [
            {"name": "scenario_name", "type": "string"},
            {"name": "filter_type",   "type": "string"},
            {"name": "filter_value",  "type": "string"},
            {"name": "include",       "type": "boolean"},
        ],
    },

    "master_customer": {
        "columns": [
            {"name": "customer_id",   "type": "string"},
            {"name": "customer_name", "type": "string"},
            {"name": "market_id",     "type": "string"},
            {"name": "region",        "type": "string"},
            {"name": "tier",          "type": "string"},
        ],
    },

    "master_market": {
        "columns": [
            {"name": "market_id",      "type": "string"},
            {"name": "market_name",    "type": "string"},
            {"name": "market_segment", "type": "string"},
        ],
    },

    "production_lines": {
        "columns": [
            {"name": "line_id",          "type": "string"},
            {"name": "plant_id",         "type": "string"},
            {"name": "line_name",        "type": "string"},
            {"name": "max_speed_fpm",    "type": "double"},
            {"name": "max_width_inches", "type": "double"},
        ],
    },

    # ── Silver Tables ─────────────────────────────────────────────

    "raw_forecasts": {
        "columns": [
            {"name": "period",        "type": "string"},
            {"name": "forecast_tons", "type": "double"},
            {"name": "model_type",    "type": "string"},
            {"name": "plant_id",      "type": "string"},
            {"name": "sku_id",        "type": "string"},
        ],
    },

    "predictions": {
        "columns": [
            {"name": "period",     "type": "string"},
            {"name": "actual",     "type": "double"},
            {"name": "predicted",  "type": "double"},
            {"name": "model_type", "type": "string"},
            {"name": "plant_id",   "type": "string"},
            {"name": "sku_id",     "type": "string"},
        ],
    },

    # ── Gold Tables (Semantic Model) ──────────────────────────────
    # Columns with "summarize" are exposed in the Power BI BIM.
    # Columns without "summarize" exist in the Delta table only.

    "forecast_versions": {
        "display_name": "Forecast Versions",
        "entity": "forecast_versions",
        "columns": [
            {"name": "plant_id",            "type": "string",  "summarize": "none"},
            {"name": "sku_id",              "type": "string",  "summarize": "none"},
            {"name": "period",              "type": "string",  "summarize": "none"},
            {"name": "forecast_tons",       "type": "double",  "summarize": "sum"},
            {"name": "model_type",          "type": "string",  "summarize": "none"},
            {"name": "version_type",        "type": "string",  "summarize": "none"},
            {"name": "version_id",          "type": "string",  "summarize": "none"},
            {"name": "snapshot_month",      "type": "string",  "summarize": "none"},
            {"name": "created_by",          "type": "string"},
            {"name": "created_at",          "type": "string"},
            {"name": "parent_version_id",   "type": "string"},
            {"name": "override_delta_tons", "type": "double"},
            {"name": "market_scale_factor", "type": "double"},
            {"name": "final_forecast_tons", "type": "double"},
            {"name": "market_id",           "type": "string"},
        ],
        "measures": [
            {"name": "Total Forecast Tons", "expression": "SUM('Forecast Versions'[forecast_tons])"},
            {"name": "Avg Forecast Tons",   "expression": "AVERAGE('Forecast Versions'[forecast_tons])", "formatString": "0.00"},
        ],
    },

    "reporting_actuals_vs_forecast": {
        "display_name": "Reporting Actuals vs Forecast",
        "entity": "reporting_actuals_vs_forecast",
        "columns": [
            {"name": "plant_id",      "type": "string",  "summarize": "none"},
            {"name": "sku_id",        "type": "string",  "summarize": "none"},
            {"name": "period",        "type": "string",  "summarize": "none"},
            {"name": "forecast_tons", "type": "double",  "summarize": "sum"},
            {"name": "actual_tons",   "type": "double",  "summarize": "sum"},
            {"name": "abs_error",     "type": "double",  "summarize": "sum"},
            {"name": "pct_error",     "type": "double",  "summarize": "none"},
            {"name": "variance",      "type": "double",  "summarize": "sum"},
            {"name": "model_type",    "type": "string",  "summarize": "none"},
            {"name": "version_type",  "type": "string",  "summarize": "none"},
            {"name": "version_id",    "type": "string",  "summarize": "none"},
            {"name": "is_future",     "type": "boolean", "summarize": "none"},
            {"name": "snapshot_date", "type": "string",  "summarize": "none"},
        ],
        "measures": [
            {"name": "Total Actual Tons",    "expression": "SUM('Reporting Actuals vs Forecast'[actual_tons])"},
            {"name": "Total Variance",       "expression": "SUM('Reporting Actuals vs Forecast'[variance])"},
            {"name": "MAPE %",              "expression": "DIVIDE(SUM('Reporting Actuals vs Forecast'[abs_error]),SUM('Reporting Actuals vs Forecast'[actual_tons]),BLANK())*100", "formatString": "0.0"},
            {"name": "Bias %",              "expression": "DIVIDE([Total Variance],[Total Actual Tons],BLANK())*100", "formatString": "0.0"},
            {"name": "Forecast Accuracy %", "expression": "100-[MAPE %]", "formatString": "0.0"},
            {"name": "Future Forecast Tons", "expression": "CALCULATE(SUM('Reporting Actuals vs Forecast'[forecast_tons]),'Reporting Actuals vs Forecast'[is_future]=TRUE())"},
        ],
    },

    "master_sku": {
        "display_name": "Master SKU",
        "entity": "master_sku",
        "columns": [
            {"name": "sku_id",            "type": "string", "summarize": "none", "isKey": True},
            {"name": "sku_name",          "type": "string", "summarize": "none"},
            {"name": "sku_group",         "type": "string", "summarize": "none"},
            {"name": "base_width_inches", "type": "double"},
            {"name": "base_weight_lbs",   "type": "double"},
            {"name": "unit_of_measure",   "type": "string"},
        ],
        "measures": [],
    },

    "master_plant": {
        "display_name": "Master Plant",
        "entity": "master_plant",
        "columns": [
            {"name": "plant_id",            "type": "string", "summarize": "none", "isKey": True},
            {"name": "plant_name",          "type": "string", "summarize": "none"},
            {"name": "region",              "type": "string", "summarize": "none"},
            {"name": "capacity_tons_month", "type": "double"},
        ],
        "measures": [],
    },

    "capacity_translation": {
        "display_name": "Capacity Translation",
        "entity": "capacity_translation",
        "columns": [
            {"name": "plant_id",         "type": "string", "summarize": "none"},
            {"name": "sku_id",           "type": "string", "summarize": "none"},
            {"name": "period",           "type": "string", "summarize": "none"},
            {"name": "forecast_tons",    "type": "double", "summarize": "sum"},
            {"name": "lineal_feet",      "type": "double", "summarize": "sum"},
            {"name": "production_hours", "type": "double", "summarize": "sum"},
            {"name": "line_id",          "type": "string"},
            {"name": "avg_width",        "type": "double"},
            {"name": "avg_speed",        "type": "double"},
            {"name": "min_width",        "type": "double"},
            {"name": "max_width",        "type": "double"},
            {"name": "min_speed",        "type": "double"},
            {"name": "max_speed",        "type": "double"},
        ],
        "measures": [
            {"name": "Total Lineal Feet",      "expression": "SUM('Capacity Translation'[lineal_feet])",      "formatString": "#,0"},
            {"name": "Total Production Hours", "expression": "SUM('Capacity Translation'[production_hours])", "formatString": "#,0.0"},
        ],
    },

    "backtest_predictions": {
        "display_name": "Backtest Predictions",
        "entity": "backtest_predictions",
        "columns": [
            {"name": "plant_id",   "type": "string", "summarize": "none"},
            {"name": "sku_id",     "type": "string", "summarize": "none"},
            {"name": "period",     "type": "string", "summarize": "none"},
            {"name": "actual",     "type": "double", "summarize": "sum"},
            {"name": "predicted",  "type": "double", "summarize": "sum"},
            {"name": "model_type", "type": "string", "summarize": "none"},
            {"name": "error",      "type": "double", "summarize": "sum"},
            {"name": "abs_error",  "type": "double", "summarize": "sum"},
            {"name": "pct_error",  "type": "double", "summarize": "none"},
        ],
        "measures": [
            {"name": "Backtest MAPE %", "expression": "DIVIDE(SUM('Backtest Predictions'[abs_error]),SUM('Backtest Predictions'[actual]),BLANK())*100", "formatString": "0.0"},
            {"name": "Total Actual",    "expression": "SUM('Backtest Predictions'[actual])",    "formatString": "#,0.0"},
            {"name": "Total Predicted", "expression": "SUM('Backtest Predictions'[predicted])", "formatString": "#,0.0"},
        ],
    },

    "forecast_waterfall": {
        "display_name": "Forecast Waterfall",
        "entity": "forecast_waterfall",
        "columns": [
            {"name": "plant_id",            "type": "string", "summarize": "none"},
            {"name": "sku_id",              "type": "string", "summarize": "none"},
            {"name": "period",              "type": "string", "summarize": "none"},
            {"name": "model_type",          "type": "string", "summarize": "none"},
            {"name": "snapshot_month",      "type": "string", "summarize": "none"},
            {"name": "baseline_tons",       "type": "double", "summarize": "sum"},
            {"name": "override_delta_tons", "type": "double", "summarize": "sum"},
            {"name": "market_scale_factor", "type": "double", "summarize": "none"},
            {"name": "consensus_tons",      "type": "double", "summarize": "sum"},
            {"name": "actual_tons",         "type": "double", "summarize": "sum"},
        ],
        "measures": [
            {"name": "Baseline Total",      "expression": "SUM('Forecast Waterfall'[baseline_tons])",       "formatString": "#,0.0"},
            {"name": "Sales Override Total", "expression": "SUM('Forecast Waterfall'[override_delta_tons])", "formatString": "#,0.0"},
            {"name": "Consensus Total",     "expression": "SUM('Forecast Waterfall'[consensus_tons])",      "formatString": "#,0.0"},
            {"name": "Waterfall Actual",    "expression": "SUM('Forecast Waterfall'[actual_tons])",         "formatString": "#,0.0"},
            {"name": "Net Adjustment %",    "expression": "DIVIDE(SUM('Forecast Waterfall'[consensus_tons])-SUM('Forecast Waterfall'[baseline_tons]),SUM('Forecast Waterfall'[baseline_tons]),BLANK())*100", "formatString": "0.0"},
        ],
    },

    "accuracy_tracking": {
        "display_name": "Accuracy Tracking",
        "entity": "accuracy_tracking",
        "columns": [
            {"name": "plant_id",       "type": "string", "summarize": "none"},
            {"name": "sku_id",         "type": "string", "summarize": "none"},
            {"name": "version_id",     "type": "string", "summarize": "none"},
            {"name": "snapshot_month", "type": "string", "summarize": "none"},
            {"name": "model_type",     "type": "string", "summarize": "none"},
            {"name": "version_type",   "type": "string", "summarize": "none"},
            {"name": "n_periods",      "type": "int64",  "summarize": "sum"},
            {"name": "mape",           "type": "double", "summarize": "none"},
            {"name": "bias",           "type": "double", "summarize": "none"},
            {"name": "rmse",           "type": "double", "summarize": "none"},
            {"name": "mae",            "type": "double", "summarize": "none"},
            {"name": "r2",             "type": "double", "summarize": "none"},
            {"name": "evaluated_at",   "type": "string", "summarize": "none"},
        ],
        "measures": [
            {"name": "Avg MAPE %",  "expression": "AVERAGE('Accuracy Tracking'[mape])",  "formatString": "0.0"},
            {"name": "Avg Bias",    "expression": "AVERAGE('Accuracy Tracking'[bias])",  "formatString": "0.00"},
            {"name": "Avg RMSE",    "expression": "AVERAGE('Accuracy Tracking'[rmse])",  "formatString": "0.00"},
            {"name": "Avg MAE",     "expression": "AVERAGE('Accuracy Tracking'[mae])",   "formatString": "0.00"},
            {"name": "Avg R\u00b2", "expression": "AVERAGE('Accuracy Tracking'[r2])",    "formatString": "0.000"},
            {"name": "Evaluations", "expression": "COUNTROWS('Accuracy Tracking')"},
        ],
    },

    # ── Gold Tables (Non-Semantic) ────────────────────────────────

    "model_recommendations": {
        "columns": [
            {"name": "plant_id",   "type": "string"},
            {"name": "sku_id",     "type": "string"},
            {"name": "model_type", "type": "string"},
            {"name": "best_mape",  "type": "double"},
        ],
    },

    "accuracy_by_plant_id": {
        "columns": [
            {"name": "plant_id",   "type": "string"},
            {"name": "model_type", "type": "string"},
            {"name": "avg_mape",   "type": "double"},
            {"name": "avg_bias",   "type": "double"},
            {"name": "avg_rmse",   "type": "double"},
            {"name": "n_grains",   "type": "int64"},
        ],
    },

    "accuracy_by_market_id": {
        "columns": [
            {"name": "market_id",  "type": "string"},
            {"name": "model_type", "type": "string"},
            {"name": "avg_mape",   "type": "double"},
            {"name": "avg_bias",   "type": "double"},
            {"name": "avg_rmse",   "type": "double"},
            {"name": "n_grains",   "type": "int64"},
        ],
    },

    # ── Phase 2 Tables ────────────────────────────────────────────

    "signal_importance": {
        "columns": [
            {"name": "signal",          "type": "string"},
            {"name": "correlation",     "type": "double"},
            {"name": "n_obs",           "type": "int64"},
            {"name": "plant_id",        "type": "string"},
            {"name": "sku_id",          "type": "string"},
            {"name": "abs_correlation", "type": "double"},
        ],
    },

    "sku_classifications": {
        "columns": [
            {"name": "sku_id",           "type": "string"},
            {"name": "total_volume",     "type": "double"},
            {"name": "cumulative_pct",   "type": "double"},
            {"name": "abc_class",        "type": "string"},
            {"name": "rrs_class",        "type": "string"},
            {"name": "demand_mean",      "type": "double"},
            {"name": "demand_std",       "type": "double"},
            {"name": "cv",               "type": "double"},
            {"name": "xyz_class",        "type": "string"},
            {"name": "combined_class",   "type": "string"},
            {"name": "n_active_periods", "type": "int64"},
            {"name": "frequency_pct",    "type": "double"},
        ],
    },

    "inventory_alignment": {
        "columns": [
            {"name": "plant_id",          "type": "string"},
            {"name": "sku_id",            "type": "string"},
            {"name": "current_inventory", "type": "double"},
            {"name": "demand_3m",         "type": "double"},
            {"name": "coverage_months",   "type": "double"},
            {"name": "risk_flag",         "type": "string"},
            {"name": "excess_tons",       "type": "double"},
            {"name": "shortfall_tons",    "type": "double"},
        ],
    },
}

# ═══════════════════════════════════════════════════════════════════
# DERIVED VIEWS
# ═══════════════════════════════════════════════════════════════════

SEMANTIC_TABLES = {k: v for k, v in TABLES.items() if "display_name" in v}

# ═══════════════════════════════════════════════════════════════════
# RELATIONSHIPS
# ═══════════════════════════════════════════════════════════════════

SEMANTIC_RELATIONSHIPS = [
    {"name": "FK_FV_SKU",          "fromTable": "Forecast Versions",             "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_FV_Plant",        "fromTable": "Forecast Versions",             "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
    {"name": "FK_Reporting_SKU",   "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_Reporting_Plant", "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
    {"name": "FK_Capacity_SKU",    "fromTable": "Capacity Translation",          "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_Capacity_Plant",  "fromTable": "Capacity Translation",          "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
    {"name": "FK_Backtest_SKU",    "fromTable": "Backtest Predictions",          "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_Backtest_Plant",  "fromTable": "Backtest Predictions",          "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
    {"name": "FK_Waterfall_SKU",   "fromTable": "Forecast Waterfall",            "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_Waterfall_Plant", "fromTable": "Forecast Waterfall",            "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
    {"name": "FK_Accuracy_SKU",    "fromTable": "Accuracy Tracking",             "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
    {"name": "FK_Accuracy_Plant",  "fromTable": "Accuracy Tracking",             "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
]

ENSURE_GOLD_TABLES = ["accuracy_tracking", "forecast_waterfall"]


# ═══════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def spark_schema(table_name: str) -> StructType:
    """Return a PySpark StructType for the given table (all columns)."""
    tbl = TABLES.get(table_name)
    if not tbl:
        raise KeyError(f"Unknown table '{table_name}'. Available: {sorted(TABLES)}")
    fields = []
    for col in tbl["columns"]:
        spark_type = _SPARK_TYPE_MAP.get(col["type"], StringType())
        fields.append(StructField(col["name"], spark_type, nullable=True))
    return StructType(fields)


def column_names(table_name: str) -> list:
    """Return ordered column names for a table."""
    tbl = TABLES.get(table_name)
    if not tbl:
        raise KeyError(f"Unknown table '{table_name}'")
    return [c["name"] for c in tbl["columns"]]


def bim_column_names(table_name: str) -> list:
    """Return only the column names exposed in the semantic model BIM."""
    tbl = TABLES.get(table_name)
    if not tbl:
        raise KeyError(f"Unknown table '{table_name}'")
    return [c["name"] for c in tbl["columns"] if "summarize" in c]


def _bim_column(col: dict) -> dict:
    """Convert a schema column dict to BIM column format."""
    bim_col = {
        "name": col["name"],
        "dataType": col["type"],
        "sourceColumn": col["name"],
        "summarizeBy": col["summarize"],
    }
    if col.get("isKey"):
        bim_col["isKey"] = True
    return bim_col


def _bim_table(table_name: str) -> dict:
    """Build a single BIM table definition."""
    tbl = TABLES[table_name]
    bim_cols = [c for c in tbl["columns"] if "summarize" in c]
    result = {
        "name": tbl["display_name"],
        "columns": [_bim_column(c) for c in bim_cols],
        "partitions": [{
            "name": tbl["entity"],
            "mode": "directLake",
            "source": {
                "type": "entity",
                "entityName": tbl["entity"],
                "schemaName": "dbo",
                "expressionSource": "DatabaseQuery",
            },
        }],
    }
    if tbl.get("measures"):
        result["measures"] = tbl["measures"]
    return result


def build_bim(sql_endpoint: str, lakehouse_name: str) -> dict:
    """Build the complete BIM JSON for the semantic model.

    Args:
        sql_endpoint: Gold lakehouse SQL endpoint connection string.
        lakehouse_name: Gold lakehouse display name.

    Returns:
        Complete BIM dict ready for base64 encoding and API submission.
    """
    sm_tables = [t for t in TABLES if "display_name" in TABLES[t]]
    return {
        "compatibilityLevel": 1604,
        "model": {
            "culture": "en-US",
            "defaultMode": "directLake",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "discourageImplicitMeasures": True,
            "tables": [_bim_table(t) for t in sm_tables],
            "relationships": SEMANTIC_RELATIONSHIPS,
            "expressions": [{
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": (
                    f'let\n    database = Sql.Database("{sql_endpoint}", '
                    f'"{lakehouse_name}")\nin\n    database'
                ),
            }],
            "annotations": [{"name": "PBI_QueryRelationships", "value": "[]"}],
        },
    }
