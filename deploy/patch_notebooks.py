"""
Patch all main notebooks to add parameter cells and fix fallback pattern.

For each notebook:
1. Insert a # @parameters / # @end_parameters block (after header, before %run)
2. Replace params["key"] with params.get("key") or key
3. Replace x = params.get("key") with x = params.get("key") or x (for lakehouse IDs)

Run once to update source files in deploy/assets/notebooks/main/.
"""

import pathlib, re

MAIN_DIR = pathlib.Path(__file__).parent / "assets" / "notebooks" / "main"

SRC = '["orders","shipments","production_history","master_sku","master_plant","master_customer","master_market","budget_volumes","inventory_finished_goods","production_lines"]'
GRAIN = '["plant_id","sku_id"]'
EXT_GRAIN = '["plant_id","sku_group","customer_id","market_id"]'
FEAT = '["price_per_ton","lead_time_days","promo_flag","safety_stock_tons"]'
HIER = '["market_id","plant_id","sku_group","sku_id","customer_id"]'
SIG = '["construction_index","interest_rate","inflation_rate","tariff_rate"]'

NOTEBOOK_PARAMS: dict[str, list[tuple[str, str]]] = {
    "00_generate_test_data": [
        ("source_lakehouse_id", ""),
        ("n_skus", "50"),
        ("n_plants", "5"),
        ("n_customers", "20"),
        ("n_markets", "4"),
        ("n_production_lines", "10"),
        ("history_months", "42"),
        ("seed", "42"),
    ],
    "01_ingest_sources": [
        ("source_lakehouse_id", ""),
        ("landing_lakehouse_id", ""),
        ("source_tables", SRC),
    ],
    "02_transform_bronze": [
        ("landing_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("source_tables", SRC),
    ],
    "03_feature_engineering": [
        ("bronze_lakehouse_id", ""),
        ("silver_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("frequency", "M"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("feature_columns", FEAT),
        ("source_tables", SRC),
    ],
    "04_train_sarima": [
        ("silver_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("test_split_ratio", "0.2"),
        ("sarima_order", "[1,1,1]"),
        ("sarima_seasonal_order", "[1,1,1,12]"),
        ("experiment_name", "ibp_demand_forecast"),
        ("registered_model_prefix", "ibp_model"),
        ("min_series_length", "24"),
    ],
    "04_train_prophet": [
        ("silver_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("test_split_ratio", "0.2"),
        ("prophet_yearly_seasonality", "true"),
        ("prophet_weekly_seasonality", "false"),
        ("prophet_changepoint_prior", "0.05"),
        ("experiment_name", "ibp_demand_forecast"),
        ("registered_model_prefix", "ibp_model"),
        ("min_series_length", "24"),
    ],
    "04_train_var": [
        ("silver_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("feature_columns", FEAT),
        ("test_split_ratio", "0.2"),
        ("var_maxlags", "12"),
        ("var_ic", "aic"),
        ("experiment_name", "ibp_demand_forecast"),
        ("registered_model_prefix", "ibp_model"),
        ("min_series_length", "24"),
    ],
    "04_train_exp_smoothing": [
        ("silver_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("test_split_ratio", "0.2"),
        ("exp_smoothing_trend", "add"),
        ("exp_smoothing_seasonal", "add"),
        ("exp_smoothing_seasonal_periods", "12"),
        ("experiment_name", "ibp_demand_forecast"),
        ("registered_model_prefix", "ibp_model"),
        ("min_series_length", "24"),
    ],
    "05_score_forecast": [
        ("silver_lakehouse_id", ""),
        ("gold_lakehouse_id", ""),
        ("date_column", "period_date"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("feature_columns", FEAT),
        ("forecast_horizon", "6"),
        ("sarima_order", "[1,1,1]"),
        ("sarima_seasonal_order", "[1,1,1,12]"),
        ("exp_smoothing_trend", "add"),
        ("exp_smoothing_seasonal", "add"),
        ("exp_smoothing_seasonal_periods", "12"),
        ("prophet_yearly_seasonality", "true"),
        ("prophet_weekly_seasonality", "false"),
        ("prophet_changepoint_prior", "0.05"),
        ("var_maxlags", "12"),
        ("var_ic", "aic"),
    ],
    "06_version_snapshot": [
        ("silver_lakehouse_id", ""),
        ("gold_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("keep_n_snapshots", "24"),
    ],
    "07_demand_to_capacity": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("capacity_output_table", "capacity_translation"),
        ("production_history_table", "production_history"),
        ("grain_columns", GRAIN),
        ("rolling_months", "3"),
        ("tons_to_lf_factor", "2000"),
        ("width_column", "width_inches"),
        ("speed_column", "line_speed_fpm"),
        ("line_id_column", "line_id"),
    ],
    "08_sales_overrides": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("overrides_table", "sales_overrides"),
        ("grain_columns", GRAIN),
    ],
    "09_market_adjustments": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("adjustments_table", "market_adjustments"),
        ("default_scale_factor", "1.0"),
        ("grain_columns", GRAIN),
    ],
    "10_consensus_build": [
        ("gold_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("grain_columns", GRAIN),
    ],
    "11_accuracy_tracking": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("accuracy_table", "accuracy_tracking"),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("extended_grains", EXT_GRAIN),
        ("source_tables", SRC),
        ("date_column", "period_date"),
    ],
    "12_aggregate_gold": [
        ("gold_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("hierarchy_levels", HIER),
    ],
    "13_budget_comparison": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("budget_table", "budget_volumes"),
        ("comparison_output_table", "budget_comparison"),
        ("over_forecast_threshold", "0.10"),
        ("under_forecast_threshold", "-0.10"),
        ("hierarchy_levels", HIER),
    ],
    "P2_01_external_signals": [
        ("silver_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("gold_lakehouse_id", ""),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("signal_columns", SIG),
        ("signals_table", "external_signals"),
        ("external_signals_enabled", "true"),
    ],
    "P2_02_scenario_modeling": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("scenarios_table", "scenario_definitions"),
        ("grain_columns", GRAIN),
        ("scenarios_enabled", "true"),
    ],
    "P2_03_sku_classification": [
        ("silver_lakehouse_id", ""),
        ("gold_lakehouse_id", ""),
        ("target_column", "tons"),
        ("grain_columns", GRAIN),
        ("sku_classification_output_table", "sku_classifications"),
        ("runner_threshold", "0.8"),
        ("repeater_threshold", "0.95"),
        ("xyz_cv_threshold_x", "0.5"),
        ("xyz_cv_threshold_y", "1.0"),
        ("sku_classification_enabled", "true"),
    ],
    "P2_04_inventory_alignment": [
        ("gold_lakehouse_id", ""),
        ("bronze_lakehouse_id", ""),
        ("output_table", "forecast_versions"),
        ("grain_columns", GRAIN),
    ],
}


def quote(val: str) -> str:
    if not val:
        return '""'
    if val.startswith("[") or val.startswith("{"):
        return f"'{val}'"
    return f'"{val}"'


def build_param_block(params: list[tuple[str, str]]) -> str:
    lines = ["# @parameters"]
    for name, default in params:
        lines.append(f"{name} = {quote(default)}")
    lines.append("# @end_parameters")
    return "\n".join(lines)


def patch_file(path: pathlib.Path):
    stem = path.stem
    if stem not in NOTEBOOK_PARAMS:
        print(f"  SKIP {stem} (no param map)")
        return

    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()
    param_defs = NOTEBOOK_PARAMS[stem]
    param_names = {p[0] for p in param_defs}

    # Find insertion point: after header comments, before first %run or code
    insert_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") and not stripped.startswith("# %run"):
            insert_idx = i + 1
            continue
        if not stripped:
            insert_idx = i + 1
            continue
        break

    # Check if already patched
    if "# @parameters" in raw:
        print(f"  SKIP {stem} (already patched)")
        return

    # Build parameter block
    param_block = build_param_block(param_defs)

    # Insert parameter block
    new_lines = lines[:insert_idx] + [param_block, ""] + lines[insert_idx:]

    # Fix fallback pattern: params["key"] → params.get("key") or key
    result = []
    for line in new_lines:
        # Pattern 1: x = params["key"] → x = params.get("key") or x
        m = re.match(r'^(\s*)(\w+)\s*=\s*params\["(\w+)"\](.*)$', line)
        if m:
            indent, var, key, rest = m.groups()
            if not rest.strip():
                line = f'{indent}{var} = params.get("{key}") or {var}'

        # Pattern 2: x = params.get("key") alone (no 'or' fallback, no int/float wrap)
        # Only for lakehouse IDs and similar that do simple assignment
        m2 = re.match(r'^(\s*)(\w+)\s*=\s*params\.get\("(\w+)"\)\s*$', line)
        if m2:
            indent, var, key = m2.groups()
            line = f'{indent}{var} = params.get("{key}") or {var}'

        # Pattern 3: parse_list_param(params["key"]) → parse_list_param(params.get("key") or key)
        m3 = re.match(r'^(\s*)(\w+)\s*=\s*parse_list_param\(params\["(\w+)"\]\)(.*)$', line)
        if m3:
            indent, var, key, rest = m3.groups()
            if not rest.strip():
                line = f'{indent}{var} = parse_list_param(params.get("{key}") or {var})'

        # Pattern 4: parse_list_param(params["key"]) inside other expressions
        line = re.sub(
            r'parse_list_param\(params\["(\w+)"\]\)',
            lambda m: f'parse_list_param(params.get("{m.group(1)}") or {m.group(1)})',
            line
        )

        result.append(line)

    path.write_text("\n".join(result), encoding="utf-8")
    print(f"  PATCHED {stem}: {len(param_defs)} params, inserted block at line {insert_idx}")


if __name__ == "__main__":
    print("Patching notebooks with parameter cells...\n")
    for f in sorted(MAIN_DIR.glob("*.py")):
        patch_file(f)
    print("\nDone. Run convert_notebooks.py to rebuild Fabric format.")
