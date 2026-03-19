"""
Generate Fabric Data Pipeline JSON definitions for IBP Forecast.

Enterprise-grade: pipelines ONLY pass lakehouse IDs.
All other config lives in the centralized ibp_config notebook module.
Pipeline definitions use {{PLACEHOLDER}} tokens replaced at deploy time.
"""

import json, pathlib

OUT_DIR = pathlib.Path(__file__).parent / "assets" / "pipelines"
OUT_DIR.mkdir(parents=True, exist_ok=True)

WS = "{{WORKSPACE_ID}}"
SRC = "{{SOURCE_LH}}"
LND = "{{LANDING_LH}}"
BRZ = "{{BRONZE_LH}}"
SLV = "{{SILVER_LH}}"
GLD = "{{GOLD_LH}}"


def nb_id(name: str) -> str:
    return f"{{{{NB_{name}}}}}"


def nb_activity(name: str, notebook_name: str, params: dict, depends_on: list[str] | None = None):
    dep = []
    for d in (depends_on or []):
        dep.append({"activity": d, "dependencyConditions": ["Succeeded"]})

    nb_params = {}
    for k, v in params.items():
        nb_params[k] = {"value": v, "type": "string"}

    return {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": dep,
        "policy": {"timeout": "0.12:00:00", "retry": 1, "retryIntervalInSeconds": 60},
        "typeProperties": {
            "notebookId": nb_id(notebook_name),
            "workspaceId": WS,
            "parameters": nb_params,
        },
    }


def pipeline(name: str, activities: list, parameters: dict):
    return {
        "name": name,
        "properties": {
            "activities": activities,
            "parameters": {k: {"type": "string", "defaultValue": v} for k, v in parameters.items()},
            "annotations": [],
        },
    }


P = lambda k: f"@pipeline().parameters.{k}"

LH_PARAMS = {
    "source_lakehouse_id":  SRC,
    "landing_lakehouse_id": LND,
    "bronze_lakehouse_id":  BRZ,
    "silver_lakehouse_id":  SLV,
    "gold_lakehouse_id":    GLD,
}


# ── Test Data Pipeline ──────────────────────────────────────────

seed_acts = [
    nb_activity("00_generate_test_data", "00_generate_test_data", {
        "source_lakehouse_id": P("source_lakehouse_id"),
    }),
]

pl_seed = pipeline("pl_ibp_seed_test_data", seed_acts, {
    "source_lakehouse_id": SRC,
})


# ── Phase 1 Pipeline ────────────────────────────────────────────

phase1_acts = [
    nb_activity("01_ingest_sources", "01_ingest_sources", {
        "source_lakehouse_id":  P("source_lakehouse_id"),
        "landing_lakehouse_id": P("landing_lakehouse_id"),
    }),
    nb_activity("02_transform_bronze", "02_transform_bronze", {
        "landing_lakehouse_id": P("landing_lakehouse_id"),
        "bronze_lakehouse_id":  P("bronze_lakehouse_id"),
    }, depends_on=["01_ingest_sources"]),
    nb_activity("03_feature_engineering", "03_feature_engineering", {
        "bronze_lakehouse_id":  P("bronze_lakehouse_id"),
        "silver_lakehouse_id":  P("silver_lakehouse_id"),
    }, depends_on=["02_transform_bronze"]),
]

for model in ["sarima", "prophet", "var", "exp_smoothing"]:
    phase1_acts.append(nb_activity(
        f"04_train_{model}", f"04_train_{model}", {
            "silver_lakehouse_id": P("silver_lakehouse_id"),
        },
        depends_on=["03_feature_engineering"],
    ))

phase1_acts.append(nb_activity("05_score_forecast", "05_score_forecast", {
    "silver_lakehouse_id": P("silver_lakehouse_id"),
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
}, depends_on=["04_train_sarima", "04_train_prophet", "04_train_var", "04_train_exp_smoothing"]))

phase1_acts.append(nb_activity("06_version_snapshot", "06_version_snapshot", {
    "silver_lakehouse_id": P("silver_lakehouse_id"),
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
}, depends_on=["05_score_forecast"]))

phase1_acts.append(nb_activity("07_demand_to_capacity", "07_demand_to_capacity", {
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
    "bronze_lakehouse_id": P("bronze_lakehouse_id"),
}, depends_on=["06_version_snapshot"]))

phase1_acts.append(nb_activity("08_sales_overrides", "08_sales_overrides", {
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
    "bronze_lakehouse_id": P("bronze_lakehouse_id"),
}, depends_on=["06_version_snapshot"]))

phase1_acts.append(nb_activity("09_market_adjustments", "09_market_adjustments", {
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
    "bronze_lakehouse_id": P("bronze_lakehouse_id"),
}, depends_on=["08_sales_overrides"]))

phase1_acts.append(nb_activity("10_consensus_build", "10_consensus_build", {
    "gold_lakehouse_id": P("gold_lakehouse_id"),
}, depends_on=["07_demand_to_capacity", "09_market_adjustments"]))

phase1_acts.append(nb_activity("11_accuracy_tracking", "11_accuracy_tracking", {
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
    "bronze_lakehouse_id": P("bronze_lakehouse_id"),
}, depends_on=["10_consensus_build"]))

phase1_acts.append(nb_activity("12_aggregate_gold", "12_aggregate_gold", {
    "gold_lakehouse_id": P("gold_lakehouse_id"),
}, depends_on=["10_consensus_build"]))

phase1_acts.append(nb_activity("13_budget_comparison", "13_budget_comparison", {
    "gold_lakehouse_id":   P("gold_lakehouse_id"),
    "bronze_lakehouse_id": P("bronze_lakehouse_id"),
}, depends_on=["10_consensus_build"]))

pl_phase1 = pipeline("pl_ibp_phase1_core", phase1_acts, LH_PARAMS)


# ── Phase 2 Pipeline ────────────────────────────────────────────

phase2_acts = [
    nb_activity("P2_01_external_signals", "P2_01_external_signals", {
        "silver_lakehouse_id": P("silver_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
    }),
    nb_activity("P2_02_scenario_modeling", "P2_02_scenario_modeling", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }),
    nb_activity("P2_03_sku_classification", "P2_03_sku_classification", {
        "silver_lakehouse_id": P("silver_lakehouse_id"),
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
    }),
    nb_activity("P2_04_inventory_alignment", "P2_04_inventory_alignment", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }),
]

pl_phase2 = pipeline("pl_ibp_phase2_advanced", phase2_acts, LH_PARAMS)


# ── Write files ─────────────────────────────────────────────────

for pl in [pl_seed, pl_phase1, pl_phase2]:
    out_path = OUT_DIR / f"{pl['name']}.json"
    out_path.write_text(json.dumps(pl, indent=2), encoding="utf-8")
    act_count = len(pl["properties"]["activities"])
    param_count = len(pl["properties"]["parameters"])
    print(f"  {pl['name']}.json  ({act_count} activities, {param_count} pipeline params)")

print(f"\nGenerated 3 pipeline definitions in {OUT_DIR}")
