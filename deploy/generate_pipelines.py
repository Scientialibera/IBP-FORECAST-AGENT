"""
Generate Fabric Data Pipeline JSON definitions for IBP Forecast.

Pipeline structure:
  1. pl_ibp_seed_test_data   -- generate synthetic data
  2. pl_ibp_train            -- ingest → bronze → features → train (5 models parallel)
  3. pl_ibp_score            -- score → version (CDC) → gold enrichment → reporting
  4. pl_ibp_refresh_model    -- refresh DirectLake semantic model
  5. pl_ibp_score_and_refresh -- orchestrator: execute score then refresh
  6. pl_ibp_phase2_advanced  -- external signals, scenarios, SKU class, inventory

Notebooks auto-discover lakehouse IDs at runtime via resolve_lakehouse_id().
Pipelines only provide notebook IDs and dependency ordering.
Pipeline definitions use {{PLACEHOLDER}} tokens replaced at deploy time.
"""

import json, pathlib, tomllib

OUT_DIR = pathlib.Path(__file__).parent / "assets" / "pipelines"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = pathlib.Path(__file__).parent / "deploy.config.toml"
with open(CONFIG_PATH, "rb") as _f:
    _deploy_cfg = tomllib.load(_f)

MODELS_ENABLED = _deploy_cfg.get("training", {}).get(
    "models_enabled", ["sarima", "prophet", "var", "exp_smoothing"]
)

WS = "{{WORKSPACE_ID}}"


def nb_id(name: str) -> str:
    return f"{{{{NB_{name}}}}}"


def nb_activity(name: str, notebook_name: str, depends_on: list[str] | None = None):
    dep = []
    for d in (depends_on or []):
        dep.append({"activity": d, "dependencyConditions": ["Succeeded"]})

    return {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": dep,
        "policy": {"timeout": "0.12:00:00", "retry": 1, "retryIntervalInSeconds": 60},
        "typeProperties": {
            "notebookId": nb_id(notebook_name),
            "workspaceId": WS,
        },
    }


def pl_id(name: str) -> str:
    """Placeholder for a child pipeline item ID, replaced at deploy time."""
    return f"{{{{PL_{name}}}}}"


def exec_pipeline_activity(name: str, pipeline_name: str,
                           depends_on: list[str] | None = None):
    dep = []
    for d in (depends_on or []):
        dep.append({"activity": d, "dependencyConditions": ["Succeeded"]})

    return {
        "name": name,
        "type": "ExecutePipeline",
        "dependsOn": dep,
        "typeProperties": {
            "pipeline": {
                "referenceName": pl_id(pipeline_name),
                "type": "PipelineReference",
            },
            "waitOnCompletion": True,
        },
    }


def pipeline(name: str, activities: list):
    return {
        "name": name,
        "properties": {
            "activities": activities,
            "annotations": [],
        },
    }


# ── 1. Test Data Pipeline ──────────────────────────────────────

seed_acts = [
    nb_activity("00_generate_test_data", "00_generate_test_data"),
]

pl_seed = pipeline("pl_ibp_seed_test_data", seed_acts)


# ── 2. Training Pipeline (ingest → train) ──────────────────────

train_acts = [
    nb_activity("01_ingest_sources", "01_ingest_sources"),
    nb_activity("02_transform_bronze", "02_transform_bronze",
                depends_on=["01_ingest_sources"]),
    nb_activity("03_feature_engineering", "03_feature_engineering",
                depends_on=["02_transform_bronze"]),
]

for model in MODELS_ENABLED:
    train_acts.append(nb_activity(
        f"04_train_{model}", f"04_train_{model}",
        depends_on=["03_feature_engineering"],
    ))

pl_train = pipeline("pl_ibp_train", train_acts)


# ── 3. Scoring Pipeline (score → gold → reporting) ─────────────

score_acts = [
    nb_activity("05_score_forecast", "05_score_forecast"),
    nb_activity("06_version_snapshot", "06_version_snapshot",
                depends_on=["05_score_forecast"]),
    nb_activity("07_demand_to_capacity", "07_demand_to_capacity",
                depends_on=["06_version_snapshot"]),
    nb_activity("08_sales_overrides", "08_sales_overrides",
                depends_on=["06_version_snapshot"]),
    nb_activity("09_market_adjustments", "09_market_adjustments",
                depends_on=["08_sales_overrides"]),
    nb_activity("10_consensus_build", "10_consensus_build",
                depends_on=["07_demand_to_capacity", "09_market_adjustments"]),
    nb_activity("11_accuracy_tracking", "11_accuracy_tracking",
                depends_on=["10_consensus_build"]),
    nb_activity("12_aggregate_gold", "12_aggregate_gold",
                depends_on=["10_consensus_build"]),
    nb_activity("13_budget_comparison", "13_budget_comparison",
                depends_on=["10_consensus_build"]),
    nb_activity("14_build_reporting_view", "14_build_reporting_view",
                depends_on=["11_accuracy_tracking", "12_aggregate_gold", "13_budget_comparison"]),
]

pl_score = pipeline("pl_ibp_score", score_acts)


# ── 4. Refresh Semantic Model + Create Report Pipeline ──────────

refresh_acts = [
    nb_activity("15_refresh_semantic_model", "15_refresh_semantic_model"),
    nb_activity("16_create_report", "16_create_report",
                depends_on=["15_refresh_semantic_model"]),
]

pl_refresh = pipeline("pl_ibp_refresh_model", refresh_acts)


# ── 5. Orchestrator: Score + Refresh ───────────────────────────

orch_acts = [
    exec_pipeline_activity("Execute_Score", "pl_ibp_score"),
    exec_pipeline_activity("Execute_Refresh", "pl_ibp_refresh_model",
                           depends_on=["Execute_Score"]),
]

pl_orch = pipeline("pl_ibp_score_and_refresh", orch_acts)


# ── 6. Phase 2 Pipeline ────────────────────────────────────────

phase2_acts = [
    nb_activity("P2_01_external_signals", "P2_01_external_signals"),
    nb_activity("P2_02_scenario_modeling", "P2_02_scenario_modeling"),
    nb_activity("P2_03_sku_classification", "P2_03_sku_classification"),
    nb_activity("P2_04_inventory_alignment", "P2_04_inventory_alignment"),
]

pl_phase2 = pipeline("pl_ibp_phase2_advanced", phase2_acts)


# ── Write files ─────────────────────────────────────────────────

all_pipelines = [pl_seed, pl_train, pl_score, pl_refresh, pl_orch, pl_phase2]

for pl in all_pipelines:
    out_path = OUT_DIR / f"{pl['name']}.json"
    out_path.write_text(json.dumps(pl, indent=2), encoding="utf-8")
    act_count = len(pl["properties"]["activities"])
    print(f"  {pl['name']}.json  ({act_count} activities)")

print(f"\nGenerated {len(all_pipelines)} pipeline definitions in {OUT_DIR}")
