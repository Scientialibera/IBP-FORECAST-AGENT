"""
Generate Fabric Data Pipeline JSON definitions for IBP Forecast.

Pipeline structure:
  1. pl_ibp_seed_test_data   -- generate synthetic data
  2. pl_ibp_train            -- ingest → bronze → features → train (4 models parallel)
  3. pl_ibp_score            -- score → version (CDC) → gold enrichment → reporting
  4. pl_ibp_refresh_model    -- refresh DirectLake semantic model
  5. pl_ibp_score_and_refresh -- orchestrator: execute score then refresh
  6. pl_ibp_phase2_advanced  -- external signals, scenarios, SKU class, inventory

Enterprise-grade: pipelines ONLY pass lakehouse IDs.
All other config lives in the centralized ibp_config notebook module.
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


def pl_id(name: str) -> str:
    """Placeholder for a child pipeline item ID, replaced at deploy time."""
    return f"{{{{PL_{name}}}}}"


def exec_pipeline_activity(name: str, pipeline_name: str, params: dict,
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
            "parameters": {k: {"value": v, "type": "Expression"} for k, v in params.items()},
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

LH_ALL = {
    "source_lakehouse_id":  SRC,
    "landing_lakehouse_id": LND,
    "bronze_lakehouse_id":  BRZ,
    "silver_lakehouse_id":  SLV,
    "gold_lakehouse_id":    GLD,
}

LH_SCORE = {
    "bronze_lakehouse_id":  BRZ,
    "silver_lakehouse_id":  SLV,
    "gold_lakehouse_id":    GLD,
}

LH_GOLD_ONLY = {
    "gold_lakehouse_id": GLD,
}


# ── 1. Test Data Pipeline ──────────────────────────────────────

seed_acts = [
    nb_activity("00_generate_test_data", "00_generate_test_data", {
        "source_lakehouse_id": P("source_lakehouse_id"),
    }),
]

pl_seed = pipeline("pl_ibp_seed_test_data", seed_acts, {
    "source_lakehouse_id": SRC,
})


# ── 2. Training Pipeline (ingest → train) ──────────────────────

train_acts = [
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

for model in MODELS_ENABLED:
    train_acts.append(nb_activity(
        f"04_train_{model}", f"04_train_{model}", {
            "silver_lakehouse_id": P("silver_lakehouse_id"),
        },
        depends_on=["03_feature_engineering"],
    ))

pl_train = pipeline("pl_ibp_train", train_acts, LH_ALL)


# ── 3. Scoring Pipeline (score → gold → reporting) ─────────────

score_acts = [
    nb_activity("05_score_forecast", "05_score_forecast", {
        "silver_lakehouse_id": P("silver_lakehouse_id"),
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
    }),
    nb_activity("06_version_snapshot", "06_version_snapshot", {
        "silver_lakehouse_id": P("silver_lakehouse_id"),
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
    }, depends_on=["05_score_forecast"]),
    nb_activity("07_demand_to_capacity", "07_demand_to_capacity", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }, depends_on=["06_version_snapshot"]),
    nb_activity("08_sales_overrides", "08_sales_overrides", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }, depends_on=["06_version_snapshot"]),
    nb_activity("09_market_adjustments", "09_market_adjustments", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }, depends_on=["08_sales_overrides"]),
    nb_activity("10_consensus_build", "10_consensus_build", {
        "gold_lakehouse_id": P("gold_lakehouse_id"),
    }, depends_on=["07_demand_to_capacity", "09_market_adjustments"]),
    nb_activity("11_accuracy_tracking", "11_accuracy_tracking", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }, depends_on=["10_consensus_build"]),
    nb_activity("12_aggregate_gold", "12_aggregate_gold", {
        "gold_lakehouse_id": P("gold_lakehouse_id"),
    }, depends_on=["10_consensus_build"]),
    nb_activity("13_budget_comparison", "13_budget_comparison", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
    }, depends_on=["10_consensus_build"]),
    nb_activity("14_build_reporting_view", "14_build_reporting_view", {
        "gold_lakehouse_id":   P("gold_lakehouse_id"),
        "bronze_lakehouse_id": P("bronze_lakehouse_id"),
        "silver_lakehouse_id": P("silver_lakehouse_id"),
    }, depends_on=["11_accuracy_tracking", "12_aggregate_gold", "13_budget_comparison"]),
]

pl_score = pipeline("pl_ibp_score", score_acts, LH_SCORE)


# ── 4. Refresh Semantic Model + Create Report Pipeline ──────────

refresh_acts = [
    nb_activity("15_refresh_semantic_model", "15_refresh_semantic_model", {
        "gold_lakehouse_id": P("gold_lakehouse_id"),
    }),
    nb_activity("16_create_report", "16_create_report", {
        "gold_lakehouse_id": P("gold_lakehouse_id"),
    }, depends_on=["15_refresh_semantic_model"]),
]

pl_refresh = pipeline("pl_ibp_refresh_model", refresh_acts, LH_GOLD_ONLY)


# ── 5. Orchestrator: Score + Refresh ───────────────────────────

orch_acts = [
    exec_pipeline_activity("Execute_Score", "pl_ibp_score", {
        "bronze_lakehouse_id":  P("bronze_lakehouse_id"),
        "silver_lakehouse_id":  P("silver_lakehouse_id"),
        "gold_lakehouse_id":    P("gold_lakehouse_id"),
    }),
    exec_pipeline_activity("Execute_Refresh", "pl_ibp_refresh_model", {
        "gold_lakehouse_id": P("gold_lakehouse_id"),
    }, depends_on=["Execute_Score"]),
]

pl_orch = pipeline("pl_ibp_score_and_refresh", orch_acts, LH_SCORE)


# ── 6. Phase 2 Pipeline ────────────────────────────────────────

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

pl_phase2 = pipeline("pl_ibp_phase2_advanced", phase2_acts, LH_ALL)


# ── Write files ─────────────────────────────────────────────────

all_pipelines = [pl_seed, pl_train, pl_score, pl_refresh, pl_orch, pl_phase2]

for pl in all_pipelines:
    out_path = OUT_DIR / f"{pl['name']}.json"
    out_path.write_text(json.dumps(pl, indent=2), encoding="utf-8")
    act_count = len(pl["properties"]["activities"])
    param_count = len(pl["properties"]["parameters"])
    print(f"  {pl['name']}.json  ({act_count} activities, {param_count} pipeline params)")

print(f"\nGenerated {len(all_pipelines)} pipeline definitions in {OUT_DIR}")
