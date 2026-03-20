# Fabric Notebook
# 16_create_report.py
# Creates or updates the IBP Backtest report (PBIR-Legacy) in the reports/ folder,
# bound to the IBP Forecast Model semantic model.

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

import json, base64, time, uuid, requests

workspace_id = spark.conf.get("trident.workspace.id")
token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

REPORT_NAME = "IBP Backtest - Actual vs Predicted"
SEMANTIC_MODEL_NAME = cfg("semantic_model_name")
ENTITY = "Backtest Predictions"
SRC = "b"

# ── Discover semantic model ID ───────────────────────────────────────────────
sm_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=SemanticModel",
    headers=headers,
)
sm_resp.raise_for_status()
sm_match = [m for m in sm_resp.json().get("value", []) if m["displayName"] == SEMANTIC_MODEL_NAME]
if not sm_match:
    raise Exception(f"Semantic model '{SEMANTIC_MODEL_NAME}' not found. Run notebook 15 first.")
semantic_model_id = sm_match[0]["id"]
logger.info(f"[report] Semantic model: {semantic_model_id}")

# ── Discover or create reports folder ────────────────────────────────────────
folders_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders",
    headers=headers,
)
folders_resp.raise_for_status()
reports_folder_id = None
for f in folders_resp.json().get("value", []):
    if f["displayName"] == "reports":
        reports_folder_id = f["id"]
        break
logger.info(f"[report] Reports folder: {reports_folder_id or 'workspace root'}")

# ── Helper functions ─────────────────────────────────────────────────────────

def b64_encode(obj):
    raw = json.dumps(obj, indent=2) if isinstance(obj, dict) else obj
    return base64.b64encode(raw.encode()).decode()


def col_expr(prop):
    return {"Column": {"Expression": {"SourceRef": {"Source": SRC}}, "Property": prop}}


def msr_expr(prop):
    return {"Measure": {"Expression": {"SourceRef": {"Source": SRC}}, "Property": prop}}


def col_select(prop):
    return {**col_expr(prop), "Name": f"{ENTITY}.{prop}"}


def msr_select(prop):
    return {**msr_expr(prop), "Name": f"{ENTITY}.{prop}"}


def agg_select(prop, func, alias):
    return {"Aggregation": {"Expression": col_expr(prop), "Function": func}, "Name": alias}


def visual_container(name, x, y, z, w, h, visual_type, projections, selects,
                     order_by=None, objects=None):
    cfg_dict = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h}}],
        "singleVisual": {
            "visualType": visual_type,
            "projections": projections,
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": SRC, "Entity": ENTITY, "Type": 0}],
                "Select": selects,
            },
            "drillFilterOtherVisuals": True,
        },
    }
    if order_by:
        cfg_dict["singleVisual"]["prototypeQuery"]["OrderBy"] = order_by
    if objects:
        cfg_dict["singleVisual"]["objects"] = objects
    return {
        "config": json.dumps(cfg_dict), "filters": "[]",
        "height": float(h), "width": float(w),
        "x": float(x), "y": float(y), "z": z,
    }


def wait_for_lro(resp, hdr):
    if resp.status_code != 202:
        return
    op_id = resp.headers.get("x-ms-operation-id", "")
    if not op_id:
        return
    for _ in range(30):
        time.sleep(3)
        poll = requests.get(f"https://api.fabric.microsoft.com/v1/operations/{op_id}", headers=hdr)
        if poll.status_code != 200:
            continue
        status = poll.json().get("status", "")
        logger.info(f"[report] LRO {status}")
        if status == "Succeeded":
            result = requests.get(f"https://api.fabric.microsoft.com/v1/operations/{op_id}/result", headers=hdr)
            return result.json() if result.status_code == 200 else None
        if status == "Failed":
            raise Exception(f"LRO failed: {poll.text}")
    raise Exception("LRO timed out")


# ── Build visual definitions ─────────────────────────────────────────────────
DROPDOWN = {"data": [{"properties": {"mode": {"expr": {"Literal": {"Value": "'Dropdown'"}}}}}]}

SLICER_Y = 15;  SLICER_H = 50
CARD_Y   = 85;  CARD_H   = 75
CHART_Y  = 185; CHART_H  = 220
TABLE_Y  = 425; TABLE_H  = 275

slicer_model = visual_container("vis_slicer_model", 20, SLICER_Y, 0, 250, SLICER_H,
    "slicer", {"Values": [{"queryRef": f"{ENTITY}.model_type"}]},
    [col_select("model_type")], objects=DROPDOWN)

slicer_plant = visual_container("vis_slicer_plant", 285, SLICER_Y, 10, 250, SLICER_H,
    "slicer", {"Values": [{"queryRef": f"{ENTITY}.plant_id"}]},
    [col_select("plant_id")], objects=DROPDOWN)

slicer_sku = visual_container("vis_slicer_sku", 550, SLICER_Y, 20, 250, SLICER_H,
    "slicer", {"Values": [{"queryRef": f"{ENTITY}.sku_id"}]},
    [col_select("sku_id")], objects=DROPDOWN)

card_mape = visual_container("vis_card_mape", 20, CARD_Y, 500, 230, CARD_H,
    "card", {"Values": [{"queryRef": f"{ENTITY}.Backtest MAPE %"}]},
    [msr_select("Backtest MAPE %")])

AVG_ABS = "Avg.abs_error"
card_avg_abs = visual_container("vis_card_avgabs", 265, CARD_Y, 510, 230, CARD_H,
    "card", {"Values": [{"queryRef": AVG_ABS}]},
    [agg_select("abs_error", 1, AVG_ABS)])

AVG_PCT = "Avg.pct_error"
card_avg_pct = visual_container("vis_card_avgpct", 510, CARD_Y, 520, 230, CARD_H,
    "card", {"Values": [{"queryRef": AVG_PCT}]},
    [agg_select("pct_error", 1, AVG_PCT)])

card_actual = visual_container("vis_card_actual", 755, CARD_Y, 530, 230, CARD_H,
    "card", {"Values": [{"queryRef": f"{ENTITY}.Total Actual"}]},
    [msr_select("Total Actual")])

card_pred = visual_container("vis_card_pred", 1000, CARD_Y, 540, 230, CARD_H,
    "card", {"Values": [{"queryRef": f"{ENTITY}.Total Predicted"}]},
    [msr_select("Total Predicted")])

line_chart = visual_container("vis_linechart", 20, CHART_Y, 1000, 1240, CHART_H,
    "lineChart",
    {"Category": [{"queryRef": f"{ENTITY}.period"}],
     "Y": [{"queryRef": f"{ENTITY}.Total Actual"}, {"queryRef": f"{ENTITY}.Total Predicted"}]},
    [col_select("period"), msr_select("Total Actual"), msr_select("Total Predicted")],
    order_by=[{"Direction": 1, "Expression": col_expr("period")}])

TABLE_COLS = ["period", "plant_id", "sku_id", "model_type",
              "actual", "predicted", "error", "abs_error", "pct_error"]
detail_table = visual_container("vis_detail_table", 20, TABLE_Y, 2000, 1240, TABLE_H,
    "tableEx",
    {"Values": [{"queryRef": f"{ENTITY}.{c}"} for c in TABLE_COLS]},
    [col_select(c) for c in TABLE_COLS],
    order_by=[{"Direction": 1, "Expression": col_expr("period")}])

# ── Build PBIR-Legacy report.json ────────────────────────────────────────────

report_config = {
    "version": "5.68",
    "themeCollection": {"baseTheme": {
        "name": "CY25SU11",
        "version": {"visual": "2.4.0", "report": "3.0.0", "page": "2.3.0"},
        "type": 2}},
    "activeSectionIndex": 0,
    "defaultDrillFilterOtherVisuals": True,
    "settings": {
        "useNewFilterPaneExperience": True, "allowChangeFilterTypes": True,
        "useStylableVisualContainerHeader": True, "useDefaultAggregateDisplayName": True,
        "useEnhancedTooltips": True},
}

report_json = {
    "config": json.dumps(report_config),
    "layoutOptimization": 0,
    "resourcePackages": [{"resourcePackage": {
        "disabled": False,
        "items": [{"name": "CY25SU11", "path": "BaseThemes/CY25SU11.json", "type": 202}],
        "name": "SharedResources", "type": 2}}],
    "sections": [{
        "config": "{}", "displayName": "Backtest: Actual vs Predicted",
        "displayOption": 1, "filters": "[]",
        "height": 720.00, "width": 1280.00, "name": "backtestpage01",
        "visualContainers": [
            slicer_model, slicer_plant, slicer_sku,
            card_mape, card_avg_abs, card_avg_pct, card_actual, card_pred,
            line_chart, detail_table,
        ],
    }],
}

definition_pbir = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
    "version": "4.0",
    "datasetReference": {"byConnection": {"connectionString": f"semanticmodelid={semantic_model_id}"}},
}

platform_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Report", "displayName": REPORT_NAME},
    "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
}

parts = [
    {"path": "report.json",     "payload": b64_encode(report_json),     "payloadType": "InlineBase64"},
    {"path": "definition.pbir", "payload": b64_encode(definition_pbir), "payloadType": "InlineBase64"},
    {"path": ".platform",       "payload": b64_encode(platform_json),   "payloadType": "InlineBase64"},
]

definition = {"parts": parts}

# ── Create or update report ──────────────────────────────────────────────────

existing_reports = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report",
    headers=headers,
)
existing_reports.raise_for_status()
existing = [r for r in existing_reports.json().get("value", []) if r["displayName"] == REPORT_NAME]

if existing:
    report_id = existing[0]["id"]
    logger.info(f"[report] Report exists ({report_id}), updating definition...")
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{report_id}/updateDefinition",
        headers=headers,
        json={"definition": definition},
    )
    if resp.status_code in (200, 202):
        wait_for_lro(resp, headers)
        logger.info("[report] Definition updated.")
    else:
        raise Exception(f"updateDefinition failed: {resp.status_code} -- {resp.text}")
else:
    logger.info("[report] Creating new report...")
    body = {
        "displayName": REPORT_NAME,
        "description": "Backtest analysis: actual vs predicted by model type with error metrics, sorted ascending by period.",
        "definition": definition,
    }
    if reports_folder_id:
        body["folderId"] = reports_folder_id
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports",
        headers=headers,
        json=body,
    )
    if resp.status_code in (200, 201):
        report_id = resp.json().get("id", "")
        logger.info(f"[report] Created: {report_id}")
    elif resp.status_code == 202:
        result = wait_for_lro(resp, headers)
        report_id = result.get("id", "") if result else ""
        logger.info(f"[report] Created: {report_id}")
    else:
        raise Exception(f"Create failed: {resp.status_code} -- {resp.text}")

logger.info(f"[report] URL: https://app.fabric.microsoft.com/groups/{workspace_id}/reports/{report_id}")
logger.info("[report] Complete.")
