# Fabric Notebook
# 16_create_report.py
#
# Creates or updates a NON-LEGACY PBIR report fully from code.
# No manual report/template required.
#
# It uses:
# - PBIR report format (definition.pbir + definition/ folder parts)
# - Fabric Create Report API
# - Fabric Update Report Definition API
#
# Assumptions:
# - Semantic model already exists (created by notebook 15)
# - The semantic model contains a table/entity named "Backtest Predictions"
# - That entity exposes these fields/measures:
#     Columns: period, plant_id, sku_id, model_type, actual, predicted, error, abs_error, pct_error
#     Measures: Backtest MAPE %, Total Actual, Total Predicted
#
# If your semantic model uses different names, update ENTITY / fields / measures below.

# @parameters
gold_lakehouse_id = ""   # kept for pipeline compatibility; not used directly in this notebook
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

import base64
import json
import time
import uuid
import requests

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
workspace_id = spark.conf.get("trident.workspace.id")
token = notebookutils.credentials.getToken("pbi")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

REPORT_NAME = "IBP Backtest - Actual vs Predicted"
REPORT_DESCRIPTION = "Programmatically generated PBIR report for backtest accuracy review."
SEMANTIC_MODEL_NAME = cfg("semantic_model_name")

# Semantic model entity + field names used by the report
ENTITY = "Backtest Predictions"
COL_PERIOD = "period"
COL_PLANT = "plant_id"
COL_SKU = "sku_id"
COL_MODEL = "model_type"
COL_ACTUAL = "actual"
COL_PRED = "predicted"
COL_ERROR = "error"
COL_ABS_ERROR = "abs_error"
COL_PCT_ERROR = "pct_error"

MSR_MAPE = "Backtest MAPE %"
MSR_TOTAL_ACTUAL = "Total Actual"
MSR_TOTAL_PRED = "Total Predicted"

# Stable IDs so updateDefinition keeps the same structure
PAGE_SUMMARY = "7f27c9d2e8a54c4db101"
PAGE_DETAIL  = "e91ab8d4c7f1438da202"

V_SLICER_MODEL  = "1a01c4d9e9af4d8da001"
V_SLICER_PLANT  = "1a01c4d9e9af4d8da002"
V_SLICER_SKU    = "1a01c4d9e9af4d8da003"

V_CARD_MAPE     = "1a01c4d9e9af4d8da011"
V_CARD_ACTUAL   = "1a01c4d9e9af4d8da012"
V_CARD_PRED     = "1a01c4d9e9af4d8da013"

V_LINE_AP       = "1a01c4d9e9af4d8da021"
V_BAR_MODEL     = "1a01c4d9e9af4d8da022"
V_BAR_SKU       = "1a01c4d9e9af4d8da023"

V_DETAIL_TABLE  = "1a01c4d9e9af4d8da031"
V_DETAIL_LINE   = "1a01c4d9e9af4d8da032"

PAGE_WIDTH = 1366
PAGE_HEIGHT = 768

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def b64_text(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def b64_json(obj: dict) -> str:
    return b64_text(json.dumps(obj, indent=2))


def lit(value):
    if isinstance(value, bool):
        v = "true" if value else "false"
    elif isinstance(value, (int, float)):
        v = str(value)
    else:
        v = f"'{value}'"
    return {"expr": {"Literal": {"Value": v}}}


def src_ref():
    return {"SourceRef": {"Entity": ENTITY}}


def col_field(name: str) -> dict:
    return {
        "Column": {
            "Expression": src_ref(),
            "Property": name,
        }
    }


def msr_field(name: str) -> dict:
    return {
        "Measure": {
            "Expression": src_ref(),
            "Property": name,
        }
    }


def qref_col(name: str) -> str:
    return f"{ENTITY}.{name}"


def qref_msr(name: str) -> str:
    return f"{ENTITY}.{name}"


def proj(field: dict, query_ref: str, active: bool = True) -> dict:
    p = {
        "field": field,
        "queryRef": query_ref,
    }
    if active:
        p["active"] = True
    return p


def pos(x, y, z, w, h):
    return {
        "x": x,
        "y": y,
        "z": z,
        "width": w,
        "height": h,
        "tabOrder": z,
    }


def visual_base(visual_id: str, visual_type: str, position: dict, query_state: dict,
                sort_def: dict | None = None, objects: dict | None = None) -> dict:
    visual = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
        "name": visual_id,
        "position": position,
        "visual": {
            "visualType": visual_type,
            "query": {
                "queryState": query_state
            },
            "drillFilterOtherVisuals": True,
        },
        "filterConfig": {"filters": []},
    }
    if sort_def:
        visual["visual"]["query"]["sortDefinition"] = sort_def
    if objects:
        visual["visual"]["objects"] = objects
    return visual


def slicer_visual(visual_id: str, field_name: str, x: int, y: int, w: int = 260, h: int = 56) -> dict:
    return visual_base(
        visual_id=visual_id,
        visual_type="slicer",
        position=pos(x, y, 10 + x, w, h),
        query_state={
            "Values": {
                "projections": [
                    proj(col_field(field_name), qref_col(field_name))
                ]
            }
        },
        objects={
            "data": [
                {
                    "properties": {
                        "mode": lit("Dropdown")
                    }
                }
            ]
        }
    )


def card_visual(visual_id: str, measure_name: str, x: int, y: int, w: int = 250, h: int = 90) -> dict:
    return visual_base(
        visual_id=visual_id,
        visual_type="card",
        position=pos(x, y, 1000 + x, w, h),
        query_state={
            "Values": {
                "projections": [
                    proj(msr_field(measure_name), qref_msr(measure_name), active=False)
                ]
            }
        }
    )


def line_chart_visual(visual_id: str, x: int, y: int, w: int, h: int) -> dict:
    return visual_base(
        visual_id=visual_id,
        visual_type="lineChart",
        position=pos(x, y, 2000 + x, w, h),
        query_state={
            "Category": {
                "projections": [
                    proj(col_field(COL_PERIOD), qref_col(COL_PERIOD))
                ]
            },
            "Y": {
                "projections": [
                    proj(msr_field(MSR_TOTAL_ACTUAL), qref_msr(MSR_TOTAL_ACTUAL), active=False),
                    proj(msr_field(MSR_TOTAL_PRED), qref_msr(MSR_TOTAL_PRED), active=False),
                ]
            }
        },
        sort_def={
            "sort": [
                {
                    "field": col_field(COL_PERIOD),
                    "direction": "Ascending"
                }
            ]
        }
    )


def bar_chart_measure_by_column(visual_id: str, category_col: str, measure_name: str,
                                x: int, y: int, w: int, h: int, descending: bool = True) -> dict:
    return visual_base(
        visual_id=visual_id,
        visual_type="barChart",
        position=pos(x, y, 3000 + x, w, h),
        query_state={
            "Category": {
                "projections": [
                    proj(col_field(category_col), qref_col(category_col))
                ]
            },
            "Y": {
                "projections": [
                    proj(msr_field(measure_name), qref_msr(measure_name), active=False)
                ]
            }
        },
        sort_def={
            "sort": [
                {
                    "field": msr_field(measure_name),
                    "direction": "Descending" if descending else "Ascending"
                }
            ]
        }
    )


def table_visual(visual_id: str, columns: list[str], x: int, y: int, w: int, h: int) -> dict:
    return visual_base(
        visual_id=visual_id,
        visual_type="tableEx",
        position=pos(x, y, 4000 + x, w, h),
        query_state={
            "Values": {
                "projections": [
                    proj(col_field(c), qref_col(c), active=False) for c in columns
                ]
            }
        },
        sort_def={
            "sort": [
                {
                    "field": col_field(COL_PERIOD),
                    "direction": "Ascending"
                }
            ]
        }
    )


def wait_for_lro(resp: requests.Response, headers: dict):
    if resp.status_code not in (200, 201, 202):
        raise Exception(f"Request failed: {resp.status_code} -- {resp.text}")

    if resp.status_code in (200, 201):
        try:
            return resp.json()
        except Exception:
            return None

    # 202 Accepted
    op_id = resp.headers.get("x-ms-operation-id")
    location = resp.headers.get("Location")
    retry_after = int(resp.headers.get("Retry-After", "5"))

    for _ in range(40):
        time.sleep(retry_after)

        if op_id:
            poll = requests.get(
                f"https://api.fabric.microsoft.com/v1/operations/{op_id}",
                headers=headers,
            )
            if poll.status_code == 200:
                body = poll.json()
                status = body.get("status")
                print(f"[report] LRO status: {status}")
                if status in ("Succeeded", "Completed"):
                    result = requests.get(
                        f"https://api.fabric.microsoft.com/v1/operations/{op_id}/result",
                        headers=headers,
                    )
                    if result.status_code == 200:
                        try:
                            return result.json()
                        except Exception:
                            return None
                    return None
                if status == "Failed":
                    raise Exception(f"LRO failed: {poll.text}")

        elif location:
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                body = poll.json()
                status = body.get("status")
                print(f"[report] LRO status: {status}")
                if status in ("Succeeded", "Completed"):
                    return body
                if status == "Failed":
                    raise Exception(f"LRO failed: {poll.text}")

    raise Exception("LRO timed out")


# -----------------------------------------------------------------------------
# FIND SEMANTIC MODEL
# -----------------------------------------------------------------------------
sm_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=SemanticModel",
    headers=headers,
)
sm_resp.raise_for_status()
sm_match = [m for m in sm_resp.json().get("value", []) if m["displayName"] == SEMANTIC_MODEL_NAME]
if not sm_match:
    raise Exception(f"Semantic model '{SEMANTIC_MODEL_NAME}' not found. Run notebook 15 first.")
semantic_model_id = sm_match[0]["id"]
print(f"[report] Semantic model: {semantic_model_id}")

# -----------------------------------------------------------------------------
# FIND reports FOLDER IF IT EXISTS
# -----------------------------------------------------------------------------
reports_folder_id = None
folders_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders",
    headers=headers,
)
if folders_resp.status_code == 200:
    for f in folders_resp.json().get("value", []):
        if f["displayName"] == "reports":
            reports_folder_id = f["id"]
            break

print(f"[report] Reports folder: {reports_folder_id or 'workspace root'}")

# -----------------------------------------------------------------------------
# THEME
# -----------------------------------------------------------------------------
theme_name = "IBPBacktestTheme"
theme_json = {
    "name": theme_name,
    "background": "#FFFFFF",
    "foreground": "#111827",
    "tableAccent": "#2563EB",
    "dataColors": [
        "#2563EB",  # blue
        "#0F172A",  # dark slate
        "#0F766E",  # teal
        "#7C3AED",  # violet
        "#F59E0B",  # amber
        "#DC2626",  # red
        "#059669",  # emerald
    ],
    "visualStyles": {
        "*": {
            "*": {
                "title": [
                    {
                        "show": True,
                        "fontFamily": "Segoe UI Semibold",
                        "fontSize": 12,
                        "color": {"solid": {"color": "#111827"}}
                    }
                ],
                "background": [
                    {
                        "show": False,
                        "transparency": 100
                    }
                ]
            }
        }
    }
}

# -----------------------------------------------------------------------------
# PBIR FILES
# -----------------------------------------------------------------------------
report_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json",
    "themeCollection": {
        "baseTheme": {
            "name": "CY26SU02",
            "reportVersionAtImport": {
                "visual": "2.6.0",
                "report": "3.1.0",
                "page": "2.3.0"
            },
            "type": "SharedResources"
        },
        "customTheme": {
            "name": theme_name,
            "reportVersionAtImport": {
                "visual": "2.6.0",
                "report": "3.1.0",
                "page": "2.3.0"
            },
            "type": "RegisteredResources"
        }
    },
    "resourcePackages": [
        {
            "name": "SharedResources",
            "type": "SharedResources",
            "items": [
                {
                    "name": "CY26SU02",
                    "path": "BaseThemes/CY26SU02.json",
                    "type": "BaseTheme"
                }
            ]
        },
        {
            "name": "RegisteredResources",
            "type": "RegisteredResources",
            "items": [
                {
                    "name": f"{theme_name}.json",
                    "path": f"{theme_name}.json",
                    "type": "CustomTheme"
                }
            ]
        }
    ],
    "settings": {
        "useStylableVisualContainerHeader": True,
        "defaultFilterActionIsDataFilter": True,
        "defaultDrillFilterOtherVisuals": True,
        "allowChangeFilterTypes": True,
        "allowInlineExploration": True,
        "useEnhancedTooltips": True
    }
}

version_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
    "version": "1.0.0"
}

pages_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
    "pageOrder": [PAGE_SUMMARY, PAGE_DETAIL],
    "activePageName": PAGE_SUMMARY
}

page_summary_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
    "name": PAGE_SUMMARY,
    "displayName": "Summary",
    "displayOption": "FitToPage",
    "height": PAGE_HEIGHT,
    "width": PAGE_WIDTH
}

page_detail_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
    "name": PAGE_DETAIL,
    "displayName": "Detail",
    "displayOption": "FitToPage",
    "height": PAGE_HEIGHT,
    "width": PAGE_WIDTH
}

definition_pbir = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
    "version": "4.0",
    "datasetReference": {
        "byConnection": {
            "connectionString": f"semanticmodelid={semantic_model_id}"
        }
    }
}

platform_json = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {
        "type": "Report",
        "displayName": REPORT_NAME
    },
    "config": {
        "version": "2.0",
        "logicalId": str(uuid.uuid4())
    }
}

# -----------------------------------------------------------------------------
# VISUALS - SUMMARY PAGE
# -----------------------------------------------------------------------------
summary_visuals = {
    V_SLICER_MODEL: slicer_visual(V_SLICER_MODEL, COL_MODEL, 20, 18, 250, 56),
    V_SLICER_PLANT: slicer_visual(V_SLICER_PLANT, COL_PLANT, 290, 18, 250, 56),
    V_SLICER_SKU: slicer_visual(V_SLICER_SKU, COL_SKU, 560, 18, 250, 56),

    V_CARD_MAPE: card_visual(V_CARD_MAPE, MSR_MAPE, 20, 95, 250, 95),
    V_CARD_ACTUAL: card_visual(V_CARD_ACTUAL, MSR_TOTAL_ACTUAL, 290, 95, 250, 95),
    V_CARD_PRED: card_visual(V_CARD_PRED, MSR_TOTAL_PRED, 560, 95, 250, 95),

    V_LINE_AP: line_chart_visual(V_LINE_AP, 20, 215, 820, 240),

    V_BAR_MODEL: bar_chart_measure_by_column(
        V_BAR_MODEL,
        COL_MODEL,
        MSR_TOTAL_PRED,
        860, 215, 486, 240,
        descending=True
    ),

    V_BAR_SKU: bar_chart_measure_by_column(
        V_BAR_SKU,
        COL_SKU,
        MSR_TOTAL_PRED,
        20, 475, 1326, 245,
        descending=True
    ),
}

# -----------------------------------------------------------------------------
# VISUALS - DETAIL PAGE
# -----------------------------------------------------------------------------
detail_visuals = {
    "detail_slicer_model": slicer_visual("detail_slicer_model", COL_MODEL, 20, 18, 250, 56),
    "detail_slicer_plant": slicer_visual("detail_slicer_plant", COL_PLANT, 290, 18, 250, 56),
    "detail_slicer_sku": slicer_visual("detail_slicer_sku", COL_SKU, 560, 18, 250, 56),

    V_DETAIL_LINE: line_chart_visual(V_DETAIL_LINE, 20, 95, 1326, 220),

    V_DETAIL_TABLE: table_visual(
        V_DETAIL_TABLE,
        [
            COL_PERIOD,
            COL_PLANT,
            COL_SKU,
            COL_MODEL,
            COL_ACTUAL,
            COL_PRED,
            COL_ERROR,
            COL_ABS_ERROR,
            COL_PCT_ERROR,
        ],
        20, 335, 1326, 385
    ),
}

# -----------------------------------------------------------------------------
# BUILD DEFINITION PARTS
# -----------------------------------------------------------------------------
parts = [
    # Core PBIR files
    {
        "path": "definition/report.json",
        "payload": b64_json(report_json),
        "payloadType": "InlineBase64"
    },
    {
        "path": "definition/version.json",
        "payload": b64_json(version_json),
        "payloadType": "InlineBase64"
    },
    {
        "path": "definition/pages/pages.json",
        "payload": b64_json(pages_json),
        "payloadType": "InlineBase64"
    },
    {
        "path": f"definition/pages/{PAGE_SUMMARY}/page.json",
        "payload": b64_json(page_summary_json),
        "payloadType": "InlineBase64"
    },
    {
        "path": f"definition/pages/{PAGE_DETAIL}/page.json",
        "payload": b64_json(page_detail_json),
        "payloadType": "InlineBase64"
    },

    # Theme
    {
        "path": f"StaticResources/RegisteredResources/{theme_name}.json",
        "payload": b64_json(theme_json),
        "payloadType": "InlineBase64"
    },

    # Semantic model binding
    {
        "path": "definition.pbir",
        "payload": b64_json(definition_pbir),
        "payloadType": "InlineBase64"
    },

    # Platform metadata
    {
        "path": ".platform",
        "payload": b64_json(platform_json),
        "payloadType": "InlineBase64"
    },
]

for vid, vjson in summary_visuals.items():
    parts.append({
        "path": f"definition/pages/{PAGE_SUMMARY}/visuals/{vid}/visual.json",
        "payload": b64_json(vjson),
        "payloadType": "InlineBase64"
    })

for vid, vjson in detail_visuals.items():
    parts.append({
        "path": f"definition/pages/{PAGE_DETAIL}/visuals/{vid}/visual.json",
        "payload": b64_json(vjson),
        "payloadType": "InlineBase64"
    })

definition = {"parts": parts}

# -----------------------------------------------------------------------------
# CREATE OR UPDATE REPORT
# -----------------------------------------------------------------------------
existing_reports = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report",
    headers=headers,
)
existing_reports.raise_for_status()
existing = [r for r in existing_reports.json().get("value", []) if r["displayName"] == REPORT_NAME]

report_id = None

if existing:
    report_id = existing[0]["id"]
    print(f"[report] Report exists ({report_id}), updating PBIR definition...")

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/updateDefinition",
        headers=headers,
        json={"definition": definition},
    )
    wait_for_lro(resp, headers)
    print("[report] Definition updated.")
else:
    print("[report] Creating new PBIR report...")
    body = {
        "displayName": REPORT_NAME,
        "description": REPORT_DESCRIPTION,
        "definition": definition,
    }
    if reports_folder_id:
        body["folderId"] = reports_folder_id

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports",
        headers=headers,
        json=body,
    )

    result = wait_for_lro(resp, headers)
    if result and isinstance(result, dict):
        report_id = result.get("id")

    if not report_id:
        # fallback lookup by name
        time.sleep(3)
        verify = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report",
            headers=headers,
        )
        verify.raise_for_status()
        match = [r for r in verify.json().get("value", []) if r["displayName"] == REPORT_NAME]
        if match:
            report_id = match[0]["id"]

    print(f"[report] Created: {report_id}")

if not report_id:
    raise Exception("Report ID could not be resolved after create/update.")

print(f"[report] URL: https://app.fabric.microsoft.com/groups/{workspace_id}/reports/{report_id}")
print("[report] Complete.")
