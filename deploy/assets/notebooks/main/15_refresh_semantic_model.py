# Fabric Notebook
# 15_refresh_semantic_model.py
# Creates the DirectLake semantic model on first run, updates definition on subsequent runs,
# then triggers a refresh. All tables must exist in lh_ibp_gold before this runs.

# @parameters
gold_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module

import json, requests, base64, time

semantic_model_name = cfg("semantic_model_name")
logger.info(f"[semantic] Create/update semantic model: {semantic_model_name}")

workspace_id = spark.conf.get("trident.workspace.id")
token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

gold_detail = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{gold_lakehouse_id}",
    headers=headers,
).json()
sql_endpoint = gold_detail["properties"]["sqlEndpointProperties"]["connectionString"]
lh_name = gold_detail["displayName"]
logger.info(f"[semantic] Gold SQL endpoint: {sql_endpoint}")
logger.info(f"[semantic] Gold lakehouse:    {lh_name}")

folders_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders",
    headers=headers,
)
sm_folder_id = None
for f in folders_resp.json().get("value", []):
    if f["displayName"] == "semantic_models":
        sm_folder_id = f["id"]
        break
logger.info(f"[semantic] Target folder: {sm_folder_id or 'workspace root'}")

bim = {
    "compatibilityLevel": 1604,
    "model": {
        "culture": "en-US",
        "defaultMode": "directLake",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "discourageImplicitMeasures": True,
        "tables": [
            {
                "name": "Forecast Versions",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id",       "summarizeBy": "none"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id",         "summarizeBy": "none"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period",         "summarizeBy": "none"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons",  "summarizeBy": "sum"},
                    {"name": "model_type",     "dataType": "string",  "sourceColumn": "model_type",     "summarizeBy": "none"},
                    {"name": "version_type",   "dataType": "string",  "sourceColumn": "version_type",   "summarizeBy": "none"},
                    {"name": "version_id",     "dataType": "string",  "sourceColumn": "version_id",     "summarizeBy": "none"},
                    {"name": "snapshot_month", "dataType": "string",  "sourceColumn": "snapshot_month",  "summarizeBy": "none"},
                ],
                "measures": [
                    {"name": "Total Forecast Tons", "expression": "SUM('Forecast Versions'[forecast_tons])"},
                    {"name": "Avg Forecast Tons",   "expression": "AVERAGE('Forecast Versions'[forecast_tons])", "formatString": "0.00"},
                ],
                "partitions": [{"name": "forecast_versions", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "forecast_versions", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Reporting Actuals vs Forecast",
                "columns": [
                    {"name": "plant_id",       "dataType": "string",  "sourceColumn": "plant_id",       "summarizeBy": "none"},
                    {"name": "sku_id",         "dataType": "string",  "sourceColumn": "sku_id",         "summarizeBy": "none"},
                    {"name": "period",         "dataType": "string",  "sourceColumn": "period",         "summarizeBy": "none"},
                    {"name": "forecast_tons",  "dataType": "double",  "sourceColumn": "forecast_tons",  "summarizeBy": "sum"},
                    {"name": "actual_tons",    "dataType": "double",  "sourceColumn": "actual_tons",    "summarizeBy": "sum"},
                    {"name": "abs_error",      "dataType": "double",  "sourceColumn": "abs_error",      "summarizeBy": "sum"},
                    {"name": "pct_error",      "dataType": "double",  "sourceColumn": "pct_error",      "summarizeBy": "none"},
                    {"name": "variance",       "dataType": "double",  "sourceColumn": "variance",       "summarizeBy": "sum"},
                    {"name": "model_type",     "dataType": "string",  "sourceColumn": "model_type",     "summarizeBy": "none"},
                    {"name": "version_type",   "dataType": "string",  "sourceColumn": "version_type",   "summarizeBy": "none"},
                    {"name": "version_id",     "dataType": "string",  "sourceColumn": "version_id",     "summarizeBy": "none"},
                    {"name": "is_future",      "dataType": "boolean", "sourceColumn": "is_future",      "summarizeBy": "none"},
                    {"name": "snapshot_date",  "dataType": "string",  "sourceColumn": "snapshot_date",  "summarizeBy": "none"},
                ],
                "measures": [
                    {"name": "Total Actual Tons",     "expression": "SUM('Reporting Actuals vs Forecast'[actual_tons])"},
                    {"name": "Total Variance",        "expression": "SUM('Reporting Actuals vs Forecast'[variance])"},
                    {"name": "MAPE %",                "expression": "DIVIDE(SUM('Reporting Actuals vs Forecast'[abs_error]),SUM('Reporting Actuals vs Forecast'[actual_tons]),BLANK())*100", "formatString": "0.0"},
                    {"name": "Bias %",                "expression": "DIVIDE([Total Variance],[Total Actual Tons],BLANK())*100", "formatString": "0.0"},
                    {"name": "Forecast Accuracy %",   "expression": "100-[MAPE %]", "formatString": "0.0"},
                    {"name": "Future Forecast Tons",  "expression": "CALCULATE(SUM('Reporting Actuals vs Forecast'[forecast_tons]),'Reporting Actuals vs Forecast'[is_future]=TRUE())"},
                ],
                "partitions": [{"name": "reporting_actuals_vs_forecast", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "reporting_actuals_vs_forecast", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Master SKU",
                "columns": [
                    {"name": "sku_id",    "dataType": "string", "sourceColumn": "sku_id",    "summarizeBy": "none", "isKey": True},
                    {"name": "sku_name",  "dataType": "string", "sourceColumn": "sku_name",  "summarizeBy": "none"},
                    {"name": "sku_group", "dataType": "string", "sourceColumn": "sku_group", "summarizeBy": "none"},
                ],
                "partitions": [{"name": "master_sku", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "master_sku", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Master Plant",
                "columns": [
                    {"name": "plant_id",   "dataType": "string", "sourceColumn": "plant_id",   "summarizeBy": "none", "isKey": True},
                    {"name": "plant_name", "dataType": "string", "sourceColumn": "plant_name", "summarizeBy": "none"},
                    {"name": "region",     "dataType": "string", "sourceColumn": "region",     "summarizeBy": "none"},
                ],
                "partitions": [{"name": "master_plant", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "master_plant", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Capacity Translation",
                "columns": [
                    {"name": "plant_id",         "dataType": "string", "sourceColumn": "plant_id",         "summarizeBy": "none"},
                    {"name": "sku_id",           "dataType": "string", "sourceColumn": "sku_id",           "summarizeBy": "none"},
                    {"name": "period",           "dataType": "string", "sourceColumn": "period",           "summarizeBy": "none"},
                    {"name": "forecast_tons",    "dataType": "double", "sourceColumn": "forecast_tons",    "summarizeBy": "sum"},
                    {"name": "lineal_feet",      "dataType": "double", "sourceColumn": "lineal_feet",      "summarizeBy": "sum"},
                    {"name": "production_hours", "dataType": "double", "sourceColumn": "production_hours", "summarizeBy": "sum"},
                ],
                "measures": [
                    {"name": "Total Lineal Feet",      "expression": "SUM('Capacity Translation'[lineal_feet])",      "formatString": "#,0"},
                    {"name": "Total Production Hours",  "expression": "SUM('Capacity Translation'[production_hours])", "formatString": "#,0.0"},
                ],
                "partitions": [{"name": "capacity_translation", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "capacity_translation", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
            {
                "name": "Backtest Predictions",
                "columns": [
                    {"name": "plant_id",    "dataType": "string", "sourceColumn": "plant_id",    "summarizeBy": "none"},
                    {"name": "sku_id",      "dataType": "string", "sourceColumn": "sku_id",      "summarizeBy": "none"},
                    {"name": "period",      "dataType": "string", "sourceColumn": "period",      "summarizeBy": "none"},
                    {"name": "actual",      "dataType": "double", "sourceColumn": "actual",      "summarizeBy": "sum"},
                    {"name": "predicted",   "dataType": "double", "sourceColumn": "predicted",   "summarizeBy": "sum"},
                    {"name": "model_type",  "dataType": "string", "sourceColumn": "model_type",  "summarizeBy": "none"},
                    {"name": "error",       "dataType": "double", "sourceColumn": "error",       "summarizeBy": "sum"},
                    {"name": "abs_error",   "dataType": "double", "sourceColumn": "abs_error",   "summarizeBy": "sum"},
                    {"name": "pct_error",   "dataType": "double", "sourceColumn": "pct_error",   "summarizeBy": "none"},
                ],
                "measures": [
                    {"name": "Backtest MAPE %", "expression": "DIVIDE(SUM('Backtest Predictions'[abs_error]),SUM('Backtest Predictions'[actual]),BLANK())*100", "formatString": "0.0"},
                    {"name": "Total Actual",    "expression": "SUM('Backtest Predictions'[actual])",    "formatString": "#,0.0"},
                    {"name": "Total Predicted", "expression": "SUM('Backtest Predictions'[predicted])", "formatString": "#,0.0"},
                ],
                "partitions": [{"name": "backtest_predictions", "mode": "directLake",
                    "source": {"type": "entity", "entityName": "backtest_predictions", "schemaName": "dbo", "expressionSource": "DatabaseQuery"}}],
            },
        ],
        "relationships": [
            {"name": "FK_FV_SKU",          "fromTable": "Forecast Versions",             "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_FV_Plant",        "fromTable": "Forecast Versions",             "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
            {"name": "FK_Reporting_SKU",   "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_Reporting_Plant", "fromTable": "Reporting Actuals vs Forecast", "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
            {"name": "FK_Capacity_SKU",    "fromTable": "Capacity Translation",          "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_Capacity_Plant",  "fromTable": "Capacity Translation",          "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
            {"name": "FK_Backtest_SKU",    "fromTable": "Backtest Predictions",           "fromColumn": "sku_id",   "toTable": "Master SKU",   "toColumn": "sku_id"},
            {"name": "FK_Backtest_Plant",  "fromTable": "Backtest Predictions",           "fromColumn": "plant_id", "toTable": "Master Plant", "toColumn": "plant_id"},
        ],
        "expressions": [
            {
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": f"let\n    database = Sql.Database(\"{sql_endpoint}\", \"{lh_name}\")\nin\n    database",
            }
        ],
        "annotations": [{"name": "PBI_QueryRelationships", "value": "[]"}],
    },
}

bim_bytes = json.dumps(bim).encode("utf-8")
bim_b64 = base64.b64encode(bim_bytes).decode("utf-8")

pbism = json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
    "version": "5.0",
    "settings": {"qnaEnabled": False},
})
pbism_b64 = base64.b64encode(pbism.encode("utf-8")).decode("utf-8")

definition = {
    "parts": [
        {"path": "model.bim",        "payload": bim_b64,   "payloadType": "InlineBase64"},
        {"path": "definition.pbism",  "payload": pbism_b64, "payloadType": "InlineBase64"},
    ]
}

sm_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=SemanticModel",
    headers=headers,
)
sm_resp.raise_for_status()
existing = [m for m in sm_resp.json().get("value", []) if m["displayName"] == semantic_model_name]

if existing:
    sm_id = existing[0]["id"]
    logger.info(f"[semantic] Model exists ({sm_id}), updating definition...")
    update_resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{sm_id}/updateDefinition",
        headers=headers,
        json={"definition": definition},
    )
    if update_resp.status_code in (200, 202):
        if update_resp.status_code == 202 and "Location" in update_resp.headers:
            lro_url = update_resp.headers["Location"]
            for _ in range(20):
                time.sleep(5)
                poll = requests.get(lro_url, headers=headers).json()
                if poll.get("status") in ("Succeeded", "Completed"):
                    break
                if poll.get("status") == "Failed":
                    raise Exception(f"Update LRO failed: {poll.get('error', {}).get('message', 'unknown')}")
        logger.info("[semantic] Definition updated.")
    else:
        raise Exception(f"updateDefinition failed: {update_resp.status_code} -- {update_resp.text}")
else:
    logger.info("[semantic] Model does not exist, creating...")
    create_body = {
        "displayName": semantic_model_name,
        "type": "SemanticModel",
        "definition": definition,
    }
    if sm_folder_id:
        create_body["folderId"] = sm_folder_id
    create_resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items",
        headers=headers,
        json=create_body,
    )
    if create_resp.status_code not in (200, 201, 202):
        raise Exception(f"Create failed: {create_resp.status_code} -- {create_resp.text}")
    if create_resp.status_code == 202 and "Location" in create_resp.headers:
        lro_url = create_resp.headers["Location"]
        for _ in range(20):
            time.sleep(8)
            poll = requests.get(lro_url, headers=headers).json()
            if poll.get("status") in ("Succeeded", "Completed"):
                break
            if poll.get("status") == "Failed":
                raise Exception(f"Create LRO failed: {poll.get('error', {}).get('message', 'unknown')}")
    logger.info("[semantic] Model created.")

sm_items = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=SemanticModel",
    headers=headers,
).json()
sm_final = [m for m in sm_items.get("value", []) if m["displayName"] == semantic_model_name]
if sm_final:
    sm_id = sm_final[0]["id"]
    logger.info(f"[semantic] Triggering refresh for {sm_id}...")
    pbi_token = notebookutils.credentials.getToken("pbi")
    refresh_resp = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{sm_id}/refreshes",
        headers={"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"},
        json={"type": "Full"},
    )
    if refresh_resp.status_code in (200, 202):
        logger.info(f"[semantic] Refresh triggered (status={refresh_resp.status_code})")
    else:
        logger.error(f"[semantic] Refresh failed: {refresh_resp.status_code} -- {refresh_resp.text}")
        raise Exception(f"Refresh failed: {refresh_resp.status_code}")

logger.info("[semantic] Complete.")
