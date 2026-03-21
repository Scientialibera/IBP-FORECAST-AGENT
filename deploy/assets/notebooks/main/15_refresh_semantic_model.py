# Fabric Notebook
# 15_refresh_semantic_model.py
# Creates the DirectLake semantic model on first run, updates definition on subsequent runs,
# then triggers a refresh. All tables must exist in lh_ibp_gold before this runs.

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/schemas_module


gold_lakehouse_id = resolve_lakehouse_id("", "gold")

import json, requests, base64, time

semantic_model_name = named(cfg("semantic_model_name"))
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
    if f["displayName"] == cfg("semantic_models_folder"):
        sm_folder_id = f["id"]
        break
logger.info(f"[semantic] Target folder: {sm_folder_id or 'workspace root'}")

bim = build_bim(sql_endpoint, lh_name)

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

# ── Configure scheduled refresh ─────────────────────────────────
schedule_enabled  = cfg("refresh_schedule_enabled")
schedule_time     = cfg("refresh_schedule_time")
schedule_timezone = cfg("refresh_schedule_timezone")

if schedule_enabled and schedule_time and sm_final:
    try:
        pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
        schedule_body = {
            "value": {
                "enabled": True,
                "days": ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
                "times": [schedule_time],
                "localTimeZoneId": schedule_timezone or "UTC",
                "notifyOption": "MailOnFailure",
            }
        }
        sched_resp = requests.patch(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{sm_id}/refreshSchedule",
            headers=pbi_headers,
            json=schedule_body,
        )
        if sched_resp.status_code == 200:
            logger.info("[semantic] Refresh schedule set: daily at %s %s", schedule_time, schedule_timezone or "UTC")
        else:
            logger.warning("[semantic] Failed to set schedule: %s -- %s", sched_resp.status_code, sched_resp.text)
    except Exception as e:
        logger.warning("[semantic] Schedule configuration failed: %s", e)
else:
    logger.info("[semantic] Scheduled refresh not configured (disabled or no model)")

logger.info("[semantic] Complete.")
