# Fabric Notebook -- Module
# config_module.py -- Lakehouse I/O utilities
# Lakehouse IDs come from parameter cells; all other config from ibp_config.

import os
import json
import logging

_cfg_logger = logging.getLogger("config")

def _get_workspace_id() -> str:
    try:
        return spark.conf.get("trident.workspace.id")
    except Exception:
        pass
    return os.environ.get("WORKSPACE_ID", os.environ.get("fabric_workspace_id", ""))


_lakehouse_cache = None

def resolve_lakehouse_id(current_value: str, tier: str) -> str:
    """Return current_value if already set (pipeline-injected).
    Otherwise discover the lakehouse by name using the Fabric API.
    tier is one of: source, landing, bronze, silver, gold."""
    if current_value:
        return current_value

    global _lakehouse_cache
    if _lakehouse_cache is None:
        import requests
        ws_id = _get_workspace_id()
        token = notebookutils.credentials.getToken("pbi")
        resp = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        _lakehouse_cache = {lh["displayName"]: lh["id"] for lh in resp.json().get("value", [])}
        _cfg_logger.info("Discovered %d lakehouses in workspace", len(_lakehouse_cache))

    lh_names = cfg("lakehouse_names")
    lh_name = named(lh_names.get(tier, ""))
    if not lh_name:
        raise ValueError(f"No lakehouse name configured for tier '{tier}'")

    lh_id = _lakehouse_cache.get(lh_name)
    if not lh_id:
        raise ValueError(f"Lakehouse '{lh_name}' (tier={tier}) not found in workspace. "
                         f"Available: {list(_lakehouse_cache.keys())}")
    _cfg_logger.info("Resolved %s_lakehouse_id -> %s (%s)", tier, lh_id[:8], lh_name)
    return lh_id


def parse_list_param(value) -> list:
    if not value:
        return []
    if isinstance(value, list):
        return value
    value = str(value).strip()
    if value.startswith("["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    return [v.strip().strip("'\"") for v in value.split(",") if v.strip()]


def lakehouse_table_path(lakehouse_id: str, table_name: str) -> str:
    ws = _get_workspace_id()
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def read_lakehouse_table(spark_session, lakehouse_id: str, table_name: str):
    path = lakehouse_table_path(lakehouse_id, table_name)
    return spark_session.read.format("delta").load(path)


def write_lakehouse_table(df, lakehouse_id: str, table_name: str, mode: str = "overwrite"):
    """Write Delta to the explicit lakehouse path (Tables/) so it lands in the correct lakehouse.
    saveAsTable only targets the default lakehouse; path-based writes respect lakehouse_id."""
    path = lakehouse_table_path(lakehouse_id, table_name)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").option("mergeSchema", "true").save(path)
