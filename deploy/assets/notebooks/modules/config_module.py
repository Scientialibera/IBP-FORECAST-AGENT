# Fabric Notebook -- Module
# config_module.py -- Lakehouse I/O utilities
# Lakehouse IDs come from parameter cells; all other config from ibp_config.

import os
import json


def _get_workspace_id() -> str:
    try:
        return spark.conf.get("trident.workspace.id")
    except Exception:
        pass
    return os.environ.get("WORKSPACE_ID", os.environ.get("fabric_workspace_id", ""))


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
