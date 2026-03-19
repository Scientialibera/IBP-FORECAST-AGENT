# Fabric Notebook -- Module
# config_module.py -- Config loader, lakehouse I/O, parameter parsing

import os
import json


def get_notebook_params() -> dict:
    """Read all notebook parameters from environment variables."""
    params = {}
    for key, value in os.environ.items():
        params[key.lower()] = value
    return params


def parse_list_param(value) -> list:
    """Parse a JSON list or comma-separated string."""
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


def parse_int_list_param(value) -> list:
    """Parse a list of integers."""
    items = parse_list_param(value)
    return [int(x) for x in items]


def lakehouse_table_path(lakehouse_id: str, table_name: str) -> str:
    """Build abfss path for a lakehouse delta table."""
    workspace_id = os.environ.get("WORKSPACE_ID", os.environ.get("fabric_workspace_id", ""))
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def read_lakehouse_table(spark, lakehouse_id: str, table_name: str):
    """Read a delta table from a lakehouse."""
    path = lakehouse_table_path(lakehouse_id, table_name)
    return spark.read.format("delta").load(path)


def write_lakehouse_table(df, lakehouse_id: str, table_name: str, mode: str = "overwrite"):
    """Write a dataframe as delta to a lakehouse."""
    path = lakehouse_table_path(lakehouse_id, table_name)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
