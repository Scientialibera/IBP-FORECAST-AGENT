# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# config_module.py -- Config loader, lakehouse I/O, parameter parsing

# CELL ********************

import os
import sys
import json


def get_notebook_params() -> dict:
    """
    Collect notebook parameters from all available sources.
    Priority: notebook globals > mssparkutils > environment variables.
    In Fabric, API-triggered parameters are injected as globals.
    """
    params = {}

    for key, value in os.environ.items():
        params[key.lower()] = str(value)

    try:
        import notebookutils
        for key in ["source_lakehouse_id", "landing_lakehouse_id", "bronze_lakehouse_id",
                     "silver_lakehouse_id", "gold_lakehouse_id", "source_tables",
                     "n_skus", "n_plants", "n_customers", "n_markets",
                     "n_production_lines", "history_months", "seed",
                     "forecast_horizon", "test_split_ratio", "min_series_length",
                     "grain_columns", "extended_grains", "target_column",
                     "date_column", "frequency", "feature_columns",
                     "sarima_order", "sarima_seasonal_order",
                     "prophet_yearly_seasonality", "prophet_weekly_seasonality",
                     "prophet_changepoint_prior", "var_maxlags", "var_ic",
                     "exp_smoothing_trend", "exp_smoothing_seasonal",
                     "exp_smoothing_seasonal_periods", "rolling_months",
                     "over_forecast_threshold", "under_forecast_threshold",
                     "hierarchy_levels", "keep_n_snapshots"]:
            try:
                val = notebookutils.notebook.getParameter(key)
                if val is not None:
                    params[key] = str(val)
            except Exception:
                pass
    except ImportError:
        pass

    main_mod = sys.modules.get("__main__")
    if main_mod:
        for attr in dir(main_mod):
            if not attr.startswith("_"):
                val = getattr(main_mod, attr, None)
                if isinstance(val, (str, int, float, bool)):
                    params[attr.lower()] = str(val)

    return params


def _get_workspace_id() -> str:
    """Get workspace ID from Spark config or environment."""
    try:
        return spark.conf.get("trident.workspace.id")
    except Exception:
        pass
    return os.environ.get("WORKSPACE_ID", os.environ.get("fabric_workspace_id", ""))


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
    ws = _get_workspace_id()
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"


def read_lakehouse_table(spark_session, lakehouse_id: str, table_name: str):
    """Read a delta table from a lakehouse."""
    path = lakehouse_table_path(lakehouse_id, table_name)
    return spark_session.read.format("delta").load(path)


def write_lakehouse_table(df, lakehouse_id: str, table_name: str, mode: str = "overwrite"):
    """Write a dataframe as delta to a lakehouse."""
    path = lakehouse_table_path(lakehouse_id, table_name)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
