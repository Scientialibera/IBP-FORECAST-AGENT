# Fabric Notebook -- Module
# capacity_module.py -- Demand-to-capacity translation: tons -> lineal feet -> production hours

import pandas as pd
import numpy as np


def compute_rolling_production_averages(production_df: pd.DataFrame,
                                        width_column: str, speed_column: str,
                                        line_id_column: str, plant_column: str,
                                        sku_column: str, date_column: str,
                                        rolling_months: int = 3) -> pd.DataFrame:
    """Compute rolling 3-month averages of width and line speed per plant/sku/line."""
    df = production_df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.sort_values([plant_column, sku_column, line_id_column, date_column])

    group_cols = [plant_column, sku_column, line_id_column]

    df["avg_width"] = df.groupby(group_cols)[width_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).mean()
    )
    df["avg_speed"] = df.groupby(group_cols)[speed_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).mean()
    )
    df["min_width"] = df.groupby(group_cols)[width_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).min()
    )
    df["max_width"] = df.groupby(group_cols)[width_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).max()
    )
    df["min_speed"] = df.groupby(group_cols)[speed_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).min()
    )
    df["max_speed"] = df.groupby(group_cols)[speed_column].transform(
        lambda x: x.rolling(rolling_months, min_periods=1).max()
    )

    latest = df.groupby(group_cols).tail(1).reset_index(drop=True)
    return latest[group_cols + ["avg_width", "avg_speed", "min_width", "max_width",
                                 "min_speed", "max_speed"]]


def translate_demand_to_capacity(forecast_df: pd.DataFrame,
                                 production_avgs: pd.DataFrame,
                                 plant_column: str, sku_column: str,
                                 line_id_column: str,
                                 tons_to_lf_factor: float = 2000) -> pd.DataFrame:
    """
    Convert forecasted tons to lineal feet and production hours.

    Lineal feet = (forecast_tons * tons_to_lf_factor * 12) / avg_width_inches
    Production hours = lineal_feet / (avg_speed_fpm * 60)
    """
    merged = forecast_df.merge(
        production_avgs,
        on=[plant_column, sku_column],
        how="left",
    )

    merged["lineal_feet"] = np.where(
        merged["avg_width"] > 0,
        (merged["forecast_tons"] * tons_to_lf_factor * 12) / merged["avg_width"],
        np.nan,
    )

    merged["production_hours"] = np.where(
        merged["avg_speed"] > 0,
        merged["lineal_feet"] / (merged["avg_speed"] * 60),
        np.nan,
    )

    return merged
