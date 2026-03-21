# Fabric Notebook -- Module
# feature_engineering_module.py -- Feature table construction for IBP forecasting

import pandas as pd
import numpy as np


def aggregate_to_grain(df: pd.DataFrame, date_column: str, grain_columns: list,
                       target_column: str, feature_columns: list, frequency: str = None) -> pd.DataFrame:
    """Aggregate raw data to grain + period level."""
    frequency = frequency or cfg("frequency")
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    df["period"] = df[date_column].dt.to_period(frequency).dt.to_timestamp()

    group_cols = grain_columns + ["period"]

    agg_dict = {target_column: "sum"}
    for fc in feature_columns:
        if fc in df.columns:
            if df[fc].dtype in ["float64", "int64", "float32", "int32"]:
                agg_dict[fc] = "mean"

    result = df.groupby(group_cols, as_index=False).agg(agg_dict)
    result = result.sort_values(group_cols).reset_index(drop=True)
    return result


def add_lag_features(df: pd.DataFrame, grain_columns: list,
                     target_column: str, lag_periods=None) -> pd.DataFrame:
    """Add lag features per grain."""
    lags = lag_periods or freq_params("default_lags")
    df = df.sort_values(grain_columns + ["period"]).copy()
    for lag in lags:
        df[f"{target_column}_lag_{lag}"] = df.groupby(grain_columns)[target_column].shift(lag)
    return df


def add_rolling_features(df: pd.DataFrame, grain_columns: list,
                         target_column: str, rolling_windows=None) -> pd.DataFrame:
    """Add rolling mean and std features per grain."""
    windows = rolling_windows or freq_params("default_rolling")
    df = df.sort_values(grain_columns + ["period"]).copy()
    for w in windows:
        df[f"{target_column}_roll_mean_{w}"] = (
            df.groupby(grain_columns)[target_column]
            .transform(lambda x: x.rolling(w, min_periods=1).mean())
        )
        df[f"{target_column}_roll_std_{w}"] = (
            df.groupby(grain_columns)[target_column]
            .transform(lambda x: x.rolling(w, min_periods=1).std())
        )
    return df


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar and cyclical features appropriate for the configured frequency."""
    df = df.copy()
    freq = cfg("frequency")
    ppy = freq_params("periods_per_year")
    df["year"] = df["period"].dt.year
    if freq == "D":
        df["month"] = df["period"].dt.dayofyear
        df["quarter"] = df["period"].dt.quarter
    elif freq == "W":
        df["month"] = df["period"].dt.isocalendar().week.astype(int)
        df["quarter"] = df["period"].dt.quarter
    else:
        df["month"] = df["period"].dt.month
        df["quarter"] = df["period"].dt.quarter
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / ppy)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / ppy)
    return df


def build_feature_table(df: pd.DataFrame, date_column: str, grain_columns: list,
                        target_column: str, feature_columns: list,
                        frequency: str = None, lag_periods=None,
                        rolling_windows=None) -> pd.DataFrame:
    """Full feature engineering pipeline."""
    frequency = frequency or cfg("frequency")
    result = aggregate_to_grain(df, date_column, grain_columns, target_column, feature_columns, frequency)
    result = add_lag_features(result, grain_columns, target_column, lag_periods)
    result = add_rolling_features(result, grain_columns, target_column, rolling_windows)
    result = add_calendar_features(result)
    return result
