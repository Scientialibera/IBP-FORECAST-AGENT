# Fabric Notebook -- Module
# override_module.py -- Sales overrides and market adjustments as transparent layers

import pandas as pd
import numpy as np


def apply_sales_overrides(baseline_df: pd.DataFrame,
                          overrides_df: pd.DataFrame,
                          grain_columns: list,
                          period_column: str = "period") -> pd.DataFrame:
    """
    Apply additive sales overrides on top of statistical baseline.
    Baseline forecast_tons is preserved. Override delta is stored separately.
    Final = forecast_tons + override_delta_tons
    """
    merge_keys = grain_columns + [period_column]

    result = baseline_df.copy()
    result["override_delta_tons"] = 0.0

    if overrides_df is not None and not overrides_df.empty:
        override_cols = merge_keys + ["override_delta_tons"]
        override_sub = overrides_df[
            [c for c in override_cols if c in overrides_df.columns]
        ].copy()

        if "override_delta_tons" not in override_sub.columns:
            if "override_tons" in overrides_df.columns:
                override_sub["override_delta_tons"] = overrides_df["override_tons"]
            else:
                override_sub["override_delta_tons"] = 0.0

        result = result.merge(override_sub, on=merge_keys, how="left",
                              suffixes=("", "_override"))
        if "override_delta_tons_override" in result.columns:
            result["override_delta_tons"] = result["override_delta_tons_override"].fillna(0.0)
            result.drop(columns=["override_delta_tons_override"], inplace=True)

    result["final_forecast_tons"] = result["forecast_tons"] + result["override_delta_tons"]
    return result


def apply_market_adjustments(forecast_df: pd.DataFrame,
                             adjustments_df: pd.DataFrame,
                             market_column: str = "market_id",
                             period_column: str = "period",
                             default_factor: float = 1.0) -> pd.DataFrame:
    """
    Apply multiplicative market-level scaling factors.
    scale_factor is stored transparently and can be removed without altering baseline.
    """
    result = forecast_df.copy()
    result["market_scale_factor"] = default_factor

    if adjustments_df is not None and not adjustments_df.empty:
        adj_cols = [market_column, period_column, "scale_factor"]
        adj_sub = adjustments_df[[c for c in adj_cols if c in adjustments_df.columns]].copy()

        if "scale_factor" in adj_sub.columns:
            result = result.merge(adj_sub, on=[c for c in [market_column, period_column]
                                               if c in adj_sub.columns],
                                  how="left", suffixes=("", "_adj"))
            if "scale_factor" in result.columns:
                result["market_scale_factor"] = result["scale_factor"].fillna(default_factor)
                result.drop(columns=["scale_factor"], inplace=True, errors="ignore")

    base = result.get("final_forecast_tons", result.get("forecast_tons", 0))
    result["final_forecast_tons"] = base * result["market_scale_factor"]
    return result


def build_consensus(system_df: pd.DataFrame, sales_df: pd.DataFrame,
                    market_df: pd.DataFrame, grain_columns: list,
                    period_column: str = "period") -> pd.DataFrame:
    """
    Build consensus forecast = system baseline + sales delta * market factor.
    All three layers are preserved as separate version_type rows.
    This function returns the final consensus values.
    """
    merge_keys = grain_columns + [period_column]

    consensus = system_df[merge_keys + ["forecast_tons"]].copy()
    consensus.rename(columns={"forecast_tons": "system_forecast"}, inplace=True)

    if sales_df is not None and not sales_df.empty:
        sales_sub = sales_df[merge_keys + ["override_delta_tons"]].copy()
        consensus = consensus.merge(sales_sub, on=merge_keys, how="left")
        consensus["override_delta_tons"] = consensus["override_delta_tons"].fillna(0.0)
    else:
        consensus["override_delta_tons"] = 0.0

    if market_df is not None and not market_df.empty:
        market_sub = market_df[merge_keys + ["market_scale_factor"]].drop_duplicates()
        consensus = consensus.merge(market_sub, on=merge_keys, how="left")
        consensus["market_scale_factor"] = consensus["market_scale_factor"].fillna(1.0)
    else:
        consensus["market_scale_factor"] = 1.0

    consensus["forecast_tons"] = (
        (consensus["system_forecast"] + consensus["override_delta_tons"])
        * consensus["market_scale_factor"]
    )
    consensus["final_forecast_tons"] = consensus["forecast_tons"]
    consensus.drop(columns=["system_forecast"], inplace=True)

    return consensus
