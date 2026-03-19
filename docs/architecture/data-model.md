# Data Model -- Forecast Versioning & Layering

## Core Principle

Every forecast row carries a `version_id`, `version_type`, and `snapshot_month`. Statistical baselines are **never overwritten**. Sales overrides and market adjustments are separate, auditable layers.

## Forecast Table Schema (`forecast_versions`)

```mermaid
erDiagram
    forecast_versions {
        string version_id PK "UUID per forecast run"
        string snapshot_month "YYYY-MM (monthly snapshot)"
        string version_type "system | sales | market_adjusted | consensus"
        string model_type "sarima | prophet | var | exp_smoothing | ensemble | override"
        string sku_id FK
        string sku_group
        string plant_id FK
        string customer_id FK
        string market_id FK
        string period "YYYY-MM forecast period"
        float forecast_tons "Forecasted volume in tons"
        float forecast_lineal_feet "Translated to lineal feet"
        float forecast_prod_hours "Translated to production hours"
        float override_delta_tons "Sales override delta (additive)"
        float market_scale_factor "Market adjustment multiplier (e.g. 1.10)"
        float final_forecast_tons "Final adjusted forecast"
        string created_by "system | user_email"
        timestamp created_at
        string parent_version_id "Links to baseline version"
    }
```

## Version Types

| Type | Source | Overwrites Baseline? | Description |
|------|--------|---------------------|-------------|
| `system` | Statistical models | N/A (IS the baseline) | Raw model output, frozen at creation |
| `sales` | Sales team input | No | Additive delta on top of `system` |
| `market_adjusted` | Market controls | No | Multiplicative scaling on `system` or `sales` |
| `consensus` | Finalization | No | Approved version = `system + sales_delta * market_factor` |

## Accuracy Tracking Schema (`accuracy_tracking`)

```mermaid
erDiagram
    accuracy_tracking {
        string tracking_id PK
        string version_id FK "Which forecast version"
        string snapshot_month "When forecast was made"
        string actual_month "Which month's actuals we're comparing"
        string sku_group
        string plant_id
        string market_id
        float forecast_tons
        float actual_tons
        float mape
        float bias
        float rmse
        string model_type
        timestamp evaluated_at
    }
```

## Capacity Translation Schema (`capacity_translation`)

```mermaid
erDiagram
    capacity_translation {
        string plant_id FK
        string line_id
        string sku_id FK
        string period "YYYY-MM"
        float forecast_tons
        float avg_width_inches "3-month rolling avg"
        float avg_line_speed_fpm "3-month rolling avg"
        float lineal_feet "tons * conversion / width"
        float production_hours "lineal_feet / line_speed / 60"
        float min_width "Min width in rolling window"
        float max_width "Max width in rolling window"
        float min_speed "Min speed in rolling window"
        float max_speed "Max speed in rolling window"
    }
```

## Budget Comparison Schema (`budget_comparison`)

```mermaid
erDiagram
    budget_comparison {
        string period "YYYY-MM"
        string market_id
        string plant_id
        string sku_group
        float budget_tons
        float forecast_tons "From consensus"
        float variance_tons
        float variance_pct
        string flag "over_forecast | under_forecast | on_track"
        float threshold_pct "Configured flag threshold"
    }
```
