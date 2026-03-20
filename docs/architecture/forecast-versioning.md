# Forecast Versioning & Layering Architecture

## Layering Flow

```mermaid
flowchart LR
    subgraph Layer1["Layer 1: Statistical Baseline"]
        A["SARIMA / Prophet / VAR / ETS<br>per grain"]
        A --> B["version_type = 'system'<br>FROZEN — never modified"]
    end

    subgraph Layer2["Layer 2: Sales Overrides"]
        C["Sales team inputs<br>additive deltas by SKU/Plant"]
        B --> D["system + override_delta<br>version_type = 'sales'"]
        C --> D
    end

    subgraph Layer3["Layer 3: Market Adjustments"]
        E["Market-level scaling<br>±X% by market"]
        D --> F["sales_forecast * scale_factor<br>version_type = 'market_adjusted'"]
        E --> F
    end

    subgraph Layer4["Layer 4: Consensus"]
        F --> G["Approved final forecast<br>version_type = 'consensus'"]
    end

    style B fill:#e8f5e9
    style D fill:#fff3e0
    style F fill:#e3f2fd
    style G fill:#f3e5f5
```

## Monthly Snapshot Process

```mermaid
sequenceDiagram
    participant Pipeline
    participant Models
    participant Silver
    participant Gold
    participant Semantic as Semantic Model
    participant Report as PBI Report

    Pipeline->>Models: Run statistical forecast (01-05)
    Models->>Silver: Write *_predictions tables (backtest)
    Models->>Gold: Write version_type='system', snapshot_month=YYYY-MM
    Note over Gold: Baseline is frozen

    Pipeline->>Gold: Apply sales overrides (08)
    Note over Gold: version_type='sales', parent=system_version_id

    Pipeline->>Gold: Apply market adjustments (09)
    Note over Gold: version_type='market_adjusted'

    Pipeline->>Gold: Build consensus (10)
    Note over Gold: version_type='consensus'

    Pipeline->>Gold: Run accuracy tracking (11)
    Note over Gold: Compare prior snapshots to actuals

    Pipeline->>Gold: Build reporting view + backtest (14)
    Note over Gold: Union *_predictions from silver into backtest_predictions

    Pipeline->>Semantic: Create/update DirectLake model (15)
    Pipeline->>Report: Create/update PBI report (16)
    Note over Report: Backtest: Actual vs Predicted
```

## Accuracy Tracking Logic

1. For each prior `snapshot_month` where `version_type = 'system'`:
   - Find periods where actuals are now available
   - Compute MAPE = mean(|forecast - actual| / actual) per grain
   - Compute bias = mean(forecast - actual) per grain
   - Store in `accuracy_tracking` table
2. Results aggregated by SKU group, plant, market for model selection
3. Best-performing model per grain informs ensemble weights or model selection

## Drill-Down Hierarchy

```
Market
  └── Plant
        └── SKU Group
              └── SKU
                    └── Customer
```

Each level shows: system forecast, sales adjusted, consensus, budget, variance, flags.
