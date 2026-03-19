# System Overview

```mermaid
flowchart TB
    subgraph Sources["Source Data"]
        orders["Orders / Shipments"]
        production["Production History"]
        master["Master Data<br>(SKU, Plant, Customer, Market)"]
        budget["Budget Volumes"]
        inventory["FG Inventory"]
        external["External Signals<br>(Construction, Rates, Tariffs)"]
    end

    subgraph Medallion["Fabric Lakehouse Medallion"]
        subgraph Landing["Landing"]
            raw_tables["Raw Source Tables"]
        end
        subgraph Bronze["Bronze"]
            clean_tables["Cleansed + Deduped"]
        end
        subgraph Silver["Silver"]
            features["Feature Table"]
            prod_metrics["Production Metrics<br>(width, line speed)"]
            sku_class["SKU Classifications"]
        end
        subgraph Gold["Gold"]
            forecasts["Versioned Forecasts<br>(system / sales / consensus)"]
            snapshots["Monthly Snapshots"]
            capacity["Capacity Translation<br>(tons → LF → hours)"]
            accuracy["Accuracy Tracking<br>(MAPE, bias)"]
            hierarchy["Hierarchical Rollups<br>(Market → Plant → SKU → Customer)"]
            budget_comp["Budget Comparison + Flags"]
            scenarios["Scenario Comparisons"]
            inv_alignment["Inventory Alignment"]
        end
    end

    subgraph Models["Statistical Models"]
        sarima["SARIMA"]
        prophet["Prophet"]
        var["VAR"]
        ets["Exp Smoothing"]
    end

    subgraph Layers["Forecast Layering"]
        baseline["Statistical Baseline<br>(never overwritten)"]
        sales_adj["Sales Override Layer"]
        market_adj["Market Adjustment Layer<br>(±X% scaling)"]
        consensus["Consensus Forecast"]
    end

    Sources --> Landing
    Landing --> Bronze
    Bronze --> Silver
    Silver --> Models
    Models --> baseline
    baseline --> sales_adj
    sales_adj --> market_adj
    market_adj --> consensus
    consensus --> Gold
    Silver --> Gold
    production --> prod_metrics
    prod_metrics --> capacity
```
