# Fabric Notebook
# 11_accuracy_tracking.py -- Compare prior forecast snapshots to actuals
# Phase 1: Core Capability -- Objective accuracy measurement

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module
# %run ../modules/accuracy_module

forecast_table = cfg("output_table")
accuracy_table = cfg("accuracy_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
extended_grains = cfg("extended_grains")
source_tables = cfg("source_tables")

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

# Load prior system forecasts
print("[accuracy] Loading forecast versions from gold.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
system_forecasts = forecast_spark.filter(forecast_spark.version_type == "system").toPandas()

if system_forecasts.empty:
    print("[accuracy] No system forecasts to evaluate.")
else:
    print(f"[accuracy] {len(system_forecasts)} system forecast rows across {system_forecasts['snapshot_month'].nunique()} snapshots")

    # Load actuals from bronze (primary source table)
    primary_table = source_tables[0] if source_tables else "orders"
    print(f"[accuracy] Loading actuals from bronze.{primary_table}")
    actuals_spark = read_lakehouse_table(spark, bronze_lakehouse_id, primary_table)
    actuals_pdf = actuals_spark.toPandas()

    # Aggregate actuals to grain + period
    from feature_engineering_module import aggregate_to_grain
    actuals_agg = aggregate_to_grain(
        actuals_pdf, cfg("date_column"), grain_columns, target_column, [], "M"
    )

    # Evaluate accuracy
    accuracy_df = evaluate_forecast_accuracy(
        system_forecasts, actuals_agg,
        grain_columns=grain_columns,
        period_column="period",
        forecast_col="forecast_tons",
        actual_col=target_column,
    )

    if accuracy_df.empty:
        print("[accuracy] No overlapping periods between forecasts and actuals.")
    else:
        print(f"[accuracy] Evaluated {len(accuracy_df)} grain/version combinations")

        # Write accuracy results
        acc_spark = spark.createDataFrame(accuracy_df)
        write_lakehouse_table(acc_spark, gold_lakehouse_id, accuracy_table, mode="append")

        # Aggregate by hierarchy levels
        for level_col in ["plant_id", "market_id"]:
            if level_col in accuracy_df.columns:
                level_agg = aggregate_accuracy_by_level(accuracy_df, [level_col])
                if not level_agg.empty:
                    level_spark = spark.createDataFrame(level_agg)
                    write_lakehouse_table(level_spark, gold_lakehouse_id,
                                         f"accuracy_by_{level_col}", mode="overwrite")
                    print(f"[accuracy] Wrote accuracy_by_{level_col}")

        # Model recommendations per grain
        recommendations = recommend_model_by_grain(accuracy_df, grain_columns, metric="mape")
        if not recommendations.empty:
            rec_spark = spark.createDataFrame(recommendations)
            write_lakehouse_table(rec_spark, gold_lakehouse_id, "model_recommendations", mode="overwrite")
            print(f"[accuracy] Model recommendations: {len(recommendations)} grains")

        # Summary
        print("\n[accuracy] Overall by model:")
        for model, grp in accuracy_df.groupby("model_type"):
            avg_mape = grp["mape"].mean()
            avg_bias = grp["bias"].mean()
            print(f"  {model}: MAPE={avg_mape:.2f}%, Bias={avg_bias:.2f}")

print("[accuracy] Complete.")
