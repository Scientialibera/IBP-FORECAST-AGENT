# Fabric notebook source
# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# METADATA ********************

# CELL ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 11_accuracy_tracking.py -- Compare prior forecast snapshots to actuals
# Phase 1: Core Capability -- Objective accuracy measurement

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run config_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run accuracy_module

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params = get_notebook_params()

gold_lakehouse_id = params["gold_lakehouse_id"]
bronze_lakehouse_id = params["bronze_lakehouse_id"]
forecast_table = params.get("output_table") or "forecast_versions"
accuracy_table = params.get("accuracy_table") or "accuracy_tracking"
target_column = params["target_column"]
grain_columns = parse_list_param(params["grain_columns"])
extended_grains = parse_list_param(params.get("extended_grains") or "")
source_tables = parse_list_param(params["source_tables"])

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
        actuals_pdf, params["date_column"], grain_columns, target_column, [], "M"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
