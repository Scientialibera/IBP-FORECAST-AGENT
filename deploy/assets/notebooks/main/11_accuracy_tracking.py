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
# %run ../modules/schemas_module
# %run ../modules/feature_engineering_module
# %run ../modules/accuracy_module


gold_lakehouse_id = resolve_lakehouse_id(gold_lakehouse_id, "gold")
bronze_lakehouse_id = resolve_lakehouse_id(bronze_lakehouse_id, "bronze")

forecast_table = cfg("output_table")
accuracy_table = cfg("accuracy_table")
target_column = cfg("target_column")
grain_columns = cfg("grain_columns")
extended_grains = cfg("extended_grains")
source_tables = cfg("source_tables")

if not gold_lakehouse_id or not bronze_lakehouse_id:
    raise ValueError("gold_lakehouse_id and bronze_lakehouse_id are required.")

# Load all forecast versions
logger.info("[accuracy] Loading forecast versions from gold.")
forecast_spark = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
all_forecasts = forecast_spark.toPandas()

# Evaluate both system (unmodified baseline) and consensus (with overrides + market adj)
EVAL_VERSION_TYPES = ["system", "consensus"]

if all_forecasts.empty:
    logger.info("[accuracy] No forecasts to evaluate.")
else:
    primary_table = source_tables[0] if source_tables else cfg("primary_table")
    logger.info(f"[accuracy] Loading actuals from bronze.{primary_table}")
    actuals_spark = read_lakehouse_table(spark, bronze_lakehouse_id, primary_table)
    actuals_pdf = actuals_spark.toPandas()

    actuals_agg = aggregate_to_grain(
        actuals_pdf, cfg("date_column"), grain_columns, target_column, [], cfg("frequency")
    )
    actuals_agg["period"] = pd.to_datetime(actuals_agg["period"]).dt.strftime("%Y-%m-%d")

    all_accuracy_frames = []

    for vtype in EVAL_VERSION_TYPES:
        vtype_df = all_forecasts[all_forecasts["version_type"] == vtype].copy()
        if vtype_df.empty:
            logger.info(f"[accuracy] No '{vtype}' forecasts found -- skipping.")
            continue

        logger.info(f"[accuracy] Evaluating '{vtype}': {len(vtype_df)} rows, "
                    f"{vtype_df['snapshot_month'].nunique()} snapshots")

        vtype_df["period"] = pd.to_datetime(vtype_df["period"]).dt.strftime("%Y-%m-%d")

        fc_col = "forecast_tons"
        if vtype == "consensus" and "final_forecast_tons" in vtype_df.columns:
            vtype_df["forecast_tons"] = vtype_df["final_forecast_tons"]

        accuracy_df = evaluate_forecast_accuracy(
            vtype_df, actuals_agg,
            grain_columns=grain_columns,
            period_column="period",
            forecast_col="forecast_tons",
            actual_col=target_column,
        )

        if accuracy_df.empty:
            logger.info(f"[accuracy] No overlapping periods for '{vtype}'.")
            continue

        accuracy_df["version_type"] = vtype
        all_accuracy_frames.append(accuracy_df)
        logger.info(f"[accuracy] '{vtype}': {len(accuracy_df)} grain/version combinations evaluated")

    if all_accuracy_frames:
        combined_accuracy = pd.concat(all_accuracy_frames, ignore_index=True)
        acc_spark = spark.createDataFrame(combined_accuracy)
        write_lakehouse_table(acc_spark, gold_lakehouse_id, accuracy_table, mode="append")
        logger.info(f"[accuracy] Wrote {len(combined_accuracy)} accuracy rows to gold.{accuracy_table}")

        for level_col in ["plant_id", "market_id"]:
            if level_col in combined_accuracy.columns:
                level_agg = aggregate_accuracy_by_level(combined_accuracy, [level_col])
                if not level_agg.empty:
                    level_spark = spark.createDataFrame(level_agg)
                    write_lakehouse_table(level_spark, gold_lakehouse_id,
                                         f"accuracy_by_{level_col}", mode="overwrite")
                    logger.info(f"[accuracy] Wrote accuracy_by_{level_col}")

        system_only = combined_accuracy[combined_accuracy["version_type"] == "system"]
        if not system_only.empty:
            recommendations = recommend_model_by_grain(system_only, grain_columns, metric=cfg("accuracy_recommendation_metric"))
            if not recommendations.empty:
                rec_spark = spark.createDataFrame(recommendations)
                write_lakehouse_table(rec_spark, gold_lakehouse_id, cfg("model_recommendations_table"), mode="overwrite")
                logger.info(f"[accuracy] Model recommendations: {len(recommendations)} grains")

        logger.info("\n[accuracy] Overall by version_type × model:")
        for (vt, model), grp in combined_accuracy.groupby(["version_type", "model_type"]):
            avg_mape = grp["mape"].mean()
            avg_bias = grp["bias"].mean()
            logger.info(f"  {vt}/{model}: MAPE={avg_mape:.2f}%, Bias={avg_bias:.2f}")
    else:
        logger.info("[accuracy] No overlapping periods between any forecasts and actuals.")

logger.info("[accuracy] Complete.")
