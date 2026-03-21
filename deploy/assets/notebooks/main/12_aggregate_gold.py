# Fabric Notebook
# 12_aggregate_gold.py -- Hierarchical roll-ups for drill-down views
# Phase 1: Core Capability

# %run ../modules/ibp_config
# %run ../modules/config_module


gold_lakehouse_id = resolve_lakehouse_id("", "gold")

from pyspark.sql import functions as F

forecast_table = cfg("output_table")
hierarchy_levels = cfg("hierarchy_levels")


logger.info("[aggregate] Loading forecast versions from gold.")
forecast_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table)
total = forecast_df.count()
logger.info(f"[aggregate] Total rows: {total}")

if total == 0:
    logger.info("[aggregate] No data. Exiting.")
else:
    # Aggregate per version_type at each hierarchy level
    for version_type in cfg("version_types"):
        vtype_df = forecast_df.filter(F.col("version_type") == version_type)
        if vtype_df.count() == 0:
            continue

        # Get latest version only
        from pyspark.sql.window import Window
        w = Window.partitionBy("version_type").orderBy(F.desc("created_at"))
        vtype_df = vtype_df.withColumn("_rn", F.row_number().over(w))
        latest_vid = vtype_df.filter(F.col("_rn") == 1).select("version_id").first()
        if latest_vid:
            vtype_df = vtype_df.filter(F.col("version_id") == latest_vid["version_id"])

        for level in range(len(hierarchy_levels)):
            group_cols = hierarchy_levels[:level + 1]
            valid_cols = [c for c in group_cols if c in vtype_df.columns]
            if not valid_cols:
                continue

            agg_df = vtype_df.groupBy(valid_cols + ["period"]).agg(
                F.sum("forecast_tons").alias("total_forecast_tons"),
                F.sum("final_forecast_tons").alias("total_final_forecast_tons"),
                F.count("*").alias("n_rows"),
                F.avg("forecast_tons").alias("avg_forecast_tons"),
            )
            agg_df = agg_df.withColumn("version_type", F.lit(version_type))
            agg_df = agg_df.withColumn("aggregation_level", F.lit("|".join(valid_cols)))
            agg_df = agg_df.withColumn("aggregated_at", F.current_timestamp())

            table_name = f"agg_{version_type}_by_{'_'.join(valid_cols)}"
            write_lakehouse_table(agg_df, gold_lakehouse_id, table_name, mode="overwrite")
            logger.info(f"[aggregate] {table_name}: {agg_df.count()} rows")

    # Grand total
    grand = forecast_df.groupBy("version_type", "model_type").agg(
        F.sum("forecast_tons").alias("total_forecast_tons"),
        F.sum("final_forecast_tons").alias("total_final_tons"),
        F.count("*").alias("n_rows"),
    ).withColumn("aggregated_at", F.current_timestamp())
    write_lakehouse_table(grand, gold_lakehouse_id, cfg("agg_grand_total_table"), mode="overwrite")
    logger.info(f"[aggregate] {cfg('agg_grand_total_table')}: {grand.count()} rows")

logger.info("[aggregate] Complete.")
