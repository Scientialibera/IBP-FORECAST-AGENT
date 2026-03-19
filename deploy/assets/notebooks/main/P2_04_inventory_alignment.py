# Fabric Notebook
# P2_04_inventory_alignment.py

# @parameters
gold_lakehouse_id = ""
bronze_lakehouse_id = ""
# @end_parameters

# %run ../modules/ibp_config
# %run ../modules/config_module
# %run ../modules/utils_module

forecast_table = cfg("output_table")
grain_columns = cfg("grain_columns")

print("[inventory] Aligning forecast with inventory positions.")
fc_df = read_lakehouse_table(spark, gold_lakehouse_id, forecast_table).toPandas()
inv_df = read_lakehouse_table(spark, bronze_lakehouse_id, "inventory_finished_goods").toPandas()

common_cols = [c for c in grain_columns if c in fc_df.columns and c in inv_df.columns]
if common_cols:
    inv_latest = inv_df.sort_values("period_date").groupby(common_cols).last().reset_index()
    aligned = fc_df.merge(inv_latest[common_cols + ["on_hand_tons", "safety_stock_tons"]],
                          on=common_cols, how="left")
    if "tons" in aligned.columns and "on_hand_tons" in aligned.columns:
        aligned["net_requirement"] = aligned["tons"] - aligned["on_hand_tons"].fillna(0)
        aligned["stock_coverage_flag"] = (aligned["on_hand_tons"].fillna(0) >= aligned["safety_stock_tons"].fillna(0)).astype(int)

    write_lakehouse_table(spark.createDataFrame(aligned), gold_lakehouse_id, "inventory_aligned_forecast", mode="overwrite")
    print(f"[inventory] {len(aligned)} aligned rows")
else:
    print("[inventory] WARNING: No common grain columns for merge")
print("[inventory] Complete.")
