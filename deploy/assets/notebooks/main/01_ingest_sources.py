# Fabric Notebook
# 01_ingest_sources.py -- Ingest source tables into Landing lakehouse
# Phase 1: Core Capability

# %run ../modules/ibp_config
# %run ../modules/config_module


source_lakehouse_id = resolve_lakehouse_id("", "source")
landing_lakehouse_id = resolve_lakehouse_id("", "landing")

source_tables = cfg("source_tables")

if not source_tables:
    raise ValueError("source_tables list is empty.")

logger.info(f"[ingest] Source lakehouse: {source_lakehouse_id}")
logger.info(f"[ingest] Landing lakehouse: {landing_lakehouse_id}")
logger.info(f"[ingest] Tables to ingest: {source_tables}")

for table_name in source_tables:
    logger.info(f"\n[ingest] Reading: {table_name}")
    df = read_lakehouse_table(spark, source_lakehouse_id, table_name)
    row_count = df.count()
    logger.info(f"[ingest] {table_name}: {row_count} rows")
    write_lakehouse_table(df, landing_lakehouse_id, table_name, mode="overwrite")
    logger.info(f"[ingest] Wrote {table_name} to landing.")

logger.info("\n[ingest] Complete.")
