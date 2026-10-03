# Databricks notebook source
"""Validate and standardize raw sales into silver.sales."""

from common.config import parse_business_keys, table_name, validate_runtime_config
from common.delta_merge import freshness_update_condition, merge_upsert
from common.runtime import configure_logging, log_metrics, widget_values
from common.spark_transforms import (
    RAW_REQUIRED_COLUMNS,
    deduplicate,
    split_silver_quality,
    standardize_silver,
    validate_required_columns,
)

logger = configure_logging("sales_silver")


def run() -> None:
    config = widget_values(
        dbutils,
        [
            "catalog_name",
            "raw_schema_name",
            "silver_schema_name",
            "sales_business_keys",
        ],
    )
    validate_runtime_config(config, set(config))
    business_keys = parse_business_keys(config["sales_business_keys"])
    source_table = table_name(config["catalog_name"], config["raw_schema_name"])
    target_table = table_name(config["catalog_name"], config["silver_schema_name"])

    if not spark.catalog.tableExists(source_table):
        log_metrics(
            logger,
            "NO_DATA",
            source_table=source_table,
            target_table=target_table,
            reason="raw table does not exist after a controlled no-file run",
            target_modified=False,
        )
        return

    raw_df = spark.read.table(source_table)
    source_rows = raw_df.count()
    if source_rows == 0:
        log_metrics(
            logger,
            "NO_DATA",
            source_table=source_table,
            target_table=target_table,
            source_rows=0,
            target_modified=False,
        )
        return
    standardized_df = standardize_silver(raw_df)
    validate_required_columns(
        standardized_df,
        RAW_REQUIRED_COLUMNS
        | set(business_keys)
        | {
            "_source_file",
            "_source_file_modified_at",
            "_record_hash",
            "_source_updated_at",
        },
    )
    accepted_df, rejected_df = split_silver_quality(standardized_df, business_keys)
    rejected_rows = rejected_df.count()
    update_column = (
        "_source_updated_at"
        if "_source_updated_at" in accepted_df.columns
        else "_ingested_at"
    )
    merge_df = deduplicate(accepted_df, business_keys, update_column)
    accepted_rows = merge_df.count()
    if accepted_rows == 0:
        log_metrics(
            logger,
            "NO_ACCEPTED_ROWS",
            source_table=source_table,
            target_table=target_table,
            source_rows=source_rows,
            accepted_rows=0,
            rejected_rows=rejected_rows,
            inserted_rows=0,
            updated_rows=0,
            target_modified=False,
        )
        return
    metrics = merge_upsert(
        spark,
        merge_df,
        target_table,
        business_keys,
        matched_update_condition=freshness_update_condition(),
    )
    log_metrics(
        logger,
        "SUCCESS",
        source_table=source_table,
        target_table=target_table,
        source_rows=source_rows,
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
        inserted_rows=metrics["inserted"],
        updated_rows=metrics["updated"],
    )


try:
    run()
except Exception:
    logger.exception("sales_silver failed during validation, transformation, or MERGE")
    raise
