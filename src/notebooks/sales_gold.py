# Databricks notebook source
"""Incrementally publish product/day sales aggregates to gold.sales."""

from common.config import table_name, validate_runtime_config
from common.delta_merge import changed_columns_condition, merge_upsert
from common.runtime import configure_logging, log_metrics, widget_values
from common.spark_transforms import build_gold, validate_required_columns

logger = configure_logging("sales_gold")


def run() -> None:
    config = widget_values(
        dbutils, ["catalog_name", "silver_schema_name", "gold_schema_name"]
    )
    validate_runtime_config(config, set(config))
    source_table = table_name(config["catalog_name"], config["silver_schema_name"])
    target_table = table_name(config["catalog_name"], config["gold_schema_name"])

    if not spark.catalog.tableExists(source_table):
        log_metrics(
            logger,
            "NO_DATA",
            source_table=source_table,
            target_table=target_table,
            reason="silver table does not exist after a controlled no-data run",
            target_modified=False,
        )
        return

    silver_df = spark.read.table(source_table)
    validate_required_columns(
        silver_df,
        {
            "product_id",
            "quantity",
            "unit_price",
            "sale_timestamp",
            "_ingested_at",
            "_source_updated_at",
        },
    )
    source_rows = silver_df.count()
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
    gold_df = build_gold(silver_df)
    aggregate_rows = gold_df.count()

    # Recomputing current aggregates and MERGEing them avoids a full-table overwrite
    # and incorporates late raw/silver updates. Only aggregates whose values changed
    # are updated, so an unchanged replay leaves existing gold rows untouched.
    metrics = merge_upsert(
        spark,
        gold_df,
        target_table,
        ["sale_date", "product_id"],
        delete_not_matched=True,
        matched_update_condition=changed_columns_condition(
            ["record_count", "total_quantity", "gross_sales", "_business_updated_at"]
        ),
    )
    log_metrics(
        logger,
        "SUCCESS",
        source_table=source_table,
        target_table=target_table,
        source_rows=source_rows,
        aggregate_rows=aggregate_rows,
        inserted_rows=metrics["inserted"],
        updated_rows=metrics["updated"],
        deleted_rows=metrics["deleted"],
    )


try:
    run()
except Exception:
    logger.exception("sales_gold failed during aggregation or MERGE")
    raise
