# Databricks notebook source
"""Ingest CSV or Parquet sales files from governed cloud storage into raw.sales."""

from common.config import (
    parse_business_keys,
    parse_csv_column_names,
    resolve_source_path,
    table_name,
    validate_csv_options,
    validate_input_format,
    validate_runtime_config,
)
from common.delta_merge import freshness_update_condition, merge_upsert
from common.runtime import configure_logging, log_metrics, widget_values
from common.spark_transforms import (
    RAW_REQUIRED_COLUMNS,
    add_raw_metadata,
    add_raw_merge_key,
    add_source_file_metadata,
    add_source_update_timestamp,
    deduplicate,
    rename_data_columns,
    validate_required_columns,
)

logger = configure_logging("sales_raw")


def run() -> None:
    config = widget_values(
        dbutils,
        [
            "catalog_name",
            "raw_schema_name",
            "source_path",
            "external_source_path",
            "input_source_type",
            "input_format",
            "csv_header",
            "csv_delimiter",
            "csv_column_names",
            "sales_business_keys",
        ],
    )
    validate_runtime_config(config, set(config))
    business_keys = parse_business_keys(config["sales_business_keys"])
    target_table = table_name(config["catalog_name"], config["raw_schema_name"])
    input_format = validate_input_format(config["input_format"])
    input_path = resolve_source_path(
        config["input_source_type"],
        config["source_path"],
        config["external_source_path"],
    )
    csv_header, csv_delimiter = validate_csv_options(
        config["csv_header"], config["csv_delimiter"]
    )
    csv_column_names = parse_csv_column_names(config["csv_column_names"])
    file_glob = (
        "*.[cC][sS][vV]"
        if input_format == "csv"
        else "*.[pP][aA][rR][qQ][uU][eE][tT]"
    )

    # binaryFile metadata discovery keeps no-file handling separate from data
    # decoding. Column pruning avoids materializing file content on the driver.
    eligible_files = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", file_glob)
        .load(input_path)
        .select("path")
    )
    eligible_count = eligible_files.count()
    if eligible_count == 0:
        log_metrics(
            logger,
            "NO_DATA",
            eligible_files=0,
            input_source_type=config["input_source_type"],
            input_format=input_format,
            target_table=target_table,
            target_modified=False,
        )
        return

    reader = (
        spark.read.format(input_format)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", file_glob)
    )
    if input_format == "csv":
        # Keep raw CSV values as strings; silver applies the governed type casts.
        reader = (
            reader.option("header", csv_header)
            .option("delimiter", csv_delimiter)
            .option("mode", "FAILFAST")
            .option("inferSchema", "false")
            # Fail when a file's header disagrees with the first file's header
            # instead of silently assigning values to the wrong columns.
            .option("enforceSchema", "false")
        )
    source_df = add_source_file_metadata(reader.load(input_path))
    if input_format == "csv" and csv_header == "false":
        source_df = rename_data_columns(source_df, csv_column_names)
    validate_required_columns(source_df, RAW_REQUIRED_COLUMNS | set(business_keys))
    source_rows = source_df.count()
    enriched_df = add_raw_metadata(source_df)
    enriched_df = add_source_update_timestamp(enriched_df)
    enriched_df = add_raw_merge_key(enriched_df, business_keys)
    merge_df = deduplicate(enriched_df, ["_raw_merge_key"], "_source_updated_at")
    deduplicated_rows = merge_df.count()
    metrics = merge_upsert(
        spark,
        merge_df,
        target_table,
        ["_raw_merge_key"],
        matched_update_condition=freshness_update_condition(),
    )
    log_metrics(
        logger,
        "SUCCESS",
        eligible_files=eligible_count,
        input_source_type=config["input_source_type"],
        input_format=input_format,
        source_rows=source_rows,
        deduplicated_rows=deduplicated_rows,
        target_table=target_table,
        **metrics,
    )


try:
    run()
except Exception:
    logger.exception("sales_raw failed: invalid schema, unreadable input, or MERGE failure")
    raise
