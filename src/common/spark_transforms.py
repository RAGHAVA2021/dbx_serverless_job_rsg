"""Serverless-compatible Spark DataFrame transformations."""

from __future__ import annotations

from collections.abc import Sequence

from common.logic import normalize_column_name


RAW_REQUIRED_COLUMNS = {
    "product_id",
    "quantity",
    "unit_price",
    "sale_timestamp",
}
RAW_FILE_METADATA_COLUMNS = ("_source_file", "_source_file_modified_at")


def validate_required_columns(df, required: set[str]) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def add_source_file_metadata(df):
    """Capture file path and modification time directly after the file read.

    Serverless and Unity Catalog compute do not support input_file_name(); the hidden
    _metadata column is only resolvable on the reader output, so call this first.
    """
    from pyspark.sql import functions as F

    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
    )


def rename_data_columns(df, names: Sequence[str]):
    """Apply ordered names to headerless data columns, keeping file metadata columns."""
    from pyspark.sql import functions as F

    data_columns = [name for name in df.columns if name not in RAW_FILE_METADATA_COLUMNS]
    if len(data_columns) != len(names):
        raise ValueError(
            "Headerless CSV column count does not match csv_column_names: "
            f"found {len(data_columns)}, configured {len(names)}"
        )
    return df.select(
        *[F.col(f"`{old}`").alias(new) for old, new in zip(data_columns, names)],
        *RAW_FILE_METADATA_COLUMNS,
    )


def add_raw_metadata(df):
    from pyspark.sql import functions as F

    # File metadata is excluded so the same record re-delivered in another file or
    # re-uploaded file hashes identically and replays stay no-op MERGEs.
    source_columns = sorted(
        name for name in df.columns if name not in RAW_FILE_METADATA_COLUMNS
    )
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_record_hash",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in source_columns])), 256),
    )


def add_source_update_timestamp(df):
    """Parse and validate an optional source update timestamp."""
    from pyspark.sql import functions as F

    if "updated_at" not in df.columns:
        return df.withColumn("_source_updated_at", F.lit(None).cast("timestamp"))

    provided = F.col("updated_at").isNotNull() & (
        F.trim(F.col("updated_at").cast("string")) != ""
    )
    parsed = F.try_to_timestamp(F.col("updated_at"))
    invalid = provided & parsed.isNull()
    if df.filter(invalid).limit(1).count():
        raise ValueError("updated_at contains values that cannot be parsed as timestamps")
    return df.withColumn("_source_updated_at", parsed)


def add_raw_merge_key(df, keys: Sequence[str]):
    """Create a stable key without collapsing null or blank business-key rows."""
    from functools import reduce
    from operator import and_

    from pyspark.sql import functions as F

    valid_key = reduce(
        and_,
        [
            F.col(key).isNotNull()
            & (F.trim(F.col(key).cast("string")) != "")
            for key in keys
        ],
    )
    business_payload = F.to_json(
        F.struct(*[F.col(key).alias(key) for key in keys]),
        options={"ignoreNullFields": "false"},
    )
    return df.withColumn(
        "_raw_merge_key",
        F.when(
            valid_key,
            F.sha2(F.concat(F.lit("business:"), business_payload), 256),
        ).otherwise(
            F.sha2(F.concat(F.lit("record:"), F.col("_record_hash")), 256)
        )
    )


def deduplicate(df, keys: Sequence[str], update_column: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Without a source update timestamp, the most recently modified file wins; the
    # file path and record hash only make the choice deterministic on ties.
    window = Window.partitionBy(*keys).orderBy(
        F.col(update_column).desc_nulls_last(),
        F.col("_source_file_modified_at").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
        F.col("_record_hash").desc_nulls_last(),
    )
    return df.withColumn("_row_number", F.row_number().over(window)).filter(
        F.col("_row_number") == 1
    ).drop("_row_number")


def silver_column_names(columns: Sequence[str]) -> list[str]:
    """Normalize business column names but keep pipeline `_` metadata columns intact."""
    return [
        name if name.startswith("_") else normalize_column_name(name) for name in columns
    ]


def standardize_silver(df):
    """Normalize names/strings and apply replaceable example business casts."""
    from pyspark.sql import functions as F

    normalized = df
    new_names = silver_column_names(df.columns)
    if len(new_names) != len(set(new_names)):
        raise ValueError("Column-name standardization produced duplicate columns")
    normalized = normalized.toDF(*new_names)
    validate_required_columns(normalized, RAW_REQUIRED_COLUMNS)
    for name, data_type in normalized.dtypes:
        if data_type == "string":
            normalized = normalized.withColumn(name, F.trim(F.col(name)))

    # PLACEHOLDER TRANSFORMATION RULES: replace these example casts/normalizations
    # with the agreed sales-domain rules before production use.
    transformed = (
        normalized.withColumn(
            "quantity", F.expr("try_cast(quantity AS DECIMAL(18,3))")
        )
        .withColumn(
            "unit_price", F.expr("try_cast(unit_price AS DECIMAL(18,2))")
        )
        .withColumn(
            "sale_timestamp", F.try_to_timestamp(F.col("sale_timestamp"))
        )
    )
    if "updated_at" in transformed.columns:
        transformed = transformed.withColumn(
            "updated_at", F.try_to_timestamp(F.col("updated_at"))
        )
    if "currency" in transformed.columns:
        transformed = transformed.withColumn(
            "currency", F.upper(F.coalesce(F.col("currency"), F.lit("UNKNOWN")))
        )
    return transformed


def split_silver_quality(df, business_keys: Sequence[str]):
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as F

    invalid_key = reduce(
        or_,
        [F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "") for key in business_keys],
    )
    invalid_measure = (
        F.col("quantity").isNull()
        | (F.col("quantity") < 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | F.col("sale_timestamp").isNull()
    )
    rejected = df.filter(invalid_key | invalid_measure).withColumn(
        "_rejection_reason",
        F.when(invalid_key, F.lit("invalid_business_key")).otherwise(
            F.lit("invalid_measure_or_timestamp")
        ),
    )
    accepted = df.filter(~(invalid_key | invalid_measure))
    return accepted, rejected


def build_gold(df):
    from pyspark.sql import functions as F

    # PLACEHOLDER BUSINESS LOGIC: daily product-level gross sales. Replace or
    # extend only after the production metric definitions are approved.
    update_expression = F.coalesce(F.col("_source_updated_at"), F.col("_ingested_at"))
    return (
        df.withColumn("sale_date", F.to_date("sale_timestamp"))
        .groupBy("sale_date", "product_id")
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_sales"),
            F.max(update_expression).alias("_business_updated_at"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
