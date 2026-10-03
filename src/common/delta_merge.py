"""Idempotent Delta table merge helper."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def freshness_update_condition(
    record_hash_column: str = "_record_hash",
    update_timestamp_column: str = "_source_updated_at",
) -> str:
    """Return a MERGE predicate that blocks duplicates and stale updates."""
    record_hash = f"`{record_hash_column}`"
    update_timestamp = f"`{update_timestamp_column}`"
    return (
        f"NOT (source.{record_hash} <=> target.{record_hash}) AND ("
        f"(source.{update_timestamp} IS NOT NULL AND ("
        f"target.{update_timestamp} IS NULL OR "
        f"source.{update_timestamp} >= target.{update_timestamp})) OR "
        f"(source.{update_timestamp} IS NULL AND target.{update_timestamp} IS NULL))"
    )


def changed_columns_condition(columns: Sequence[str]) -> str:
    """Return a MERGE predicate that is true only when a compared value changed."""
    if not columns:
        raise ValueError("changed_columns_condition requires at least one column")
    return " OR ".join(
        f"NOT (source.`{column}` <=> target.`{column}`)" for column in columns
    )


def merge_result_metrics(
    source_rows: int,
    version_before: int,
    latest_version: int,
    operation_metrics: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Report MERGE counts, treating an unchanged table version as a no-op.

    Delta skips the commit when a MERGE changes nothing, so the latest history entry
    then belongs to an earlier operation and its metrics must not be reported.
    """
    if latest_version == version_before:
        metrics: Mapping[str, Any] = {}
    else:
        metrics = operation_metrics or {}
    return {
        "source": source_rows,
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
    }


def merge_upsert(
    spark,
    source_df,
    target_table: str,
    keys: Sequence[str],
    *,
    delete_not_matched: bool = False,
    matched_update_condition: str | None = None,
) -> dict[str, int]:
    """Create a Delta table once or MERGE by keys and return operation metrics."""
    from delta.tables import DeltaTable

    source_rows = source_df.count()
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("errorifexists").saveAsTable(target_table)
        return {"source": source_rows, "inserted": source_rows, "updated": 0, "deleted": 0}

    condition = " AND ".join([f"target.`{key}` <=> source.`{key}`" for key in keys])
    try:
        target = DeltaTable.forName(spark, target_table)
        version_before = target.history(1).select("version").first()[0]
        merge_builder = target.alias("target").merge(source_df.alias("source"), condition)
        if matched_update_condition:
            merge_builder = merge_builder.whenMatchedUpdateAll(
                condition=matched_update_condition
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        if delete_not_matched:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()
        merge_builder.execute()
        latest = target.history(1).select("version", "operationMetrics").first()
        return merge_result_metrics(
            source_rows, version_before, latest["version"], latest["operationMetrics"]
        )
    except Exception as exc:
        raise RuntimeError(f"MERGE into {target_table} failed") from exc
