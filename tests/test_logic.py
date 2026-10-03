from decimal import Decimal

import pytest

from common.logic import (
    deduplicate_records,
    gold_aggregate_records,
    raw_transform_records,
    require_columns,
    silver_quality_records,
)
from common.delta_merge import (
    changed_columns_condition,
    freshness_update_condition,
    merge_result_metrics,
)
from common.spark_transforms import (
    RAW_FILE_METADATA_COLUMNS,
    RAW_REQUIRED_COLUMNS,
    silver_column_names,
)


def test_silver_column_names_keep_raw_metadata_columns():
    raw_columns = [
        "sales_id",
        "Product ID",
        "quantity",
        "unit_price",
        "sale_timestamp",
        *RAW_FILE_METADATA_COLUMNS,
        "_ingested_at",
        "_record_hash",
        "_source_updated_at",
        "_raw_merge_key",
    ]
    names = silver_column_names(raw_columns)
    assert names[1] == "product_id"
    assert {
        "_source_file",
        "_source_file_modified_at",
        "_ingested_at",
        "_record_hash",
        "_source_updated_at",
        "_raw_merge_key",
    } <= set(names)


def test_merge_metrics_ignore_stale_history_after_no_op_merge():
    previous = {"numTargetRowsInserted": "5", "numTargetRowsUpdated": "2"}
    assert merge_result_metrics(7, 3, 3, previous) == {
        "source": 7,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
    }
    assert merge_result_metrics(7, 3, 4, previous)["inserted"] == 5
    assert merge_result_metrics(7, 3, 4, None)["updated"] == 0


def test_changed_columns_condition_ignores_unlisted_columns():
    condition = changed_columns_condition(["record_count", "gross_sales"])
    assert condition == (
        "NOT (source.`record_count` <=> target.`record_count`) OR "
        "NOT (source.`gross_sales` <=> target.`gross_sales`)"
    )
    assert "_gold_processed_at" not in condition
    with pytest.raises(ValueError):
        changed_columns_condition([])


def test_required_column_validation():
    require_columns(["sales_id", "quantity"], ["sales_id"])
    with pytest.raises(ValueError, match="unit_price"):
        require_columns(["sales_id", "quantity"], ["sales_id", "unit_price"])


def test_required_sales_columns_do_not_hardcode_the_business_key():
    assert "sales_id" not in RAW_REQUIRED_COLUMNS
    assert {"product_id", "quantity", "unit_price", "sale_timestamp"} <= (
        RAW_REQUIRED_COLUMNS
    )


def test_deduplication_keeps_latest_record_and_drops_null_key():
    records = [
        {"sales_id": "1", "updated_at": "2026-01-01", "quantity": 1},
        {"sales_id": "1", "updated_at": "2026-01-02", "quantity": 2},
        {"sales_id": None, "updated_at": "2026-01-03", "quantity": 3},
    ]
    assert deduplicate_records(records, ["sales_id"], "updated_at") == [records[1]]


def test_deduplication_uses_timestamp_order_not_lexical_order():
    records = [
        {"sales_id": "1", "updated_at": "2026-09-30T00:00:00Z"},
        {"sales_id": "1", "updated_at": "2026-10-01T00:00:00Z"},
    ]
    assert deduplicate_records(records, ["sales_id"], "updated_at") == [records[1]]
    with pytest.raises(ValueError, match="Invalid timestamp"):
        deduplicate_records(
            [{"sales_id": "1", "updated_at": "2026-9-30"}],
            ["sales_id"],
            "updated_at",
        )


def test_raw_transformation_adds_file_and_ingestion_metadata():
    transformed = raw_transform_records(
        [{"sales_id": "1"}], "volume/sales/a.parquet", "2026-01-01T00:00:00Z"
    )
    assert transformed[0]["_source_file"].endswith("a.parquet")
    assert transformed[0]["_ingested_at"] == "2026-01-01T00:00:00Z"
    assert len(transformed[0]["_record_hash"]) == 64


def test_silver_quality_rules_standardize_and_reject_bad_records():
    accepted, rejected = silver_quality_records(
        [
            {
                "Sales ID": " 1 ",
                "quantity": "2",
                "unit_price": "3.50",
                "sale_timestamp": "2026-01-02T10:00:00Z",
            },
            {
                "Sales ID": "",
                "quantity": "-1",
                "unit_price": "x",
                "sale_timestamp": "not-a-timestamp",
            },
        ],
        ["sales_id"],
    )
    assert accepted[0]["sales_id"] == "1"
    assert accepted[0]["unit_price"] == Decimal("3.50")
    assert "invalid_business_key" in rejected[0]["_rejection_reasons"]
    assert "negative_quantity" in rejected[0]["_rejection_reasons"]
    assert "invalid_sale_timestamp" in rejected[0]["_rejection_reasons"]


def test_freshness_merge_condition_blocks_duplicate_and_stale_updates():
    condition = freshness_update_condition()
    assert "_record_hash" in condition
    assert "_source_updated_at" in condition
    assert ">=" in condition


def test_gold_aggregation_logic():
    result = gold_aggregate_records(
        [
            {
                "sale_timestamp": "2026-01-02T10:00:00Z",
                "product_id": "A",
                "quantity": Decimal("2"),
                "unit_price": Decimal("4.50"),
            },
            {
                "sale_timestamp": "2026-01-02T12:00:00Z",
                "product_id": "A",
                "quantity": Decimal("1"),
                "unit_price": Decimal("4.50"),
            },
        ]
    )
    assert result[0]["record_count"] == 2
    assert result[0]["total_quantity"] == Decimal("3")
    assert result[0]["gross_sales"] == Decimal("13.50")
    assert result[0]["_gold_processed_at"]
