"""Pure-Python reference transformations used by fast local unit tests.

These helpers test the portable parts of the corresponding Spark rules without
pretending to execute Spark or Delta locally.
"""

from __future__ import annotations

import re
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    if not normalized:
        raise ValueError(f"Column name {name!r} normalizes to an empty value")
    return normalized


def require_columns(columns: Iterable[str], required: Iterable[str]) -> None:
    available = set(columns)
    missing = sorted(set(required) - available)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def deduplicate_records(
    records: Iterable[dict[str, Any]], keys: list[str], update_field: str
) -> list[dict[str, Any]]:
    """Keep the record with the greatest update value for each business key."""
    winners: dict[tuple[Any, ...], dict[str, Any]] = {}
    for record in records:
        key = tuple(record.get(name) for name in keys)
        if any(value is None or value == "" for value in key):
            continue
        current = winners.get(key)
        incoming_update = parse_timestamp(record.get(update_field))
        current_update = (
            parse_timestamp(current.get(update_field)) if current is not None else None
        )
        if current is None or (incoming_update or datetime.min.replace(tzinfo=timezone.utc)) >= (
            current_update or datetime.min.replace(tzinfo=timezone.utc)
        ):
            winners[key] = dict(record)
    return list(winners.values())


def parse_timestamp(value: Any) -> datetime | None:
    """Parse ISO-like timestamps consistently; empty values represent no timestamp."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"Invalid timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def raw_transform_records(
    records: Iterable[dict[str, Any]], source_file: str, ingested_at: str
) -> list[dict[str, Any]]:
    transformed = []
    for record in records:
        payload = json.dumps(record, sort_keys=True, default=str, separators=(",", ":"))
        transformed.append(
            {
                **record,
                "_source_file": source_file,
                "_ingested_at": ingested_at,
                "_record_hash": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }
        )
    return transformed


def silver_quality_records(
    records: Iterable[dict[str, Any]], business_keys: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize fields and split valid from rejected example sales records."""
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for original in records:
        record = {normalize_column_name(k): v for k, v in original.items()}
        for name, value in list(record.items()):
            if isinstance(value, str):
                record[name] = value.strip()
        reasons: list[str] = []
        if any(record.get(key) in (None, "") for key in business_keys):
            reasons.append("invalid_business_key")
        for field in ("quantity", "unit_price"):
            try:
                value = Decimal(str(record.get(field)))
                if value < 0:
                    reasons.append(f"negative_{field}")
                record[field] = value
            except (InvalidOperation, TypeError):
                reasons.append(f"invalid_{field}")
        try:
            record["sale_timestamp"] = parse_timestamp(record.get("sale_timestamp"))
            if record["sale_timestamp"] is None:
                reasons.append("invalid_sale_timestamp")
        except ValueError:
            reasons.append("invalid_sale_timestamp")
        if reasons:
            rejected.append({**record, "_rejection_reasons": sorted(set(reasons))})
        else:
            accepted.append(record)
    return accepted, rejected


def gold_aggregate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate accepted records by sales date and product."""
    groups: dict[tuple[str, Any], dict[str, Any]] = defaultdict(
        lambda: {"record_count": 0, "total_quantity": Decimal("0"), "gross_sales": Decimal("0")}
    )
    for record in records:
        timestamp = record["sale_timestamp"]
        sale_date = str(timestamp)[:10]
        key = (sale_date, record.get("product_id"))
        group = groups[key]
        quantity = Decimal(str(record["quantity"]))
        unit_price = Decimal(str(record["unit_price"]))
        group["record_count"] += 1
        group["total_quantity"] += quantity
        group["gross_sales"] += quantity * unit_price
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "sale_date": sale_date,
            "product_id": product_id,
            **values,
            "_business_updated_at": now,
            "_gold_processed_at": now,
        }
        for (sale_date, product_id), values in sorted(groups.items(), key=str)
    ]
