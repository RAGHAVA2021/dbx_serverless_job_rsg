"""Configuration parsing and validation shared by notebooks and local tests."""

from __future__ import annotations

import re
from collections.abc import Mapping

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_AZURE_ABFSS_PATH = re.compile(
    r"^abfss://[^/@]+@[^/@]+\.dfs\.core\.windows\.net(?:/.*)?$",
    re.IGNORECASE,
)
ALLOWED_SOURCE_TYPES = {"volume", "external_location"}
ALLOWED_INPUT_FORMATS = {"csv", "parquet"}


def parse_business_keys(value: str) -> list[str]:
    """Parse a comma-separated business-key setting while preserving order."""
    keys = [part.strip() for part in value.split(",") if part.strip()]
    if not keys:
        raise ValueError("sales_business_keys must contain at least one column")
    if len(keys) != len(set(keys)):
        raise ValueError("sales_business_keys contains duplicate columns")
    for key in keys:
        validate_identifier(key, "business key")
    return keys


def parse_csv_column_names(value: str) -> list[str]:
    """Parse the explicit schema names used for headerless CSV files."""
    names = [part.strip() for part in value.split(",") if part.strip()]
    if not names:
        raise ValueError("csv_column_names must contain at least one column")
    if len(names) != len(set(names)):
        raise ValueError("csv_column_names contains duplicate columns")
    for name in names:
        validate_identifier(name, "CSV column name")
    return names


def validate_identifier(value: str, label: str) -> str:
    """Accept simple UC identifier components and reject unresolved placeholders."""
    if not value or "<" in value or "${" in value or not _IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{label} must be replaced with a simple Unity Catalog identifier; got {value!r}"
        )
    return value


def validate_volume_source_path(path: str) -> str:
    """Require a resolved Unity Catalog Volume path."""
    if "<" in path or "${" in path:
        raise ValueError(f"source_path contains an unresolved placeholder: {path!r}")
    parts = [part for part in path.split("/") if part]
    if len(parts) < 5 or parts[0] != "Volumes":
        raise ValueError(
            "source_path must use /Volumes/<catalog>/<schema>/<volume>/<path>"
        )
    return path


def validate_external_source_path(path: str) -> str:
    """Require an Azure ABFSS path governed by a UC external location."""
    if "<" in path or "${" in path:
        raise ValueError(f"external_source_path contains an unresolved placeholder: {path!r}")
    if "?" in path or "#" in path:
        raise ValueError(
            "external_source_path must not contain query parameters, fragments, or SAS tokens"
        )
    if not _AZURE_ABFSS_PATH.fullmatch(path):
        raise ValueError(
            "external_source_path must use "
            "abfss://<container>@<account>.dfs.core.windows.net/<path>"
        )
    return path


def validate_source_path(path: str) -> str:
    """Backward-compatible alias for Volume-path validation."""
    return validate_volume_source_path(path)


def validate_input_source_type(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ALLOWED_SOURCE_TYPES:
        raise ValueError(
            f"input_source_type must be one of {sorted(ALLOWED_SOURCE_TYPES)}; got {value!r}"
        )
    return normalized


def validate_input_format(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ALLOWED_INPUT_FORMATS:
        raise ValueError(
            f"input_format must be one of {sorted(ALLOWED_INPUT_FORMATS)}; got {value!r}"
        )
    return normalized


def validate_csv_options(header: str, delimiter: str) -> tuple[str, str]:
    normalized_header = header.strip().lower()
    if normalized_header not in {"true", "false"}:
        raise ValueError("csv_header must be true or false")
    # Spark's CSV reader accepts the two-character escape "\t" for tab; job
    # parameters cannot easily carry a literal tab.
    if len(delimiter) != 1 and delimiter != "\\t":
        raise ValueError("csv_delimiter must be exactly one character or \\t")
    return normalized_header, delimiter


def resolve_source_path(
    input_source_type: str, source_path: str, external_source_path: str
) -> str:
    """Select and validate the configured storage path."""
    source_type = validate_input_source_type(input_source_type)
    if source_type == "volume":
        return validate_volume_source_path(source_path)
    return validate_external_source_path(external_source_path)


def validate_runtime_config(config: Mapping[str, str], required: set[str]) -> None:
    # A delimiter may legitimately be whitespace, so it is not stripped.
    missing = sorted(
        name
        for name in required
        if not (
            config.get(name, "")
            if name == "csv_delimiter"
            else config.get(name, "").strip()
        )
    )
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    for name in required & {
        "catalog_name",
        "raw_schema_name",
        "silver_schema_name",
        "gold_schema_name",
    }:
        validate_identifier(config[name], name)
    if "input_source_type" in required:
        validate_input_source_type(config["input_source_type"])
    if "input_format" in required:
        validate_input_format(config["input_format"])
    if {"input_source_type", "source_path", "external_source_path"} <= required:
        resolve_source_path(
            config["input_source_type"],
            config["source_path"],
            config["external_source_path"],
        )
    elif "source_path" in required:
        validate_volume_source_path(config["source_path"])
    if {"csv_header", "csv_delimiter"} <= required:
        validate_csv_options(config["csv_header"], config["csv_delimiter"])
    if "csv_column_names" in required:
        parse_csv_column_names(config["csv_column_names"])
    if "sales_business_keys" in required:
        parse_business_keys(config["sales_business_keys"])


def table_name(catalog: str, schema: str, table: str = "sales") -> str:
    """Build a validated three-part UC table name."""
    return ".".join(
        [
            validate_identifier(catalog, "catalog_name"),
            validate_identifier(schema, "schema_name"),
            validate_identifier(table, "table_name"),
        ]
    )
