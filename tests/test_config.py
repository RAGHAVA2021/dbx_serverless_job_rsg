from pathlib import Path

import pytest
import yaml

from common.config import (
    parse_business_keys,
    parse_csv_column_names,
    resolve_source_path,
    validate_csv_options,
    validate_input_format,
    validate_runtime_config,
    validate_source_path,
)


ROOT = Path(__file__).parents[1]


def test_business_key_configuration_validation():
    assert parse_business_keys("sales_id, line_id") == ["sales_id", "line_id"]
    with pytest.raises(ValueError, match="duplicate"):
        parse_business_keys("sales_id,sales_id")
    with pytest.raises(ValueError, match="at least one"):
        parse_business_keys(" , ")


def test_runtime_configuration_rejects_placeholders():
    with pytest.raises(ValueError, match="replaced"):
        validate_runtime_config(
            {"catalog_name": "<catalog-name>"}, {"catalog_name"}
        )
    assert validate_source_path("/Volumes/catalog/landing/files/sales/").startswith(
        "/Volumes/"
    )


def test_configurable_input_source_and_format():
    volume_path = "/Volumes/catalog/landing/files/sales/"
    external_path = (
        "abfss://landing@storageaccount.dfs.core.windows.net/sales/"
    )
    assert resolve_source_path("volume", volume_path, "<unused>") == volume_path
    assert (
        resolve_source_path("external_location", "<unused>", external_path)
        == external_path
    )
    assert validate_input_format("CSV") == "csv"
    assert validate_input_format("parquet") == "parquet"
    assert validate_csv_options("TRUE", "|") == ("true", "|")
    assert parse_csv_column_names("sales_id, product_id") == [
        "sales_id",
        "product_id",
    ]
    assert validate_csv_options("true", "\\t") == ("true", "\\t")
    validate_runtime_config(
        {"csv_header": "true", "csv_delimiter": " "}, {"csv_header", "csv_delimiter"}
    )
    with pytest.raises(ValueError, match="csv_delimiter"):
        validate_csv_options("true", ";;")
    with pytest.raises(ValueError, match="input_source_type"):
        resolve_source_path("azure", volume_path, external_path)
    with pytest.raises(ValueError, match="input_format"):
        validate_input_format("json")
    with pytest.raises(ValueError, match="abfss"):
        resolve_source_path("external_location", volume_path, "https://example.test")
    with pytest.raises(ValueError, match="SAS"):
        resolve_source_path(
            "external_location", volume_path, f"{external_path}?sv=secret"
        )


def test_bundle_configuration_and_dependency_dag():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    job_file = yaml.safe_load((ROOT / "resources/sales_job.yml").read_text())
    required_variables = {
        "catalog_name",
        "raw_schema_name",
        "silver_schema_name",
        "gold_schema_name",
        "volume_schema_name",
        "volume_name",
        "input_source_type",
        "input_format",
        "source_path",
        "external_source_path",
        "csv_header",
        "csv_delimiter",
        "csv_column_names",
        "sales_business_keys",
        "notification_email",
        "schedule_pause_status",
        "file_arrival_pause_status",
        "file_arrival_min_time_between_triggers_seconds",
        "file_arrival_wait_after_last_change_seconds",
    }
    assert required_variables <= set(bundle["variables"])
    assert bundle["targets"]["dev"]["variables"]["schedule_pause_status"] == "PAUSED"
    assert (
        bundle["targets"]["dev"]["variables"]["file_arrival_pause_status"]
        == "PAUSED"
    )
    assert bundle["variables"]["input_source_type"]["default"] == "volume"
    assert bundle["variables"]["input_format"]["default"] == "parquet"

    job = job_file["resources"]["jobs"]["sales_lakeflow_job"]
    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert list(tasks) == ["sales_raw", "sales_silver", "sales_gold"]
    assert tasks["sales_silver"]["depends_on"] == [{"task_key": "sales_raw"}]
    assert tasks["sales_gold"]["depends_on"] == [{"task_key": "sales_silver"}]
    assert tasks["sales_silver"]["run_if"] == "ALL_SUCCESS"
    assert tasks["sales_gold"]["run_if"] == "ALL_SUCCESS"
    assert job["schedule"]["timezone_id"] == "Australia/Sydney"
    assert job["schedule"]["pause_status"] == "${var.schedule_pause_status}"
    assert job["trigger"] == {
        "pause_status": "${var.file_arrival_pause_status}",
        "file_arrival": {
            "url": "${var.external_source_path}",
            "min_time_between_triggers_seconds": "${var.file_arrival_min_time_between_triggers_seconds}",
            "wait_after_last_change_seconds": "${var.file_arrival_wait_after_last_change_seconds}",
        },
    }
    assert job["environments"] == [
        {
            "environment_key": "sales_environment",
            "spec": {"client": "4", "dependencies": ["../dist/*.whl"]},
        }
    ]
    assert all(task["environment_key"] == "sales_environment" for task in job["tasks"])
    job_parameters = {parameter["name"] for parameter in job["parameters"]}
    assert {
        "input_source_type",
        "input_format",
        "source_path",
        "external_source_path",
        "csv_header",
        "csv_delimiter",
        "csv_column_names",
    } <= job_parameters
    raw_parameters = tasks["sales_raw"]["notebook_task"]["base_parameters"]
    assert {
        "input_source_type",
        "input_format",
        "source_path",
        "external_source_path",
        "csv_header",
        "csv_delimiter",
        "csv_column_names",
    } <= set(raw_parameters)
    forbidden = {"new_cluster", "existing_cluster_id", "job_cluster_key"}
    assert not any(forbidden & set(task) for task in job["tasks"])
