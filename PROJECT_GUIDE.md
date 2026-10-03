# Sales Lakeflow Project Guide

## 1. Project purpose

This project deploys a serverless Databricks Lakeflow Job that reads sales files,
validates and standardizes their contents, and publishes governed Delta tables in a
raw-silver-gold layout.

It is intended as a configurable starting point for a batch sales ingestion workload.
The same code can read CSV or Parquet files from either a Unity Catalog Volume or an
Azure Data Lake Storage Gen2 path governed by a Unity Catalog external location.

The project is packaged and deployed as a Databricks Declarative Automation Bundle
(DAB). The bundle defines the job, its parameters, trigger settings, task dependencies,
serverless environment, retries, timeouts, and notifications.

The project does not create Azure storage, a storage credential, an external location,
a catalog, schemas, or a Volume. Those governed resources must exist before deployment.
It also does not contain credentials or use DBFS mounts.

## 2. Processing flow

The job runs three notebook tasks in sequence:

```text
CSV or Parquet files
        |
        v
sales_raw --ALL_SUCCESS--> sales_silver --ALL_SUCCESS--> sales_gold
        |                         |                         |
        v                         v                         v
<catalog>.<raw>.sales    <catalog>.<silver>.sales   <catalog>.<gold>.sales
```

### Raw task

The `sales_raw` task:

- Recursively discovers files matching the configured CSV or Parquet format.
- Exits successfully without changing a table when no eligible files exist.
- Reads CSV in `FAILFAST` mode, rejects files whose header differs from the first
  file's header, and keeps CSV values as strings.
- Validates required columns and configured business-key columns (exact,
  case-sensitive names).
- Adds source file path and modification time (from the `_metadata` column), ingestion
  timestamp, record hash, and source-update metadata.
- Deduplicates records and MERGEs them into the raw Delta table.
- Prevents an older record from replacing a newer record when `updated_at` is supplied.
  Without `updated_at`, the version from the most recently modified file wins.

### Silver task

The `sales_silver` task:

- Normalizes business column names (pipeline `_` metadata columns are kept as-is) and
  trims string values.
- Casts quantity, unit price, sale timestamp, and optional update timestamp.
- Rejects missing business keys, negative or invalid measures, and invalid timestamps.
- Deduplicates accepted records using the configured business keys.
- MERGEs accepted records into the silver Delta table.
- Counts rejected records in task metrics. It does not currently persist rejected rows
  to a quarantine table. Because the whole raw table is reprocessed, the count covers all
  currently rejected raw rows, and a rejected update leaves the earlier accepted silver
  version unchanged.

### Gold task

The `sales_gold` task produces the current example business output:

- One row per `sale_date` and `product_id`.
- Record count.
- Total quantity.
- Gross sales calculated as `quantity * unit_price`.

Gold updates an existing aggregate only when its values changed and deletes aggregates
that no longer exist in silver.

The silver casts and gold metrics are marked as placeholder business rules in the code.
They must be reviewed and replaced or extended before production use.

## 3. Compute and deployment model

The notebook tasks use Databricks serverless job compute. No cluster ID, Spark version,
node type, worker count, init script, or DBFS mount is configured.

Reusable code under `src/common` is built as a pure-Python wheel and installed into the
shared serverless environment. The notebooks remain orchestration-only.

The bundle contains two deployment targets:

| Target | Purpose | Automatic triggers |
|---|---|---|
| `dev` | Development and validation | Always forced to `PAUSED` |
| `prod` | Production deployment | Paused by default; explicitly configurable |

## 4. Input configuration

### Storage source

Set `input_source_type` to one of the following values:

| Value | Read path | Required governance |
|---|---|---|
| `volume` | `source_path` | Existing Unity Catalog Volume |
| `external_location` | `external_source_path` | Existing UC external location covering the ABFSS prefix |

Example Volume path:

```text
/Volumes/<catalog>/<schema>/<volume>/sales/
```

Example Azure external-location path:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<registered-prefix>/sales/
```

The Azure path must not contain a SAS token, query string, or fragment. Access should be
provided through the Unity Catalog storage credential and external location.

### File format

Set `input_format` to either `csv` or `parquet`.

CSV supports these additional settings:

| Variable | Meaning | Default |
|---|---|---|
| `csv_header` | Whether the first row contains column names | `true` |
| `csv_delimiter` | One-character delimiter, or `\t` for tab | `,` |
| `csv_column_names` | Ordered column names used when there is no header | Example sales columns |

When `csv_header=false`, the number of configured column names must exactly match the
number of columns in the CSV file.

### Expected input columns

The current implementation requires:

| Column | Purpose |
|---|---|
| Configured business key, default `sales_id` | Identifies a sale for MERGE and deduplication |
| `product_id` | Gold aggregation dimension |
| `quantity` | Decimal quantity; must be non-negative after casting |
| `unit_price` | Decimal unit price; must be non-negative after casting |
| `sale_timestamp` | Determines the sales date |

Optional columns:

| Column | Behavior |
|---|---|
| `updated_at` | Controls freshness so stale records cannot overwrite newer records |
| `currency` | Trimmed and converted to uppercase; null becomes `UNKNOWN` |

`sales_business_keys` can contain one column or a comma-separated composite key, for
example `sales_id,line_id`.

## 5. Trigger configuration

The job defines two independent automatic triggers:

1. A daily schedule at 06:00 in the `Australia/Sydney` timezone.
2. A file-arrival trigger that monitors `external_source_path`.

Configure the trigger mode with these two variables:

| Operating mode | `schedule_pause_status` | `file_arrival_pause_status` |
|---|---|---|
| Daily only | `UNPAUSED` | `PAUSED` |
| File arrival only | `PAUSED` | `UNPAUSED` |
| Daily and file arrival | `UNPAUSED` | `UNPAUSED` |
| Manual only | `PAUSED` | `PAUSED` |

> **Unverified:** the job defines both the legacy `schedule` and `trigger` fields. The
> Jobs API may permit only one per job (the newer Beta `triggers` list exists for multiple
> triggers), which would make deployment fail even when both are paused. Confirm on the
> first dev deployment; if rejected, keep only the needed trigger or migrate to `triggers`.

File-arrival behavior is tuned with:

| Variable | Default | Purpose |
|---|---:|---|
| `file_arrival_min_time_between_triggers_seconds` | `60` | Minimum time between file-triggered runs |
| `file_arrival_wait_after_last_change_seconds` | `30` | Quiet period after the last detected change |

The current file-arrival trigger always monitors `external_source_path`. Enable it only
when the job reads the intended external-location directory. A Volume can be used as the
batch input, but the current trigger configuration does not monitor `source_path`.

Prefer adding files with new, unique names. Overwriting an existing object might be
detected as a change, but immutable landing files are more reliable and easier to audit.

The job has `max_concurrent_runs: 1`. If two triggers fire while a run is active,
Databricks concurrency and queue behavior controls whether another run waits or is
skipped.

## 6. Important ingestion behavior

Each run recursively rereads every matching file under the selected directory. The raw
and silver MERGE logic makes identical reprocessing idempotent and blocks stale updates,
but the job does not maintain a file checkpoint or processed-file ledger.

Therefore:

- It is safe to replay the same unchanged data from a row-merge perspective.
- Runtime and storage-listing cost can grow as the landing directory grows.
- This is not strict “process each file exactly once” ingestion.
- Deleting a source file does not delete its corresponding raw or silver record.

For very large or continuously growing landing areas, consider archiving processed files
or changing ingestion to Auto Loader with a governed checkpoint location.

## 7. Complete bundle variable reference

| Variable | Purpose | Default |
|---|---|---|
| `catalog_name` | Existing Unity Catalog catalog | `<catalog-name>` |
| `raw_schema_name` | Schema containing the raw table | `<raw-schema-name>` |
| `silver_schema_name` | Schema containing the silver table | `<silver-schema-name>` |
| `gold_schema_name` | Schema containing the gold table | `<gold-schema-name>` |
| `volume_schema_name` | Schema containing the source Volume | `<volume-schema-name>` |
| `volume_name` | Source Volume name | `<volume-name>` |
| `input_source_type` | `volume` or `external_location` | `volume` |
| `input_format` | `csv` or `parquet` | `parquet` |
| `source_path` | Volume input directory | Derived `/Volumes/.../sales/` path |
| `external_source_path` | Governed Azure ABFSS input directory and monitored trigger path | Placeholder ABFSS path |
| `csv_header` | CSV header flag | `true` |
| `csv_delimiter` | CSV delimiter | `,` |
| `csv_column_names` | Ordered names for headerless CSV | Example sales columns |
| `sales_business_keys` | MERGE and deduplication key columns | `sales_id` |
| `notification_email` | Job failure notification recipient | `<notification-email>` |
| `schedule_pause_status` | Daily schedule state | `PAUSED` |
| `file_arrival_pause_status` | File-arrival trigger state | `PAUSED` |
| `file_arrival_min_time_between_triggers_seconds` | Minimum file-trigger interval | `60` |
| `file_arrival_wait_after_last_change_seconds` | File-arrival quiet period | `30` |

All angle-bracket placeholders must be replaced before workspace validation or
deployment.

## 8. Configuration examples

The following fragments are passed to Databricks bundle commands using `--var`.

### Volume with Parquet and manual execution

```bash
--var catalog_name=your_catalog \
--var raw_schema_name=raw_sales \
--var silver_schema_name=silver_sales \
--var gold_schema_name=gold_sales \
--var volume_schema_name=landing \
--var volume_name=sales_files \
--var input_source_type=volume \
--var input_format=parquet \
--var schedule_pause_status=PAUSED \
--var file_arrival_pause_status=PAUSED
```

### External location with CSV and daily execution

```bash
--var input_source_type=external_location \
--var input_format=csv \
--var 'external_source_path=abfss://<container>@<account>.dfs.core.windows.net/<prefix>/sales/' \
--var csv_header=true \
--var 'csv_delimiter=,' \
--var schedule_pause_status=UNPAUSED \
--var file_arrival_pause_status=PAUSED
```

### External location with CSV and file-arrival execution

```bash
--var input_source_type=external_location \
--var input_format=csv \
--var 'external_source_path=abfss://<container>@<account>.dfs.core.windows.net/<prefix>/sales/' \
--var schedule_pause_status=PAUSED \
--var file_arrival_pause_status=UNPAUSED
```

### External location with both triggers

```bash
--var input_source_type=external_location \
--var input_format=csv \
--var 'external_source_path=abfss://<container>@<account>.dfs.core.windows.net/<prefix>/sales/' \
--var schedule_pause_status=UNPAUSED \
--var file_arrival_pause_status=UNPAUSED
```

### Headerless pipe-delimited CSV with a composite key

```bash
--var input_source_type=external_location \
--var input_format=csv \
--var csv_header=false \
--var 'csv_delimiter=|' \
--var csv_column_names=sales_id,line_id,product_id,quantity,unit_price,sale_timestamp,updated_at,currency \
--var sales_business_keys=sales_id,line_id
```

## 9. Unity Catalog access requirements

The future job identity requires least-privilege access appropriate to the selected
configuration:

- `USE CATALOG` on the configured catalog.
- `USE SCHEMA` on the raw, silver, and gold schemas, and on the Volume schema for Volume
  input.
- `READ VOLUME` plus parent access for Volume input.
- `READ FILES` on the external location for Azure external-location input and file
  arrival monitoring.
- `SELECT` on source tables used by downstream tasks.
- `SELECT` and `MODIFY` on existing target tables (MERGE reads the target).
- `CREATE TABLE` on target schemas if the job creates the `sales` tables.

The Azure identity behind the storage credential must also be able to list and read the
configured ADLS prefix.

## 10. Notifications, retries, and limits

- Failure notifications go to `notification_email`.
- Each task retries twice with a 60-second retry interval.
- Each task has a one-hour timeout.
- The overall job has a three-hour timeout.
- Only one job run executes concurrently.
- Downstream tasks run only when their dependency succeeds.

## 11. Local validation and workspace lifecycle

Install the local development dependencies and run:

```bash
python -m pytest
python -m compileall -q src tests
```

Local tests validate configuration, column-renaming and MERGE-predicate helpers,
pure-Python reference versions of the business rules, and bundle structure. They do not
execute Spark,
Delta MERGE, Azure access, serverless library installation, or file-arrival behavior.

When a workspace becomes available, first list authentication profiles and explicitly
choose the correct one. Then validate the bundle before deployment:

```bash
databricks auth profiles
databricks bundle validate --strict -t dev --profile <chosen-profile>
databricks bundle validate --strict -t prod --profile <chosen-profile> \
  --var schedule_pause_status=PAUSED \
  --var file_arrival_pause_status=PAUSED
```

Deploy production with both triggers paused first. Enable the required trigger mode only
after validating permissions, representative input files, output tables, and business
rules in development.

## 12. Current boundaries

- Only CSV and Parquet input are supported.
- External cloud input is currently restricted to Azure ABFSS paths.
- The table name is fixed as `sales`; catalog and schema names are configurable.
- The daily time and timezone are currently fixed in `resources/sales_job.yml`.
- The file-arrival monitor is currently tied to `external_source_path`.
- Rejected silver rows are counted but not persisted.
- The transformations and gold metrics are examples that require business approval.
- The bundle has not been validated against or deployed to a workspace from this local
  project.
