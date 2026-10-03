# Serverless Sales Lakeflow Job Bundle

This repository is a local-only Databricks Declarative Automation Bundle (DAB) for a
serverless Lakeflow Job with configurable daily and file-arrival triggers. It deliberately
contains placeholders and has not been connected to, validated against, or deployed to a
Databricks workspace.

See [PROJECT_GUIDE.md](PROJECT_GUIDE.md) for the project purpose, processing behavior,
complete configuration reference, trigger modes, permissions, and worked examples.

## Project structure

```text
.
├── databricks.yml
├── resources/
│   └── sales_job.yml
├── src/
│   ├── common/
│   │   ├── config.py
│   │   ├── delta_merge.py
│   │   ├── logic.py
│   │   ├── runtime.py
│   │   └── spark_transforms.py
│   └── notebooks/
│       ├── sales_raw.py
│       ├── sales_silver.py
│       └── sales_gold.py
├── tests/
├── pyproject.toml
└── requirements-dev.txt
```

The bundle builds `src/common` as a small wheel and installs it through a shared serverless
environment (`client: "4"`) referenced by every notebook task.
That keeps reusable validation, transformations, logging, and Delta merge behavior out
of the orchestration notebooks and makes the pure business rules testable locally.

## Job and task sequence

The single job contains this strict success-only DAG:

```text
sales_raw --ALL_SUCCESS--> sales_silver --ALL_SUCCESS--> sales_gold
```

- `sales_raw` discovers eligible CSV or Parquet files in either a Unity Catalog Volume or
  an Azure ADLS path governed by a Unity Catalog external location, validates the input,
  adds `_source_file`, `_source_file_modified_at` (from the `_metadata` column), and
  `_ingested_at`, deduplicates, and MERGEs into
  `${var.catalog_name}.${var.raw_schema_name}.sales`.
- `sales_silver` normalizes columns and strings, applies example casts and quality rules,
  rejects null/invalid business keys and invalid measures, deduplicates by the configured
  key and latest update timestamp, and MERGEs accepted rows into the silver table.
- `sales_gold` computes the placeholder daily product aggregate and reconciles it with a
  Delta MERGE by `(sale_date, product_id)`. It does not overwrite or rebuild the table.

`ALL_SUCCESS` is explicit, so a raw failure prevents silver and a silver failure prevents
gold. Each task retries twice at 60-second intervals, retries timeouts, and has a one-hour
timeout; the overall job has a three-hour timeout and only one concurrent run.

## Variables and placeholders

Replace every angle-bracket placeholder before workspace validation or deployment:

| Variable | Purpose | Local default |
|---|---|---|
| `catalog_name` | Existing UC catalog | `<catalog-name>` |
| `raw_schema_name` | Existing raw schema | `<raw-schema-name>` |
| `silver_schema_name` | Existing silver schema | `<silver-schema-name>` |
| `gold_schema_name` | Existing gold schema | `<gold-schema-name>` |
| `volume_schema_name` | Schema containing the Volume | `<volume-schema-name>` |
| `volume_name` | Existing source Volume | `<volume-name>` |
| `input_source_type` | Storage mode: `volume` or `external_location` | `volume` |
| `input_format` | File format: `csv` or `parquet` | `parquet` |
| `source_path` | Volume landing path | `/Volumes/${var.catalog_name}/${var.volume_schema_name}/${var.volume_name}/sales/` |
| `external_source_path` | Azure path covered by a UC external location | `abfss://<container>@<storage-account>.dfs.core.windows.net/<external-location-prefix>/sales/` |
| `csv_header` | CSV has a header row | `true` |
| `csv_delimiter` | Single-character CSV delimiter | `,` |
| `csv_column_names` | Ordered names for headerless CSV input | `sales_id,product_id,quantity,unit_price,sale_timestamp,updated_at,currency` |
| `sales_business_keys` | Comma-separated MERGE keys | `sales_id` |
| `notification_email` | Failure recipient | `<notification-email>` |
| `schedule_pause_status` | Schedule state | `PAUSED` |
| `file_arrival_pause_status` | External-location file-arrival trigger state | `PAUSED` |
| `file_arrival_min_time_between_triggers_seconds` | Minimum interval between file-triggered runs | `60` |
| `file_arrival_wait_after_last_change_seconds` | Quiet period before a file-triggered run | `30` |

The dev target always forces both automatic triggers to `PAUSED`. Production also leaves
both triggers paused by default. The two pause variables are independent, so production
can use daily-only, file-arrival-only, both, or manual-only operation:

| Mode | `schedule_pause_status` | `file_arrival_pause_status` |
|---|---|---|
| Daily at 06:00 Australia/Sydney | `UNPAUSED` | `PAUSED` |
| New or updated file under `external_source_path` | `PAUSED` | `UNPAUSED` |
| Either event | `UNPAUSED` | `UNPAUSED` |
| Manual runs only | `PAUSED` | `PAUSED` |

> **Unverified:** the job sets both the legacy `schedule` and `trigger` fields. The Jobs
> API may allow only one of them per job (the newer Beta `triggers` list exists for multiple
> triggers), in which case deployment fails even with both paused. `bundle validate` does
> not detect this; confirm with the first dev deployment and, if rejected, keep only the
> trigger you need or migrate to `triggers`.

Enable either trigger only through an explicit production deployment override after
review. The file-arrival trigger monitors `external_source_path`; therefore use it only
when that path is the intended landing directory and is governed by a Unity Catalog
external location.

Do not put workspace URLs, principals, credentials, or environment-specific object names
into source control. Supply target overrides in an uncommitted variable file or through
the CLI when a workspace exists.

When dev and prod use separate catalogs with the same schema and table names, set only
`catalog_name` per target in an uncommitted, target-scoped file such as
`.databricks/bundle/dev/variable-overrides.json` (`.databricks/` is git-ignored):

```json
{ "catalog_name": "<dev-catalog-name>" }
```

In CI, set `BUNDLE_VAR_catalog_name` inside the dev or prod job only. A `--var
catalog_name=...` flag or an exported `BUNDLE_VAR_catalog_name` overrides the override
file and applies to whichever target is deployed, so pass it only on a command whose
`-t` target matches that catalog.

## No-file behavior

`sales_raw` first performs a metadata-only `binaryFile` discovery with a recursive
`*.csv` or `*.parquet` filter based on `input_format`. If the selected directory exists
but has no eligible files, it logs `NO_DATA`,
exits successfully, and does not create, write, or MERGE the raw table. A missing or
unreadable path is different: discovery raises an error and the notebook fails clearly.

For CSV, the reader uses the configured header and delimiter, fails on malformed input,
fails when a file's header differs from the first file's header (`enforceSchema=false`),
and keeps raw values as strings. Use `csv_delimiter=\t` for tab-delimited files. The silver transformation owns the numeric and timestamp
casts. When `csv_header=false`, `csv_column_names` supplies the ordered names and its count
must exactly match the number of CSV columns.

Silver uses ANSI-safe tolerant casts for quantity, price, and sale timestamp. Invalid
values become null and are counted as rejected records instead of crashing the task.

## Idempotency and row counts

Raw records receive a stable `_record_hash` and `_raw_merge_key`. For valid records, the
raw merge key is derived from `sales_business_keys`. For null or blank business keys it is
derived from the record hash, so distinct invalid records are retained for silver quality
reporting instead of being collapsed into one row. Silver rejects invalid keys and
deduplicates accepted rows on the configured business keys.

When `updated_at` exists, raw parses it into `_source_updated_at` and fails clearly if a
non-empty value is not a valid timestamp. Both batch deduplication and Delta MERGE use this
timestamp. A matched row updates only when its record hash changed and it is not older than
the target. An identical replay therefore makes no target change, and a late older record
cannot overwrite newer data. When neither version has an update timestamp, the version
from the most recently modified file wins, because every run rereads all files and orders
duplicates by `_source_file_modified_at` (then file path) before the MERGE.

The common Delta helper creates a target only on its first non-no-data write; subsequent
runs use `WHEN MATCHED UPDATE ALL` and `WHEN NOT MATCHED INSERT ALL`. Gold additionally
deletes target aggregate keys absent from the current source aggregate, preventing stale
groups after a sale changes date or product. Gold updates a matched aggregate only when
its count, quantity, gross sales, or business-update timestamp changed, so an unchanged
replay leaves gold rows untouched. This is a MERGE reconciliation, not a table
overwrite. The helper reads the last
Delta operation metrics to report actual inserted and updated counts, and reports zero
when the table version did not change (Delta does not commit a no-op MERGE). Silver also logs
source, accepted, and rejected counts. Gold logs input, aggregate, insert, and update
counts. These `count()` calls favor auditability over minimum scan cost and can later be
replaced with an operational metrics framework if volumes become large.

## Serverless input access

The tasks intentionally have no `new_cluster`, `existing_cluster_id`, `job_cluster_key`,
Spark version, node type, worker count, instance pool, init script, or Spark configuration.
For supported notebook tasks, omission of cluster configuration selects serverless job
compute. The code uses DataFrame APIs only—no RDDs, SparkContext, DBFS mounts, `/mnt`, or
driver-local data paths.

For `input_source_type=volume`, files are read through a Unity Catalog Volume:

```text
/Volumes/<catalog>/<schema>/<volume>/sales/
```

The Volume, catalog, and schemas must already exist. This bundle intentionally does not
create governance objects or grant permissions.

For `input_source_type=external_location`, set `external_source_path` to an Azure ADLS
Gen2 ABFSS directory:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<registered-prefix>/sales/
```

That URL must be contained by an existing Unity Catalog external location backed by an
Azure storage credential, normally a managed identity through a Databricks Access
Connector. The order is storage credential, then external location, then the job grant.
Do not add storage-account keys, SAS tokens, OAuth secrets, mount commands, or `fs.azure.*`
Spark configuration to this project. Runtime validation rejects external paths containing
query strings or fragments so SAS tokens cannot be passed as source-path parameters.

Example source selection overrides:

```bash
# Volume + Parquet
--var input_source_type=volume --var input_format=parquet

# Governed Azure external location + CSV
--var input_source_type=external_location --var input_format=csv \
  --var 'external_source_path=abfss://<container>@<account>.dfs.core.windows.net/<prefix>/sales/' \
  --var csv_header=true --var 'csv_delimiter=,'

# File-arrival-only automation for that external CSV directory
--var schedule_pause_status=PAUSED \
  --var file_arrival_pause_status=UNPAUSED

# Daily schedule plus file-arrival automation
--var schedule_pause_status=UNPAUSED \
  --var file_arrival_pause_status=UNPAUSED

# Headerless CSV additionally requires the exact ordered source columns
--var csv_header=false \
  --var csv_column_names=sales_id,product_id,quantity,unit_price,sale_timestamp
```

## Future Unity Catalog permissions

Grant the future job identity least privilege, normally through an account group or
service principal. It will need:

- `USE CATALOG` on the selected catalog.
- `USE SCHEMA` on the Volume schema and on raw, silver, and gold schemas.
- For Volume input: `READ VOLUME` on the source Volume, plus `USE CATALOG` and
  `USE SCHEMA` on its parents.
- For Azure external-location input: `READ FILES` on the Unity Catalog external location.
  The storage credential and Azure managed identity must also be able to list and read the
  configured ABFSS prefix.
- `SELECT` on the raw table for the silver task and on the silver table for gold.
- `SELECT` and `MODIFY` on existing raw, silver, and gold target tables (MERGE and the
  Delta history lookup used for metrics read the target).
- `CREATE TABLE` on each target schema if the job is expected to create its target on the
  first run.

Ownership and inherited grants can change the effective requirements. Inspect effective
grants before deployment and prefer group ownership rather than an individual user.

## Validation stages

### Fully local validation

These steps need neither a workspace nor Databricks authentication:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest
.venv/bin/python -m build --wheel
```

The wheel build needs network access to fetch `setuptools` into its isolated environment.

The pytest suite covers configuration parsing and source-path security, headerless CSV and
delimiter settings, silver column renaming of raw metadata columns, MERGE predicate text,
bundle variables, DAG dependencies, schedule timezone, and absence of cluster
configuration. Deduplication, hashing, silver quality, and gold aggregation are tested
against pure-Python reference implementations in `logic.py`, not the Spark code. Spark
and Delta APIs are imported only inside runtime functions, so actual DataFrame and Delta
execution remains a workspace-runtime check.

### Static validation without workspace access

Local YAML parsing, Python compilation, placeholder-name consistency scans, forbidden
serverless-pattern scans, and wheel inspection are static checks. They catch syntax and
structural defects but do not prove that the workspace supports the generated schema.

### Databricks workspace validation

After a workspace exists, authenticate to an explicitly selected profile and run:

```bash
databricks --version
databricks auth profiles
databricks bundle validate --strict -t dev --profile <chosen-profile>
databricks bundle validate --strict -t prod --profile <chosen-profile> \
  --var schedule_pause_status=PAUSED
```

These commands resolve workspace-aware bundle substitutions and validate the job resource.
They were intentionally not run for this local-only request.

### Runtime validation requiring serverless compute and actual data

Only an authorized workspace run can validate Volume or external-location readability,
Azure identity access, source CSV/Parquet schemas, serverless library installation, UC
privileges, first-table creation, Delta MERGE metrics, data quality behavior at scale, and
final business results. After workspace validation and review, the future commands are:

```bash
databricks bundle deploy -t dev --profile <chosen-profile>
databricks bundle run sales_lakeflow_job -t dev --profile <chosen-profile>

# Production: validate and deploy paused first. Unpause only by explicit decision.
databricks bundle deploy -t prod --profile <chosen-profile> \
  --var schedule_pause_status=PAUSED
```

Deployment and execution are intentionally deferred. Never auto-select a profile; list
all profiles and choose the intended workspace explicitly.

## Replacing placeholders later

1. Create or identify the catalog and schemas. Then either create a Volume, or register an
   Azure storage credential and external location covering the ABFSS source prefix.
2. Set `input_source_type`, `input_format`, the selected source path, and CSV options where
   applicable. Place representative sales files in that directory.
3. Confirm the actual required input columns and replace the example transformation and
   quality rules marked `PLACEHOLDER` in `spark_transforms.py`.
4. Set the correct business key or composite keys, such as `sales_id,line_id`.
5. Supply all bundle variable values without committing secrets or identities.
6. Grant the job identity the UC permissions above.
7. Run local validation, then strict workspace validation against dev.
8. Deploy and run dev with the schedule paused; compare counts and data to an approved
   fixture. Promote to prod paused, then explicitly set `UNPAUSED` only after approval.

## Assumptions

- Incoming CSV or Parquet has at least every configured business-key column plus
  `product_id`, `quantity`, `unit_price`, and `sale_timestamp`; `updated_at` and `currency`
  are optional.
- CSV input normally has a header containing those column names. Raw requires the exact,
  case-sensitive names; silver name normalization does not rescue headers such as
  `Product ID`. CSV values remain strings in raw and are cast in silver.
- Parquet files in one directory share a schema; without `mergeSchema`, columns present
  in only some files can be dropped.
- Silver reprocesses the whole raw table, so the rejected count includes all currently
  rejected raw rows, and a rejected update leaves the previously accepted silver version
  in place.
- Non-empty `updated_at` values must be parseable by Spark `to_timestamp`.
- The example gold grain is one row per sale date and product.
- Tables are Delta tables and schemas already exist.
- Rejected silver rows are counted and logged but not persisted; add a governed quarantine
  table if retention is required.
- Batch sizes make explicit audit counts acceptable; optimize these actions after observing
  production scale.
