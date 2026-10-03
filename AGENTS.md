# Declarative Automation Bundles Project

This project defines a serverless Lakeflow Job through a Databricks Declarative
Automation Bundle. Read the `databricks-core`, `databricks-jobs`, `databricks-dabs`,
`databricks-serverless-migration`, and `databricks-unity-catalog` skills before changes.

Do not add cluster configuration, DBFS mounts, RDD APIs, hard-coded workspace details,
or credentials. Keep transformations in `src/common` and notebooks orchestration-only.
Run local tests and YAML/static checks before proposing workspace validation.

