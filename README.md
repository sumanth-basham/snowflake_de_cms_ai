# Snowflake Metadata-Driven File Ingestion Framework — GCP Platform

A production-grade, metadata-driven file ingestion and transformation framework built natively for **GCP + Snowflake**. A single YAML configuration per dataset drives all three pipeline stages: raw landing (Stage 1), standardisation & validation (Stage 2), and curated outputs (Stage 3).

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Snowflake account** | ACCOUNTADMIN or SYSADMIN role for initial setup |
| **GCP project** | A GCS bucket for source files and a Snowflake storage integration |
| **Python 3.8+** | Required only for local YAML validation tests |
| **Python packages** | `pyyaml`, `jsonschema`, `pytest` (for tests) |

---

## Repository Structure

```
snowflake_de_cms_ai/
├── configs/
│   ├── schema.yaml                    # YAML structure validation contract
│   └── datasets/
│       ├── claims_txt.yaml            # Claims TXT delta load example
│       ├── orders_csv.yaml            # Orders CSV full load example
│       └── customers_parquet.yaml     # Customers Parquet delta + schema evolution
│
├── ddl/
│   ├── 00_database_and_schemas.sql    # Database, warehouse, schema creation
│   ├── 01_file_formats.sql            # Named file formats (CSV, TXT, Parquet)
│   ├── 02_external_stages.sql         # External stage templates (update GCS URLs)
│   ├── 03_control_tables.sql          # Control and metadata tables
│   ├── 04_audit_tables.sql            # Audit tables
│   ├── 05_reject_tables.sql          # Reject / quarantine tables
│   ├── 06_rbac.sql                    # RBAC roles and grants
│   └── 07_snowpipe_gcs_pubsub.sql     # Snowpipe auto-ingestion via GCS Pub/Sub
│
├── framework/
│   ├── master_runner.sql              # Master entry point stored procedure (SQL)
│   ├── config_loader.sql              # YAML loading and validation utilities
│   ├── stage1_handler.sql             # Stage 1 raw ingestion handler
│   ├── stage2_handler.sql             # Stage 2 standardisation handler
│   ├── stage3_handler.sql             # Stage 3 post-processing handler
│   ├── python_udfs/
│   │   ├── yaml_parser.sql            # Python UDF: YAML text → VARIANT
│   │   └── schema_validator.sql       # Python UDF: jsonschema validation
│   ├── snowpark/
│   │   ├── master_runner_sp.py        # Snowpark Python stored procedure (main entry point)
│   │   └── yaml_loader_sp.py          # Snowpark YAML loader
│   └── utils/
│       ├── log_writer.sql             # Centralised logging utilities
│       ├── field_validator.sql        # Runtime field-reference validation
│       ├── sql_generator.sql          # Type-conversion SQL generation
│       ├── merge_generator.sql        # MERGE / APPEND / OVERWRITE execution
│       └── chunk_planner.sql          # Delta load chunk planning
│
├── orchestration/
│   ├── tasks_setup.sql                # Snowflake Tasks orchestration
│   └── airflow/
│       └── snowflake_ingestion_dag.py # Apache Airflow DAG option
│
└── tests/
    └── validate_yaml.py               # YAML structure validation tests (48 tests)
```

---

## Step-by-Step Setup and Run Instructions

Follow the steps below **in order**. Each step lists the exact file to run and any notes.

### Step 1 — Create the Database, Warehouse, and Schemas (SQL)

Run as `ACCOUNTADMIN` or `SYSADMIN` in a Snowflake worksheet.

```
File: ddl/00_database_and_schemas.sql
```

This creates the `CMS_AI_DB` database, the compute warehouse, and the seven schemas: `RAW`, `STG`, `CURATED`, `CONTROL`, `AUDIT`, `REJECTS`, `UTIL`.

---

### Step 2 — Create Named File Formats (SQL)

```
File: ddl/01_file_formats.sql
```

Creates the named file formats (`FMT_CSV`, `FMT_TXT`, `FMT_PARQUET`) used by COPY INTO during Stage 1.

---

### Step 3 — Create External Stages (SQL)

```
File: ddl/02_external_stages.sql
```

> **Before running:** Update the GCS bucket URLs and storage integration name to match your GCP environment.

Creates external stages pointing to your GCS buckets for raw file landing, framework configs, and archives.

---

### Step 4 — Create Control Tables (SQL)

```
File: ddl/03_control_tables.sql
```

Creates the seven control tables in the `CONTROL` schema: `PIPELINE_RUN_LOG`, `BATCH_LOG`, `FILE_LOG`, `CHUNK_LOG`, `YAML_EXECUTION_LOG`, `SOURCE_SCHEMA_REGISTRY`, `FRAMEWORK_CONFIG`.

---

### Step 5 — Create Audit Tables (SQL)

```
File: ddl/04_audit_tables.sql
```

Creates the four audit tables in the `AUDIT` schema: `MERGE_AUDIT_LOG`, `SCHEMA_DRIFT_LOG`, `STAGE3_ACTION_LOG`, `VALIDATION_LOG`.

---

### Step 6 — Create Reject Tables (SQL)

```
File: ddl/05_reject_tables.sql
```

Creates the reject/quarantine tables in the `REJECTS` schema for rows that fail Stage 2 validation.

---

### Step 7 — Set Up RBAC Roles and Grants (SQL)

```
File: ddl/06_rbac.sql
```

> **Before running:** Customise the role assignments and user grants for your team.

Creates three roles: `INGESTION_ADMIN`, `INGESTION_OPERATOR`, `INGESTION_ANALYST`, and grants appropriate privileges.

---

### Step 8 — (Optional) Set Up Snowpipe Auto-Ingestion (SQL)

```
File: ddl/07_snowpipe_gcs_pubsub.sql
```

> Only needed if you want automatic file ingestion via GCS Pub/Sub notifications.

Sets up Snowpipe for auto-ingestion when files land in GCS.

---

### Step 9 — Deploy Python UDFs (SQL)

These SQL files contain embedded Python code that runs inside Snowflake. Run them in this order:

```
File 1: framework/python_udfs/yaml_parser.sql
File 2: framework/python_udfs/schema_validator.sql
```

- **yaml_parser.sql** — Creates a Python UDF that converts YAML text to Snowflake `VARIANT`.
- **schema_validator.sql** — Creates a Python UDF that validates YAML configs against `configs/schema.yaml` using `jsonschema`.

---

### Step 10 — Deploy Utility Stored Procedures (SQL)

Run all five utility files. Order matters — `log_writer` should be first since other utilities may reference logging:

```
File 1: framework/utils/log_writer.sql
File 2: framework/utils/chunk_planner.sql
File 3: framework/utils/field_validator.sql
File 4: framework/utils/sql_generator.sql
File 5: framework/utils/merge_generator.sql
```

---

### Step 11 — Deploy Config Loader and Stage Handlers (SQL)

Run in this order — the config loader must exist before the stage handlers, and Stage 1 must exist before Stage 2:

```
File 1: framework/config_loader.sql
File 2: framework/stage1_handler.sql
File 3: framework/stage2_handler.sql
File 4: framework/stage3_handler.sql
```

---

### Step 12 — Deploy the Master Runner (SQL)

```
File: framework/master_runner.sql
```

This is the main SQL stored procedure that orchestrates the full pipeline (Stage 1 → Stage 2 → Stage 3).

---

### Step 13 — (Optional) Deploy Snowpark Python Stored Procedures

If you prefer the Snowpark Python implementation over the SQL stored procedures:

```
File 1: framework/snowpark/yaml_loader_sp.py
File 2: framework/snowpark/master_runner_sp.py
```

Deploy these via a Snowpark session (e.g., using SnowSQL, Snowflake VS Code extension, or a Snowpark Python client). `master_runner_sp.py` depends on `yaml_loader_sp.py`.

---

### Step 14 — Upload YAML Configs to Snowflake Stage

Upload the schema validation contract and dataset configs to the internal framework stage:

```sql
PUT file://configs/schema.yaml              @UTIL.STG_FW_CONFIGS/          AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://configs/datasets/claims_txt.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://configs/datasets/orders_csv.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://configs/datasets/customers_parquet.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

---

### Step 15 — Run the Pipeline

With everything deployed, execute the pipeline for any dataset with a single call:

```sql
-- Using the SQL stored procedure
CALL UTIL.MASTER_RUNNER('claims_txt.yaml', 'datasets/claims_txt.yaml', 'file_ingestion');
CALL UTIL.MASTER_RUNNER('orders_csv.yaml', 'datasets/orders_csv.yaml', 'file_ingestion');
CALL UTIL.MASTER_RUNNER('customers_parquet.yaml', 'datasets/customers_parquet.yaml', 'file_ingestion');
```

```sql
-- Using the Snowpark Python stored procedure (if deployed in Step 13)
CALL UTIL.MASTER_RUNNER_SP('claims_txt.yaml', 'datasets/claims_txt.yaml', 'file_ingestion', FALSE);
-- Set the last parameter to TRUE for post-Snowpipe mode (skips Stage 1)
CALL UTIL.MASTER_RUNNER_SP('claims_txt.yaml', 'datasets/claims_txt.yaml', 'file_ingestion', TRUE);
```

---

### Step 16 — (Optional) Set Up Orchestration

Choose one of the two scheduling options:

**Option A — Snowflake Tasks (recommended)**

```
File: orchestration/tasks_setup.sql
```

Creates one Snowflake Task per dataset. Resume and run tasks:

```sql
ALTER TASK UTIL.TASK_CLAIMS_TXT_DELTA RESUME;
EXECUTE TASK UTIL.TASK_CLAIMS_TXT_DELTA;
```

**Option B — Apache Airflow**

```
File: orchestration/airflow/snowflake_ingestion_dag.py
```

A thin Airflow DAG that calls `MASTER_RUNNER` via `SnowflakeOperator`. All business logic remains in Snowflake.

---

## Running the YAML Validation Tests (Python)

Before deploying to Snowflake, you can validate your YAML dataset configs locally:

```bash
# Install dependencies
pip install pyyaml jsonschema pytest

# Run all 48 tests
pytest tests/validate_yaml.py -v
```

---

## Quick Reference — Deployment Order Summary

| Step | File(s) | Type | Purpose |
|------|---------|------|---------|
| 1 | `ddl/00_database_and_schemas.sql` | SQL | Database, warehouse, schemas |
| 2 | `ddl/01_file_formats.sql` | SQL | Named file formats |
| 3 | `ddl/02_external_stages.sql` | SQL | GCS external stages |
| 4 | `ddl/03_control_tables.sql` | SQL | Control/metadata tables |
| 5 | `ddl/04_audit_tables.sql` | SQL | Audit tables |
| 6 | `ddl/05_reject_tables.sql` | SQL | Reject/quarantine tables |
| 7 | `ddl/06_rbac.sql` | SQL | Roles and grants |
| 8 | `ddl/07_snowpipe_gcs_pubsub.sql` | SQL | Snowpipe (optional) |
| 9 | `framework/python_udfs/yaml_parser.sql` | SQL+Python | YAML → VARIANT UDF |
| 9 | `framework/python_udfs/schema_validator.sql` | SQL+Python | Schema validation UDF |
| 10 | `framework/utils/log_writer.sql` | SQL | Logging utilities |
| 10 | `framework/utils/chunk_planner.sql` | SQL | Chunk planning |
| 10 | `framework/utils/field_validator.sql` | SQL | Field validation |
| 10 | `framework/utils/sql_generator.sql` | SQL | SQL generation |
| 10 | `framework/utils/merge_generator.sql` | SQL | Merge execution |
| 11 | `framework/config_loader.sql` | SQL | Config loading |
| 11 | `framework/stage1_handler.sql` | SQL | Stage 1 handler |
| 11 | `framework/stage2_handler.sql` | SQL | Stage 2 handler |
| 11 | `framework/stage3_handler.sql` | SQL | Stage 3 handler |
| 12 | `framework/master_runner.sql` | SQL | Master runner (SQL) |
| 13 | `framework/snowpark/yaml_loader_sp.py` | Python | Snowpark YAML loader (optional) |
| 13 | `framework/snowpark/master_runner_sp.py` | Python | Snowpark master runner (optional) |
| 14 | `configs/schema.yaml` + `configs/datasets/*.yaml` | YAML | Upload configs to stage |
| 15 | — | SQL | Run the pipeline |
| 16 | `orchestration/tasks_setup.sql` or `orchestration/airflow/snowflake_ingestion_dag.py` | SQL / Python | Scheduling (optional) |