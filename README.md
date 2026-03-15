# Snowflake Metadata-Driven File Ingestion Framework — GCP Platform

A production-grade, metadata-driven file ingestion and transformation framework built natively for **GCP + Snowflake**.

## Overview

This framework provides a reusable, parameter-driven pipeline for ingesting files from cloud storage into Snowflake through three ordered stages: raw landing (Stage1), standardisation and validation (Stage2), and curated outputs (Stage3).

A single master entry point handles any dataset by accepting three runtime parameters:

| Parameter | Description |
|---|---|
| `yaml_name` | Dataset YAML filename (e.g. `claims_txt.yaml`) |
| `yaml_file_path` | Path within the config stage (e.g. `datasets/claims_txt.yaml`) |
| `process_type` | Process type — currently `file_ingestion` |

```sql
-- Run any dataset with one call (Snowpark Python SP — GCP platform)
CALL UTIL.MASTER_RUNNER_SP(
    'claims_txt.yaml',
    'datasets/claims_txt.yaml',
    'file_ingestion',
    FALSE   -- FALSE = full pipeline; TRUE = post-Snowpipe (Stage2+Stage3 only)
);

-- Original SQL SP (still available as fallback)
CALL UTIL.MASTER_RUNNER(
    'claims_txt.yaml',
    'datasets/claims_txt.yaml',
    'file_ingestion'
);
```

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
│   ├── 02_external_stages.sql         # External stage templates
│   ├── 03_control_tables.sql          # Control and metadata tables
│   ├── 04_audit_tables.sql            # Audit tables
│   ├── 05_reject_tables.sql           # Reject / quarantine tables
│   └── 06_rbac.sql                    # RBAC roles and grants
│
├── framework/
│   ├── master_runner.sql              # Master entry point stored procedure
│   ├── config_loader.sql              # YAML loading and validation utilities
│   ├── stage1_handler.sql             # Stage1 raw ingestion handler
│   ├── stage2_handler.sql             # Stage2 standardisation handler
│   ├── stage3_handler.sql             # Stage3 post-processing handler
│   ├── python_udfs/
│   │   ├── yaml_parser.sql            # Python UDF: YAML → VARIANT
│   │   └── schema_validator.sql       # Python UDF: jsonschema validation
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
    └── validate_yaml.py               # YAML structure validation tests
```

---

## Architecture

### Three-Stage Design

```
Cloud Storage
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ STAGE 1 – RAW                                        │
│  • COPY INTO raw table                               │
│  • ALL payload columns as VARCHAR                    │
│  • Source schema discovery (INFER_SCHEMA)            │
│  • batch_load_date as TIMESTAMP_NTZ                  │
│  • File, batch, chunk tracking                       │
│  Target schema: RAW                                  │
└───────────────────────┬─────────────────────────────┘
                        │ Discovered columns + batch context
                        ▼
┌─────────────────────────────────────────────────────┐
│ STAGE 2 – STG                                        │
│  • Runtime field-reference validation (fail-fast)    │
│  • Schema drift detection and handling               │
│  • Type conversions (LONG, INT, DECIMAL, DATE, etc.) │
│  • Null checks and composite unique checks           │
│  • Reject routing to REJECTS schema                  │
│  • MERGE / APPEND / OVERWRITE to STG table           │
│  Target schema: STG                                  │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│ STAGE 3 – CURATED                                    │
│  • Execute ordered sql_query actions from YAML       │
│  • CREATE OR REPLACE VIEW / TABLE / etc.             │
│  • Each action: name + type + value                  │
│  Target schema: CURATED                              │
└─────────────────────────────────────────────────────┘
```

### Snowflake Schemas

| Schema | Purpose |
|---|---|
| `RAW` | Stage1 raw landing tables — all payload columns as VARCHAR |
| `STG` | Stage2 typed, validated, merged data |
| `CURATED` | Stage3 views, aggregates, and published tables |
| `CONTROL` | Pipeline run logs, batch logs, file logs, chunk logs, YAML execution logs |
| `AUDIT` | Merge audit, schema drift log, Stage3 action log, validation log |
| `REJECTS` | Quarantine tables for rows failing Stage2 validation |
| `UTIL` | Framework stored procedures, Python UDFs, file formats, stages |

---

## YAML Configuration

Every dataset is configured through a single YAML file with three sections that map directly to the three execution stages.

### Stage1 — Raw Ingestion

```yaml
stage1:
  source_file_path: s3://my-bucket/raw/claims/
  source_arrival_file_path: s3://my-bucket/raw/claims/arrival/
  file_pattern: ".*\\.txt"
  target_schema: RAW
  target_table: CLAIMS_TXT_RAW
  load_type: delta          # full | delta | adhoc
  file_type: txt            # csv | txt | parquet
  archive_files: true
  zip_handling: true
  allow_zero_byte_files: false
  header_special_chars_cleanup: true
  read_options:
    field_delimiter: "|"
    skip_header: 1
    null_if: ["", "NULL"]
    trim_space: true
    encoding: UTF8
```

### Stage2 — Standardisation and Validation

```yaml
stage2:
  load_type: delta
  target_schema: STG
  target_table: CLAIMS_TXT_STD
  primary_keys: ["claim_id"]
  merge_util: "yes"           # yes = MERGE upsert | no = APPEND or OVERWRITE
  schema_changes: "no"        # yes = ADD COLUMN on drift | no = FAIL on drift
  delete_column_name: delete_flag
  delete_column_value: "D"
  fields_long_conversion: ["claim_id", "member_id"]
  fields_integer_conversion: ["line_number"]
  fields_float_conversion: ["allowed_ratio"]
  fields_decimal_conversion:
    - column_name: billed_amount
      precision: 18
      scale: 2
  fields_timestamp_conversion: ["ingestion_ts"]
  fields_date_conversion: ["service_date#MM/dd/yyyy"]   # col#format syntax
  fields_boolean_conversion: ["adjusted_flag"]
  fields_null_check: ["claim_id", "member_id", "service_date"]
  fields_composite_unique_check: [["claim_id", "line_number"]]
```

#### `merge_util` vs `schema_changes` — These Are Separate Controls

| Control | Responsibility | Effect |
|---|---|---|
| `merge_util: yes` | Data loading strategy | Use MERGE with primary_keys for upsert |
| `merge_util: no` | Data loading strategy | Use APPEND (delta/adhoc) or OVERWRITE (full) |
| `schema_changes: yes` | Schema evolution | ADD COLUMN when source has new fields |
| `schema_changes: no` | Schema evolution | FAIL when source schema differs from target |

#### Date and Timestamp Format Syntax

The `#` separator encodes the format mask inline with the column name:

```yaml
fields_date_conversion: ["order_date#MM/dd/yyyy", "service_date"]
#                         ─────────┬──────────── ──────┬──────
#                         col name─┘  format mask      └─ AUTO (no format = AUTO)
```

The framework parses this in `sql_generator.sql` using `SPLIT_PART(entry, '#', 1)` for the column name and `SPLIT_PART(entry, '#', 2)` for the format. If no `#` is present, `AUTO` is used.

Generated SQL:
```sql
TRY_TO_DATE(order_date, 'MM/dd/yyyy')  AS order_date
TRY_TO_DATE(service_date)              AS service_date   -- AUTO
```

### Stage3 — Curated Outputs

```yaml
stage3:
  - name: "vw_claims_curated"
    type: "sql_query"
    value: >
      CREATE OR REPLACE VIEW CURATED.VW_CLAIMS_CURATED AS
      SELECT claim_id, member_id, billed_amount
      FROM STG.CLAIMS_TXT_STD
      WHERE COALESCE(delete_flag, 'N') <> 'D';
```

Each item must have `name`, `type`, and `value`. Items execute in list order. `type: sql_query` executes `value` as a SQL statement via `EXECUTE IMMEDIATE`.

---

## Schema Validation

### `configs/schema.yaml` — What Is Validated Statically

The schema contract validates YAML structure before Stage1 begins. It checks:

- Required sections (`stage1`, `stage2`, `stage3`)
- Allowed keys (additional properties are forbidden)
- Enum values (`load_type`, `file_type`, `merge_util`, `schema_changes`, `type`)
- Data types (string, boolean, integer, array, object)
- Array item formats (decimal conversion objects, composite unique groups)
- Stage3 list structure with `name`, `type`, `value`

**What is NOT validated statically:**
- Source column existence (columns are unknown before file arrival)
- Stage2 field references (validated at runtime after Stage1 schema discovery)

### Runtime Field-Reference Validation

After Stage1 discovers the file headers via `INFER_SCHEMA`, all Stage2 column references are validated against the discovered schema. This applies to:

- `primary_keys`
- `delete_column_name`
- `fields_long_conversion`, `fields_integer_conversion`, `fields_float_conversion`
- `fields_decimal_conversion`
- `fields_timestamp_conversion`, `fields_date_conversion`
- `fields_boolean_conversion`
- `fields_null_check`
- `fields_composite_unique_check`

If any referenced column does not exist in the discovered schema, the framework raises a `FIELD_REFERENCE_VALIDATION_FAILED` exception with a complete list of missing columns before Stage2 begins.

---

## Deployment Order

```sql
-- 1. Database and schemas (run as ACCOUNTADMIN or SYSADMIN)
-- Execute: ddl/00_database_and_schemas.sql

-- 2. File formats
-- Execute: ddl/01_file_formats.sql

-- 3. External stages (update storage integration and URLs first)
-- Execute: ddl/02_external_stages.sql

-- 4. Control tables
-- Execute: ddl/03_control_tables.sql

-- 5. Audit tables
-- Execute: ddl/04_audit_tables.sql

-- 6. Reject tables
-- Execute: ddl/05_reject_tables.sql

-- 7. RBAC (customize role assignments)
-- Execute: ddl/06_rbac.sql

-- 8. Python UDFs
-- Execute: framework/python_udfs/yaml_parser.sql
-- Execute: framework/python_udfs/schema_validator.sql

-- 9. Framework utilities
-- Execute: framework/utils/log_writer.sql
-- Execute: framework/utils/chunk_planner.sql
-- Execute: framework/utils/field_validator.sql
-- Execute: framework/utils/sql_generator.sql
-- Execute: framework/utils/merge_generator.sql

-- 10. Config loader and stage handlers
-- Execute: framework/config_loader.sql
-- Execute: framework/stage1_handler.sql
-- Execute: framework/stage2_handler.sql
-- Execute: framework/stage3_handler.sql

-- 11. Master runner
-- Execute: framework/master_runner.sql

-- 12. Upload YAML configs to internal stage
-- PUT file://configs/schema.yaml              @UTIL.STG_FW_CONFIGS/          AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://configs/datasets/claims_txt.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- 13. Tasks (optional)
-- Execute: orchestration/tasks_setup.sql
```

---

## Execution Examples

```sql
CALL UTIL.MASTER_RUNNER('claims_txt.yaml',        'datasets/claims_txt.yaml',        'file_ingestion');
CALL UTIL.MASTER_RUNNER('orders_csv.yaml',        'datasets/orders_csv.yaml',        'file_ingestion');
CALL UTIL.MASTER_RUNNER('customers_parquet.yaml', 'datasets/customers_parquet.yaml', 'file_ingestion');
```

---

## Orchestration Options

### Option 1: Snowflake Tasks (Recommended)

See `orchestration/tasks_setup.sql`. One Task per dataset calls a wrapper procedure that calls `MASTER_RUNNER`.

```sql
ALTER TASK UTIL.TASK_CLAIMS_TXT_DELTA RESUME;
EXECUTE TASK UTIL.TASK_CLAIMS_TXT_DELTA;   -- manual run / restart
```

### Option 2: Apache Airflow

See `orchestration/airflow/snowflake_ingestion_dag.py`. Thin DAGs that call `MASTER_RUNNER` via `SnowflakeOperator`. All business logic stays in Snowflake.

**Use Airflow when:** cross-system dependencies exist, centralised multi-platform monitoring is required, or dynamic DAG generation from a dataset registry is needed.

---

## Control and Audit Tables

| Table | Purpose |
|---|---|
| `CONTROL.PIPELINE_RUN_LOG` | One row per master execution |
| `CONTROL.BATCH_LOG` | One row per file batch |
| `CONTROL.FILE_LOG` | One row per source file |
| `CONTROL.CHUNK_LOG` | One row per chunk (delta restartability) |
| `CONTROL.YAML_EXECUTION_LOG` | Every YAML load and validation event |
| `CONTROL.SOURCE_SCHEMA_REGISTRY` | Discovered column headers from Stage1 |
| `CONTROL.FRAMEWORK_CONFIG` | Chunk size, retry limits, etc. |
| `AUDIT.MERGE_AUDIT_LOG` | MERGE row counts |
| `AUDIT.SCHEMA_DRIFT_LOG` | Schema evolution events |
| `AUDIT.STAGE3_ACTION_LOG` | Stage3 action execution records |
| `AUDIT.VALIDATION_LOG` | Field-level validation results |
| `REJECTS.REJECT_LOG` | Summary reject counts |
| `REJECTS.<dataset>_REJECT` | Dataset quarantine tables |

---

## Testing

```bash
pip install pyyaml jsonschema pytest
pytest tests/validate_yaml.py -v
# 48 tests — all passing
```