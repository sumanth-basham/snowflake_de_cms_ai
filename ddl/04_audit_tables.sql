-- =============================================================================
-- 04_audit_tables.sql
-- Purpose : Detailed audit tables for merge operations, schema evolution,
--           and Stage3 action logs.
--
-- Tables
--   AUDIT.MERGE_AUDIT_LOG      – MERGE statement row-level outcome tracking
--   AUDIT.SCHEMA_DRIFT_LOG     – schema drift / evolution event log
--   AUDIT.STAGE3_ACTION_LOG    – Stage3 action execution records
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA AUDIT;

-- -----------------------------------------------------------------------
-- MERGE_AUDIT_LOG
-- Records rows_inserted, rows_updated, rows_deleted from every MERGE run.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.MERGE_AUDIT_LOG (
    merge_audit_id      VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    target_schema       VARCHAR(128)   NOT NULL,
    target_table        VARCHAR(255)   NOT NULL,
    merge_strategy      VARCHAR(32)    NOT NULL             COMMENT 'MERGE_UPSERT | APPEND | OVERWRITE',
    rows_inserted       NUMBER(18,0)   DEFAULT 0,
    rows_updated        NUMBER(18,0)   DEFAULT 0,
    rows_deleted        NUMBER(18,0)   DEFAULT 0,
    rows_unchanged      NUMBER(18,0)   DEFAULT 0,
    primary_keys        VARCHAR(2048)                       COMMENT 'Comma-separated PK columns used in MERGE',
    merge_sql_hash      VARCHAR(64)                         COMMENT 'SHA-256 of the generated MERGE SQL (for change detection)',
    executed_at         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Row-count outcomes from every MERGE / APPEND / OVERWRITE operation';

-- -----------------------------------------------------------------------
-- SCHEMA_DRIFT_LOG
-- Records every time a schema mismatch or evolution event is detected.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.SCHEMA_DRIFT_LOG (
    drift_id            VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    target_schema       VARCHAR(128)   NOT NULL,
    target_table        VARCHAR(255)   NOT NULL,
    drift_type          VARCHAR(32)    NOT NULL             COMMENT 'NEW_COLUMN | DROPPED_COLUMN | TYPE_CHANGE | COLUMN_ORDER_CHANGE',
    column_name         VARCHAR(255)                        COMMENT 'Affected column',
    source_type         VARCHAR(128)                        COMMENT 'Data type in source / raw table',
    target_type         VARCHAR(128)                        COMMENT 'Data type in target STG table',
    resolution          VARCHAR(32)    NOT NULL             COMMENT 'ADDED | FAILED | IGNORED',
    resolution_detail   VARCHAR(2048)                       COMMENT 'DDL executed or failure reason',
    schema_changes_flag VARCHAR(3)     NOT NULL             COMMENT 'yes | no (from YAML schema_changes)',
    detected_at         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Every schema drift / evolution event detected during Stage2';

-- -----------------------------------------------------------------------
-- STAGE3_ACTION_LOG
-- Records execution of every Stage3 name/type/value action.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.STAGE3_ACTION_LOG (
    action_log_id       VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    action_sequence     NUMBER(5,0)    NOT NULL             COMMENT 'Order index in stage3 list (1-based)',
    action_name         VARCHAR(255)   NOT NULL             COMMENT 'name field from stage3 item',
    action_type         VARCHAR(64)    NOT NULL             COMMENT 'type field (sql_query, etc.)',
    sql_executed        VARCHAR(16384)                      COMMENT 'Actual SQL sent to Snowflake',
    status              VARCHAR(16)    NOT NULL DEFAULT 'RUNNING',
    rows_affected       NUMBER(18,0)                        COMMENT 'Rows affected by the SQL (if applicable)',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ,
    error_message       VARCHAR(4096)
)
COMMENT = 'Execution records for every Stage3 post-processing action';

-- -----------------------------------------------------------------------
-- VALIDATION_LOG
-- Field-level validation failures (null checks, composite unique checks).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AUDIT.VALIDATION_LOG (
    validation_log_id   VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    validation_type     VARCHAR(64)    NOT NULL             COMMENT 'NULL_CHECK | COMPOSITE_UNIQUE | FIELD_REFERENCE | TYPE_CONVERSION',
    field_name          VARCHAR(255)                        COMMENT 'Column(s) involved',
    validation_result   VARCHAR(16)    NOT NULL             COMMENT 'PASSED | FAILED',
    failure_count       NUMBER(18,0)   DEFAULT 0            COMMENT 'Number of rows failing this check',
    error_detail        VARCHAR(2048),
    evaluated_at        TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Field-level and row-level validation outcomes from Stage2';
