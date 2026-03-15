-- =============================================================================
-- 03_control_tables.sql
-- Purpose : Framework control and metadata tables.
--           These tables drive orchestration, state management, and restarts.
--
-- Tables
--   CONTROL.PIPELINE_RUN_LOG   – one row per master execution (run_id)
--   CONTROL.BATCH_LOG          – one row per file batch within a run
--   CONTROL.FILE_LOG           – one row per individual file processed
--   CONTROL.CHUNK_LOG          – one row per chunk for delta loads
--   CONTROL.YAML_EXECUTION_LOG – one row per YAML loaded and validated
--   CONTROL.TASK_EXECUTION_LOG – one row per Snowflake Task execution
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA CONTROL;

-- -----------------------------------------------------------------------
-- PIPELINE_RUN_LOG
-- One row per invocation of the master runner stored procedure.
-- run_id is the primary correlation key across all log tables.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.PIPELINE_RUN_LOG (
    run_id              VARCHAR(64)    NOT NULL             COMMENT 'UUID generated at start of each master execution',
    yaml_name           VARCHAR(255)   NOT NULL             COMMENT 'Dataset YAML file name (e.g. claims_txt.yaml)',
    yaml_file_path      VARCHAR(1024)  NOT NULL             COMMENT 'Full stage path to the YAML file',
    process_type        VARCHAR(64)    NOT NULL             COMMENT 'Process type (file_ingestion)',
    load_type           VARCHAR(16)                         COMMENT 'full | delta | adhoc',
    status              VARCHAR(16)    NOT NULL DEFAULT 'RUNNING'
                                                            COMMENT 'RUNNING | SUCCESS | FAILED | PARTIAL',
    stage1_status       VARCHAR(16)                         COMMENT 'Stage1 exit status',
    stage2_status       VARCHAR(16)                         COMMENT 'Stage2 exit status',
    stage3_status       VARCHAR(16)                         COMMENT 'Stage3 exit status',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ                       COMMENT 'Set when run completes or fails',
    duration_seconds    NUMBER(10,2)                        COMMENT 'Calculated as end_ts - start_ts',
    rows_loaded_stage1  NUMBER(18,0)   DEFAULT 0            COMMENT 'Total rows landed in raw table',
    rows_valid_stage2   NUMBER(18,0)   DEFAULT 0            COMMENT 'Rows passing Stage2 validation',
    rows_rejected_stage2 NUMBER(18,0)  DEFAULT 0            COMMENT 'Rows quarantined in reject table',
    error_message       VARCHAR(4096)                       COMMENT 'First fatal error message',
    created_at          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'One row per master pipeline execution (run_id)';

-- -----------------------------------------------------------------------
-- BATCH_LOG
-- Tracks individual file batches within a run.
-- A batch groups files discovered in one execution window.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.BATCH_LOG (
    batch_id            VARCHAR(64)    NOT NULL             COMMENT 'UUID for this batch',
    run_id              VARCHAR(64)    NOT NULL             COMMENT 'FK to PIPELINE_RUN_LOG',
    yaml_name           VARCHAR(255)   NOT NULL,
    batch_sequence      NUMBER(10,0)                        COMMENT 'Order within the run',
    files_discovered    NUMBER(10,0)   DEFAULT 0            COMMENT 'Number of files found in source path',
    files_processed     NUMBER(10,0)   DEFAULT 0,
    files_failed        NUMBER(10,0)   DEFAULT 0,
    rows_loaded         NUMBER(18,0)   DEFAULT 0,
    status              VARCHAR(16)    NOT NULL DEFAULT 'RUNNING',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ,
    error_message       VARCHAR(4096)
)
COMMENT = 'One row per file batch within a pipeline run';

-- -----------------------------------------------------------------------
-- FILE_LOG
-- Tracks processing status of every individual source file.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.FILE_LOG (
    file_log_id         VARCHAR(64)    NOT NULL             COMMENT 'UUID for this file record',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    source_file_name    VARCHAR(1024)  NOT NULL             COMMENT 'File name only',
    source_file_path    VARCHAR(2048)  NOT NULL             COMMENT 'Full stage path',
    file_size_bytes     NUMBER(18,0)                        COMMENT 'File size in bytes',
    file_last_modified  TIMESTAMP_NTZ                       COMMENT 'Cloud storage last-modified timestamp',
    rows_loaded         NUMBER(18,0)   DEFAULT 0,
    status              VARCHAR(16)    NOT NULL DEFAULT 'PENDING'
                                                            COMMENT 'PENDING | LOADING | SUCCESS | FAILED | SKIPPED',
    skip_reason         VARCHAR(1024)                       COMMENT 'Reason when status = SKIPPED (zero-byte, duplicate, etc.)',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ,
    error_message       VARCHAR(4096)
)
COMMENT = 'One row per source file processed';

-- -----------------------------------------------------------------------
-- CHUNK_LOG
-- Delta-load chunking – each file is split into N chunks for scalability.
-- Framework manages chunk size; YAML does not need to configure chunking.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.CHUNK_LOG (
    chunk_id            VARCHAR(64)    NOT NULL             COMMENT 'UUID for this chunk',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    file_log_id         VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    chunk_sequence      NUMBER(10,0)   NOT NULL             COMMENT 'Chunk index within file (1-based)',
    chunk_offset        NUMBER(18,0)   NOT NULL DEFAULT 0   COMMENT 'Row offset from start of file',
    chunk_size          NUMBER(10,0)   NOT NULL             COMMENT 'Target rows per chunk',
    rows_in_chunk       NUMBER(18,0)   DEFAULT 0,
    rows_valid          NUMBER(18,0)   DEFAULT 0,
    rows_rejected       NUMBER(18,0)   DEFAULT 0,
    status              VARCHAR(16)    NOT NULL DEFAULT 'PENDING'
                                                            COMMENT 'PENDING | PROCESSING | SUCCESS | FAILED',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ,
    error_message       VARCHAR(4096),
    is_restartable      BOOLEAN        NOT NULL DEFAULT TRUE COMMENT 'Can this chunk be retried without data duplication?'
)
COMMENT = 'One row per chunk in a delta file – supports restartable partial loads';

-- -----------------------------------------------------------------------
-- YAML_EXECUTION_LOG
-- Records every YAML load, parse, and schema-validation event.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.YAML_EXECUTION_LOG (
    yaml_exec_id        VARCHAR(64)    NOT NULL             COMMENT 'UUID for this YAML validation event',
    run_id              VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    yaml_file_path      VARCHAR(1024)  NOT NULL,
    yaml_content        VARIANT                             COMMENT 'Parsed YAML stored as JSON VARIANT',
    schema_validation   VARCHAR(16)    NOT NULL DEFAULT 'PENDING'
                                                            COMMENT 'PENDING | PASSED | FAILED',
    validation_errors   VARIANT                             COMMENT 'Array of jsonschema validation error messages',
    loaded_at           TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Audit trail for every YAML file loaded and validated by the framework';

-- -----------------------------------------------------------------------
-- TASK_EXECUTION_LOG
-- Snowflake Tasks write execution records here.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.TASK_EXECUTION_LOG (
    task_exec_id        VARCHAR(64)    NOT NULL             COMMENT 'UUID for this task execution',
    task_name           VARCHAR(255)   NOT NULL             COMMENT 'Snowflake Task name',
    run_id              VARCHAR(64)                         COMMENT 'Linked pipeline run_id (if available)',
    yaml_name           VARCHAR(255),
    trigger_type        VARCHAR(32)                         COMMENT 'SCHEDULED | MANUAL | STREAM',
    status              VARCHAR(16)    NOT NULL DEFAULT 'RUNNING',
    start_ts            TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_ts              TIMESTAMP_NTZ,
    error_message       VARCHAR(4096)
)
COMMENT = 'Snowflake Task execution records';

-- -----------------------------------------------------------------------
-- Source schema discovery registry
-- Discovered column headers are persisted here for runtime field validation
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.SOURCE_SCHEMA_REGISTRY (
    schema_reg_id       VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    yaml_name           VARCHAR(255)   NOT NULL,
    raw_table           VARCHAR(512)   NOT NULL             COMMENT 'Fully-qualified raw table name',
    discovered_columns  VARIANT        NOT NULL             COMMENT 'Array of column names discovered from file header',
    file_type           VARCHAR(16)    NOT NULL,
    discovered_at       TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Source column headers discovered at Stage1 – used for Stage2 field validation';
