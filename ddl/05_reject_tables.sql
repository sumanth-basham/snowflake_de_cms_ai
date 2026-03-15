-- =============================================================================
-- 05_reject_tables.sql
-- Purpose : Quarantine / reject tables for rows that fail Stage2 validation.
--
-- Design  : One generic reject table per dataset.
--           Reject rows carry all original string columns from Stage1
--           plus framework columns plus reject reason(s).
--
-- Tables
--   REJECTS.REJECT_LOG        – generic reject metadata indexed by run/batch
--   REJECTS.<DATASET>_REJECT  – dataset-specific quarantine tables (examples)
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA REJECTS;

-- -----------------------------------------------------------------------
-- REJECT_LOG  –  summary reject counts per run / batch
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REJECTS.REJECT_LOG (
    reject_log_id       VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    run_id              VARCHAR(64)    NOT NULL,
    batch_id            VARCHAR(64)    NOT NULL,
    chunk_id            VARCHAR(64),
    yaml_name           VARCHAR(255)   NOT NULL,
    source_table        VARCHAR(512)   NOT NULL             COMMENT 'Fully-qualified raw source table',
    reject_table        VARCHAR(512)   NOT NULL             COMMENT 'Fully-qualified reject quarantine table',
    reject_reason_code  VARCHAR(64)    NOT NULL             COMMENT 'NULL_CHECK | TYPE_CONVERSION | COMPOSITE_UNIQUE | BUSINESS_RULE',
    reject_count        NUMBER(18,0)   NOT NULL DEFAULT 0,
    logged_at           TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Summary reject counts – indexed by run/batch/reason for dashboards';

-- -----------------------------------------------------------------------
-- Template for dataset-specific reject quarantine tables.
-- The framework creates these dynamically if they do not exist.
-- Framework-generated DDL pattern:
--
--   CREATE TABLE IF NOT EXISTS REJECTS.<dataset>_REJECT (
--     reject_id            VARCHAR(64),
--     run_id               VARCHAR(64),
--     batch_id             VARCHAR(64),
--     chunk_id             VARCHAR(64),
--     source_file_name     VARCHAR(1024),
--     source_row_number    NUMBER(18,0),
--     reject_reason        VARCHAR(4096),
--     reject_reason_codes  VARIANT,        -- JSON array of reason codes
--     ... <all source payload columns as VARCHAR> ...,
--     batch_load_date      TIMESTAMP_NTZ,
--     rejected_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
--   );
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- Example: CLAIMS_TXT_REJECT
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REJECTS.CLAIMS_TXT_REJECT (
    reject_id            VARCHAR(64)    NOT NULL,
    run_id               VARCHAR(64)    NOT NULL,
    batch_id             VARCHAR(64)    NOT NULL,
    chunk_id             VARCHAR(64),
    yaml_name            VARCHAR(255)   NOT NULL,
    source_file_name     VARCHAR(1024),
    source_file_path     VARCHAR(2048),
    source_row_number    NUMBER(18,0),
    reject_reason        VARCHAR(4096)  NOT NULL             COMMENT 'Human-readable combined reject message',
    reject_reason_codes  VARIANT                             COMMENT 'JSON array: ["NULL_CHECK:claim_id","TYPE_CONVERSION:billed_amount"]',
    -- all source payload columns stored as VARCHAR (matching Stage1 raw schema)
    claim_id             VARCHAR(4096),
    member_id            VARCHAR(4096),
    provider_id          VARCHAR(4096),
    line_number          VARCHAR(4096),
    service_date         VARCHAR(4096),
    paid_date            VARCHAR(4096),
    billed_amount        VARCHAR(4096),
    paid_amount          VARCHAR(4096),
    adjusted_flag        VARCHAR(4096),
    allowed_ratio        VARCHAR(4096),
    ingestion_ts         VARCHAR(4096),
    last_update_ts       VARCHAR(4096),
    delete_flag          VARCHAR(4096),
    -- framework metadata
    batch_load_date      TIMESTAMP_NTZ,
    rejected_at          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Quarantine table for claims_txt rows that fail Stage2 validation';

-- -----------------------------------------------------------------------
-- Example: ORDERS_CSV_REJECT
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REJECTS.ORDERS_CSV_REJECT (
    reject_id            VARCHAR(64)    NOT NULL,
    run_id               VARCHAR(64)    NOT NULL,
    batch_id             VARCHAR(64)    NOT NULL,
    chunk_id             VARCHAR(64),
    yaml_name            VARCHAR(255)   NOT NULL,
    source_file_name     VARCHAR(1024),
    source_file_path     VARCHAR(2048),
    source_row_number    NUMBER(18,0),
    reject_reason        VARCHAR(4096)  NOT NULL,
    reject_reason_codes  VARIANT,
    order_id             VARCHAR(4096),
    customer_id          VARCHAR(4096),
    order_date           VARCHAR(4096),
    order_created_ts     VARCHAR(4096),
    order_amount         VARCHAR(4096),
    tax_amount           VARCHAR(4096),
    discount_pct         VARCHAR(4096),
    order_line_count     VARCHAR(4096),
    priority_order_flag  VARCHAR(4096),
    order_deleted_flag   VARCHAR(4096),
    batch_load_date      TIMESTAMP_NTZ,
    rejected_at          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Quarantine table for orders_csv rows that fail Stage2 validation';
