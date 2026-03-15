-- =============================================================================
-- 00_database_and_schemas.sql
-- Purpose : Create the database, virtual warehouse, and all schemas used
--           by the metadata-driven file ingestion framework.
-- Run once per environment (DEV / QA / PROD).
-- =============================================================================

-- -----------------------------------------------------------------------
-- Database
-- -----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS INGESTION_FW
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Metadata-driven file ingestion framework database';

USE DATABASE INGESTION_FW;

-- -----------------------------------------------------------------------
-- Virtual Warehouse  (adjust size / auto-suspend per environment)
-- -----------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS FW_WH
  WAREHOUSE_SIZE       = 'SMALL'
  AUTO_SUSPEND         = 120
  AUTO_RESUME          = TRUE
  INITIALLY_SUSPENDED  = TRUE
  COMMENT = 'Warehouse for file ingestion framework execution';

-- -----------------------------------------------------------------------
-- Schemas
-- -----------------------------------------------------------------------

-- RAW     : Stage1 raw landing tables – all payload columns as VARCHAR
CREATE SCHEMA IF NOT EXISTS RAW
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Stage1 – raw file landing (all payload columns as STRING)';

-- STG     : Stage2 standardised tables – typed, validated, merged
CREATE SCHEMA IF NOT EXISTS STG
  DATA_RETENTION_TIME_IN_DAYS = 14
  COMMENT = 'Stage2 – standardised and validated data';

-- CURATED : Stage3 curated views, aggregates, and published objects
CREATE SCHEMA IF NOT EXISTS CURATED
  DATA_RETENTION_TIME_IN_DAYS = 30
  COMMENT = 'Stage3 – curated, business-ready outputs';

-- CONTROL : Metadata / control tables driving orchestration
CREATE SCHEMA IF NOT EXISTS CONTROL
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'Framework control and metadata tables';

-- AUDIT   : Detailed audit trail of every framework operation
CREATE SCHEMA IF NOT EXISTS AUDIT
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'Framework audit tables (merge audit, schema drift, YAML log)';

-- REJECTS : Quarantine tables for bad / rejected rows
CREATE SCHEMA IF NOT EXISTS REJECTS
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'Reject / quarantine tables for invalid rows';

-- UTIL    : Shared framework utilities (UDFs, helper procedures)
CREATE SCHEMA IF NOT EXISTS UTIL
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Framework utility UDFs and stored procedures';
