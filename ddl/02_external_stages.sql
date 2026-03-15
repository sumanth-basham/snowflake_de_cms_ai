-- =============================================================================
-- 02_external_stages.sql
-- Purpose : Template external stage definitions.
--           One external stage per cloud bucket / container.
--           Stages are referenced by COPY INTO in Stage1.
--
-- IMPORTANT: Replace placeholder values (storage_integration, url) with
--            environment-specific values before deploying.
--            Storage integrations must be created separately and authorised
--            by a cloud admin before these stages can be used.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- -----------------------------------------------------------------------
-- Storage integration (template – run once, requires ACCOUNTADMIN)
-- -----------------------------------------------------------------------
-- CREATE STORAGE INTEGRATION S3_INGESTION_INT
--   TYPE                      = EXTERNAL_STAGE
--   STORAGE_PROVIDER          = 'S3'
--   ENABLED                   = TRUE
--   STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::123456789012:role/SnowflakeIngestRole'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/raw/');

-- After creating the integration run:
--   DESC INTEGRATION S3_INGESTION_INT;
-- and add the Snowflake IAM user to the trust policy of the AWS role.

-- -----------------------------------------------------------------------
-- Claims TXT source stage
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_CLAIMS_TXT
  STORAGE_INTEGRATION = S3_INGESTION_INT
  URL                 = 's3://my-bucket/raw/claims/'
  FILE_FORMAT         = UTIL.FW_TXT_PIPE_FORMAT
  COMMENT             = 'External stage for raw claims TXT files';

-- -----------------------------------------------------------------------
-- Orders CSV source stage
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_ORDERS_CSV
  STORAGE_INTEGRATION = S3_INGESTION_INT
  URL                 = 's3://my-bucket/raw/orders/'
  FILE_FORMAT         = UTIL.FW_CSV_FORMAT
  COMMENT             = 'External stage for raw orders CSV files';

-- -----------------------------------------------------------------------
-- Customers Parquet source stage
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_CUSTOMERS_PARQUET
  STORAGE_INTEGRATION = S3_INGESTION_INT
  URL                 = 's3://my-bucket/raw/customers/'
  FILE_FORMAT         = UTIL.FW_PARQUET_FORMAT
  COMMENT             = 'External stage for raw customers Parquet files';

-- -----------------------------------------------------------------------
-- Framework config stage  (YAML files are stored here)
-- Internal stage – no cloud storage integration required
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_FW_CONFIGS
  COMMENT = 'Internal stage for framework YAML configuration files';

-- Upload a YAML config with:
--   PUT file://configs/datasets/claims_txt.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE;
--   PUT file://configs/schema.yaml              @UTIL.STG_FW_CONFIGS/          AUTO_COMPRESS=FALSE;

-- -----------------------------------------------------------------------
-- Archive stage  (processed files are moved here)
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_ARCHIVE
  STORAGE_INTEGRATION = S3_INGESTION_INT
  URL                 = 's3://my-bucket/archive/'
  COMMENT             = 'Archive destination for successfully processed source files';
