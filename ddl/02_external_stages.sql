-- =============================================================================
-- 02_external_stages.sql
-- Purpose : GCS external stage definitions for the GCP + Snowflake framework.
--           Each external stage points to a GCS bucket path and is backed by
--           a Snowflake GCS storage integration.
--
-- GCP-specific design notes
-- ─────────────────────────
-- 1. Storage integration uses STORAGE_PROVIDER = 'GCS', not 'S3'.
--    Snowflake creates a GCP service account; grant that SA the
--    "Storage Object Viewer" (or "Storage Admin" for archive writes)
--    IAM role on the GCS bucket.
--
-- 2. No AWS IAM role ARN is needed. GCS uses a GCP service account
--    email returned by DESC INTEGRATION.
--
-- 3. Snowpipe auto-ingest (configured separately in 07_snowpipe_gcs_pubsub.sql)
--    also references these same external stages.
--
-- 4. URL format is  gcs://<bucket>/<prefix>/  (not s3://)
--
-- IMPORTANT: Replace placeholder values (bucket names, project IDs) with
--            environment-specific values before deploying.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- -----------------------------------------------------------------------
-- GCS Storage Integration  (run once, requires ACCOUNTADMIN)
-- -----------------------------------------------------------------------
-- CREATE STORAGE INTEGRATION GCS_INGESTION_INT
--   TYPE                      = EXTERNAL_STAGE
--   STORAGE_PROVIDER          = 'GCS'
--   ENABLED                   = TRUE
--   STORAGE_ALLOWED_LOCATIONS = ('gcs://my-gcp-bucket/');

-- After creating the integration run:
--   DESC INTEGRATION GCS_INGESTION_INT;
-- The output includes a STORAGE_GCP_SERVICE_ACCOUNT field.
-- Grant that service account the IAM binding on your GCS bucket:
--
--   gcloud storage buckets add-iam-policy-binding gs://my-gcp-bucket \
--     --member="serviceAccount:<snowflake_sa>@<project>.iam.gserviceaccount.com" \
--     --role="roles/storage.objectAdmin"
--
-- For read-only source buckets use roles/storage.objectViewer.
-- For archive writes to a separate bucket use roles/storage.objectCreator.

-- -----------------------------------------------------------------------
-- Claims TXT source stage  (GCS)
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_CLAIMS_TXT
  STORAGE_INTEGRATION = GCS_INGESTION_INT
  URL                 = 'gcs://my-gcp-bucket/raw/claims/'
  FILE_FORMAT         = UTIL.FW_TXT_PIPE_FORMAT
  COMMENT             = 'GCS external stage for raw claims TXT files';

-- -----------------------------------------------------------------------
-- Orders CSV source stage  (GCS)
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_ORDERS_CSV
  STORAGE_INTEGRATION = GCS_INGESTION_INT
  URL                 = 'gcs://my-gcp-bucket/raw/orders/'
  FILE_FORMAT         = UTIL.FW_CSV_FORMAT
  COMMENT             = 'GCS external stage for raw orders CSV files';

-- -----------------------------------------------------------------------
-- Customers Parquet source stage  (GCS)
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_CUSTOMERS_PARQUET
  STORAGE_INTEGRATION = GCS_INGESTION_INT
  URL                 = 'gcs://my-gcp-bucket/raw/customers/'
  FILE_FORMAT         = UTIL.FW_PARQUET_FORMAT
  COMMENT             = 'GCS external stage for raw customers Parquet files';

-- -----------------------------------------------------------------------
-- Framework config stage  (internal – no cloud storage integration needed)
-- YAML configuration files are stored here via PUT command.
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_FW_CONFIGS
  COMMENT = 'Internal stage for framework YAML configuration files';

-- Upload YAML configs with:
--   PUT file://configs/datasets/claims_txt.yaml @UTIL.STG_FW_CONFIGS/datasets/ AUTO_COMPRESS=FALSE;
--   PUT file://configs/schema.yaml              @UTIL.STG_FW_CONFIGS/          AUTO_COMPRESS=FALSE;

-- -----------------------------------------------------------------------
-- Archive stage  (processed GCS files are moved here after load)
-- Requires Storage Admin IAM role on the archive bucket.
-- -----------------------------------------------------------------------
CREATE OR REPLACE STAGE UTIL.STG_ARCHIVE
  STORAGE_INTEGRATION = GCS_INGESTION_INT
  URL                 = 'gcs://my-gcp-bucket/archive/'
  COMMENT             = 'GCS archive destination for successfully processed source files';

-- -----------------------------------------------------------------------
-- Snowpipe notification stage  (used by 07_snowpipe_gcs_pubsub.sql)
-- Snowpipe reads files from the source stage above and lands them in RAW.
-- A separate Pub/Sub notification integration is required for auto-ingest.
-- See ddl/07_snowpipe_gcs_pubsub.sql for the NOTIFICATION INTEGRATION
-- and PIPE definitions.
-- -----------------------------------------------------------------------
-- Notification integration template (requires ACCOUNTADMIN):
--
-- CREATE NOTIFICATION INTEGRATION GCS_PUBSUB_INT
--   TYPE                 = QUEUE
--   NOTIFICATION_PROVIDER = GOOGLE_PUBSUB
--   ENABLED              = TRUE
--   GOOGLE_PUBSUB_SUBSCRIPTION_NAME =
--       'projects/my-gcp-project/subscriptions/snowflake-ingestion-sub';
--
-- After creation:
--   DESC INTEGRATION GCS_PUBSUB_INT;
-- Grant the returned GCP service account  Pub/Sub Subscriber  on the
-- subscription and  Storage Object Viewer  on the source bucket.
