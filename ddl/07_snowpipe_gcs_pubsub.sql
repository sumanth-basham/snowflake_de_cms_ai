-- =============================================================================
-- 07_snowpipe_gcs_pubsub.sql
-- Purpose : Snowpipe auto-ingest setup for GCS-sourced files using
--           GCS Pub/Sub event notifications.
--
-- GCP + Snowflake integration flow
-- ─────────────────────────────────
--   GCS bucket
--     │  (file upload triggers)
--     ▼
--   GCS Pub/Sub topic  →  subscription
--     │  (Snowflake polls via notification integration)
--     ▼
--   Snowpipe  (COPY INTO RAW table, Stage1 landing)
--     │
--     ▼
--   RAW table  →  Stage2 (triggered by Snowflake Task)
--
-- When to use Snowpipe vs scheduled COPY INTO (opinionated guidance)
-- ──────────────────────────────────────────────────────────────────
-- USE SNOWPIPE when:
--   • Near-real-time file arrival is required (< 1 min latency)
--   • Files arrive irregularly or unpredictably throughout the day
--   • You want event-driven ingestion without polling overhead
--   • The source team pushes files as they are produced (streaming-like)
--   • Example: claims files arriving continuously from a hospital feed
--
-- USE SCHEDULED COPY INTO (Tasks) when:
--   • Files arrive on a known schedule (hourly, daily batch drops)
--   • Simpler operations model is preferred (no Pub/Sub infrastructure)
--   • Load volume justifies batching for Snowflake credit efficiency
--   • You need tight control over exactly when ingestion runs
--   • Example: daily overnight order file drops from an ERP system
--
-- RECOMMENDATION for this framework:
--   • Use Snowpipe for Stage1 raw landing of continuously arriving files
--   • Use scheduled Tasks for Stage2 and Stage3 (run after raw table
--     has been populated by Snowpipe, detected via table stream or row count)
--   • Snowpipe lands data to RAW; Stage2/Stage3 run on schedule
--   • This hybrid model avoids chaining all stages to Pub/Sub events
--
-- Important: Snowpipe does NOT purge source files automatically.
--            File lifecycle management (move to archive, delete after N days)
--            must be handled separately:
--            • Use GCS Object Lifecycle Management policies to delete/archive
--            • Or call REMOVE @stage/file from a post-load Task
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ============================================================================
-- PREREQUISITE: GCS Pub/Sub Setup (performed outside Snowflake)
-- ============================================================================
-- 1. Create a GCS Pub/Sub topic on the source bucket:
--      gsutil notification create \
--        -t projects/my-gcp-project/topics/snowflake-ingest-topic \
--        -f json \
--        gs://my-gcp-bucket
--
-- 2. Create a Pub/Sub subscription (pull):
--      gcloud pubsub subscriptions create snowflake-ingestion-sub \
--        --topic=projects/my-gcp-project/topics/snowflake-ingest-topic
--
-- 3. Grant the Snowflake notification SA subscriber access (after step 4):
--      gcloud pubsub subscriptions add-iam-policy-binding \
--        snowflake-ingestion-sub \
--        --member="serviceAccount:<snowflake_sa>@<project>.iam.gserviceaccount.com" \
--        --role="roles/pubsub.subscriber"
-- ============================================================================

-- ============================================================================
-- STEP 1: Notification Integration  (requires ACCOUNTADMIN)
-- Run once per GCP project / Pub/Sub subscription.
-- ============================================================================
-- CREATE NOTIFICATION INTEGRATION GCS_PUBSUB_INT
--   TYPE                          = QUEUE
--   NOTIFICATION_PROVIDER         = GOOGLE_PUBSUB
--   ENABLED                       = TRUE
--   GOOGLE_PUBSUB_SUBSCRIPTION_NAME =
--       'projects/my-gcp-project/subscriptions/snowflake-ingestion-sub';

-- After creation, retrieve the GCP service account Snowflake uses:
--   DESC INTEGRATION GCS_PUBSUB_INT;
-- Look for GCS_PUBSUB_SERVICE_ACCOUNT in the output and grant it
-- roles/pubsub.subscriber on the subscription (see step 3 above).

-- ============================================================================
-- STEP 2: Snowpipe Definitions
-- One pipe per dataset / file type.
-- Each pipe lands files into the corresponding RAW table.
-- The COPY INTO here uses the same named file formats as the rest of
-- the framework, ensuring consistent parsing behaviour.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Pipe: Claims TXT  (pipe-delimited, delta load pattern)
-- ---------------------------------------------------------------------------
-- Auto-ingest = TRUE enables event-driven loading via Pub/Sub.
-- The COPY INTO statement here is the Stage1 landing pattern:
--   all columns land as VARCHAR through the named file format.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PIPE UTIL.PIPE_CLAIMS_TXT_RAW
  AUTO_INGEST       = TRUE
  INTEGRATION       = GCS_PUBSUB_INT
  COMMENT           = 'Snowpipe: auto-ingest claims TXT files from GCS into RAW.CLAIMS_TXT_RAW'
AS
COPY INTO RAW.CLAIMS_TXT_RAW
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,   -- positional source columns (VARCHAR by format)
        $11, $12, $13,
        -- framework metadata stamped at load time
        UUID_STRING()                      AS run_id,
        UUID_STRING()                      AS batch_id,
        NULL                               AS chunk_id,
        METADATA$FILENAME                  AS source_file_name,
        '@UTIL.STG_CLAIMS_TXT'             AS source_file_path,
        'claims_txt.yaml'                  AS yaml_name,
        'file_ingestion'                   AS process_type,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS batch_load_date
    FROM @UTIL.STG_CLAIMS_TXT
)
FILE_FORMAT = (FORMAT_NAME = 'INGESTION_FW.UTIL.FW_TXT_PIPE_FORMAT')
PATTERN     = '.*\\.txt'
ON_ERROR    = CONTINUE;

-- Retrieve Snowpipe notification channel for Pub/Sub:
--   SHOW PIPES LIKE 'PIPE_CLAIMS_TXT_RAW';
-- The notification_channel column contains the GCS Pub/Sub endpoint.
-- Configure your GCS notification to push to this endpoint.

-- ---------------------------------------------------------------------------
-- Pipe: Orders CSV  (comma-delimited, full load pattern)
-- ---------------------------------------------------------------------------
-- Note: For full loads, Snowpipe is less appropriate than scheduled COPY INTO
-- because full loads typically truncate+reload on schedule.
-- Snowpipe is shown here as a landing mechanism; Stage2 handles the
-- TRUNCATE+INSERT logic when load_type=full.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PIPE UTIL.PIPE_ORDERS_CSV_RAW
  AUTO_INGEST       = TRUE
  INTEGRATION       = GCS_PUBSUB_INT
  COMMENT           = 'Snowpipe: auto-ingest orders CSV files from GCS into RAW.ORDERS_CSV_RAW'
AS
COPY INTO RAW.ORDERS_CSV_RAW
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        UUID_STRING()                      AS run_id,
        UUID_STRING()                      AS batch_id,
        NULL                               AS chunk_id,
        METADATA$FILENAME                  AS source_file_name,
        '@UTIL.STG_ORDERS_CSV'             AS source_file_path,
        'orders_csv.yaml'                  AS yaml_name,
        'file_ingestion'                   AS process_type,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS batch_load_date
    FROM @UTIL.STG_ORDERS_CSV
)
FILE_FORMAT = (FORMAT_NAME = 'INGESTION_FW.UTIL.FW_CSV_FORMAT')
PATTERN     = '.*\\.csv'
ON_ERROR    = CONTINUE;

-- ---------------------------------------------------------------------------
-- Pipe: Customers Parquet  (Snappy compressed, delta load)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PIPE UTIL.PIPE_CUSTOMERS_PARQUET_RAW
  AUTO_INGEST       = TRUE
  INTEGRATION       = GCS_PUBSUB_INT
  COMMENT           = 'Snowpipe: auto-ingest customers Parquet files from GCS into RAW.CUSTOMERS_PARQUET_RAW'
AS
COPY INTO RAW.CUSTOMERS_PARQUET_RAW
FROM (
    SELECT
        -- Parquet: columns accessed via $1:<column_name>::STRING to enforce VARCHAR landing
        $1:customer_id::STRING,
        $1:email::STRING,
        $1:age::STRING,
        $1:tenure_months::STRING,
        $1:annual_income::STRING,
        $1:credit_limit::STRING,
        $1:credit_score_normalized::STRING,
        $1:date_of_birth::STRING,
        $1:enrollment_date::STRING,
        $1:is_active::STRING,
        $1:is_deleted::STRING,
        $1:email_opt_in::STRING,
        $1:created_at::STRING,
        $1:updated_at::STRING,
        -- framework metadata
        UUID_STRING()                         AS run_id,
        UUID_STRING()                         AS batch_id,
        NULL                                  AS chunk_id,
        METADATA$FILENAME                     AS source_file_name,
        '@UTIL.STG_CUSTOMERS_PARQUET'         AS source_file_path,
        'customers_parquet.yaml'              AS yaml_name,
        'file_ingestion'                      AS process_type,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ    AS batch_load_date
    FROM @UTIL.STG_CUSTOMERS_PARQUET
)
FILE_FORMAT = (FORMAT_NAME = 'INGESTION_FW.UTIL.FW_PARQUET_FORMAT')
PATTERN     = '.*\\.parquet'
ON_ERROR    = CONTINUE;

-- ============================================================================
-- PIPE MANAGEMENT COMMANDS
-- ============================================================================

-- Pause a pipe (during maintenance or deployments):
--   ALTER PIPE UTIL.PIPE_CLAIMS_TXT_RAW    PAUSE;
--   ALTER PIPE UTIL.PIPE_ORDERS_CSV_RAW    PAUSE;
--   ALTER PIPE UTIL.PIPE_CUSTOMERS_PARQUET_RAW PAUSE;

-- Resume:
--   ALTER PIPE UTIL.PIPE_CLAIMS_TXT_RAW    RESUME;

-- Check pipe status and pending file count:
--   SELECT SYSTEM$PIPE_STATUS('INGESTION_FW.UTIL.PIPE_CLAIMS_TXT_RAW');

-- Refresh pipe (force re-scan of stage for missed files):
--   ALTER PIPE UTIL.PIPE_CLAIMS_TXT_RAW REFRESH;

-- ============================================================================
-- GCS FILE LIFECYCLE MANAGEMENT
-- ============================================================================
-- Snowpipe does NOT purge source files. Use one of these approaches:
--
-- Option A: GCS Object Lifecycle Management (recommended for simplicity)
--   Add a lifecycle rule to the GCS bucket to delete or archive objects
--   older than N days:
--     gsutil lifecycle set lifecycle.json gs://my-gcp-bucket
--   lifecycle.json:
--     {
--       "rule": [{
--         "action": {"type": "Delete"},
--         "condition": {"age": 7, "matchesPrefix": ["raw/"]}
--       }]
--     }
--
-- Option B: Post-load Task removes files from stage
--   After Stage2 completes successfully, run:
--     REMOVE @UTIL.STG_CLAIMS_TXT/<file_name>;
--   The framework's FILE_LOG records processed file names for this purpose.
--
-- Option C: Move to archive bucket
--   Use the Snowflake COPY FILES command (preview feature) or a GCS
--   Object Transfer job triggered by a Cloud Function on Pub/Sub ack.

-- ============================================================================
-- SNOWPIPE STATUS TABLE  (framework tracks pipe events)
-- ============================================================================
CREATE TABLE IF NOT EXISTS CONTROL.SNOWPIPE_EVENT_LOG (
    pipe_event_id    VARCHAR(64)    NOT NULL             COMMENT 'UUID',
    pipe_name        VARCHAR(255)   NOT NULL             COMMENT 'Fully-qualified pipe name',
    dataset_name     VARCHAR(255)   NOT NULL             COMMENT 'Dataset YAML name',
    file_name        VARCHAR(1024)  NOT NULL             COMMENT 'File that triggered the pipe',
    pipe_status      VARCHAR(32)    NOT NULL             COMMENT 'LOADED | LOAD_FAILED | PARTIALLY_LOADED | COPY_SKIPPED',
    rows_loaded      NUMBER(18,0)   DEFAULT 0,
    rows_failed      NUMBER(18,0)   DEFAULT 0,
    first_error_msg  VARCHAR(4096),
    pipe_received_at TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    stage2_triggered BOOLEAN        NOT NULL DEFAULT FALSE
                                             COMMENT 'TRUE once Stage2 Task has been triggered for this file'
)
COMMENT = 'Records every file event processed by Snowpipe (populated via COPY_HISTORY or pipe status polling)';

-- ============================================================================
-- TASK: Snowpipe → Stage2 bridge
-- ============================================================================
-- After Snowpipe lands files in RAW, a scheduled Task detects new rows and
-- triggers Stage2 + Stage3 via MASTER_RUNNER.
--
-- The Task checks for RAW rows where batch_load_date > last processed time,
-- then calls MASTER_RUNNER which handles Stage2/Stage3 only
-- (Stage1 is already complete via Snowpipe).
--
-- Pattern: set p_stage1_complete = TRUE in MASTER_RUNNER to skip Stage1.
-- This is handled by the Snowpipe-aware Task wrappers in tasks_setup.sql.
-- ============================================================================
CREATE OR REPLACE PROCEDURE UTIL.RUN_POST_SNOWPIPE_CLAIMS()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Post-Snowpipe Task: runs Stage2+Stage3 for claims after Snowpipe lands files in RAW'
AS
$$
DECLARE
    v_pending_rows NUMBER;
BEGIN
    -- Check for new rows in RAW not yet processed by Stage2
    SELECT COUNT(*) INTO v_pending_rows
    FROM RAW.CLAIMS_TXT_RAW
    WHERE batch_load_date >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
      AND run_id NOT IN (
          SELECT DISTINCT run_id FROM CONTROL.PIPELINE_RUN_LOG
          WHERE yaml_name = 'claims_txt.yaml'
            AND stage2_status = 'SUCCESS'
      );

    IF :v_pending_rows > 0 THEN
        -- Trigger Stage2+Stage3 only (Stage1 already done by Snowpipe)
        CALL UTIL.MASTER_RUNNER_SP(
            'claims_txt.yaml',
            'datasets/claims_txt.yaml',
            'file_ingestion',
            TRUE   -- p_snowpipe_mode: skip Stage1
        );
    END IF;

    RETURN 'CHECKED: ' || :v_pending_rows::VARCHAR || ' pending rows';
END;
$$;

-- Task: check every 15 minutes for Snowpipe-loaded claims files
CREATE OR REPLACE TASK UTIL.TASK_POST_SNOWPIPE_CLAIMS
    WAREHOUSE   = FW_WH
    SCHEDULE    = 'USING CRON */15 * * * * UTC'
    COMMENT     = 'Post-Snowpipe Stage2+Stage3 trigger for claims TXT files'
AS
    CALL UTIL.RUN_POST_SNOWPIPE_CLAIMS();

-- ALTER TASK UTIL.TASK_POST_SNOWPIPE_CLAIMS RESUME;
