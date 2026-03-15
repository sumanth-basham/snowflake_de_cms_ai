-- =============================================================================
-- tasks_setup.sql
-- Purpose : Snowflake Tasks-based orchestration for the GCP + Snowflake
--           ingestion framework.
--
-- GCP platform notes
-- ──────────────────
-- • Snowpipe auto-ingest handles Stage1 raw landing for event-driven datasets.
--   The post-Snowpipe Tasks below invoke MASTER_RUNNER_SP with
--   p_snowpipe_mode = TRUE to run Stage2 + Stage3 only.
--
-- • For scheduled full loads (orders), Tasks run MASTER_RUNNER_SP with
--   p_snowpipe_mode = FALSE to execute all three stages via COPY INTO.
--
-- • All Tasks reference UTIL.MASTER_RUNNER_SP (Snowpark Python SP).
--   The original UTIL.MASTER_RUNNER (SQL SP) is still available as a fallback.
--
-- Design
-- ──────
--   One TASK per dataset per schedule.
--   Each task calls UTIL.MASTER_RUNNER_SP with hardcoded yaml parameters.
--   Tasks can be chained (predecessor → successor) for dependency ordering.
--
-- Task anatomy
--   Root task    : triggered on schedule or manually
--   Child tasks  : triggered on parent success (AFTER TASK_NAME)
--
-- Restart handling
--   On failure, Tasks can be retried manually:
--     EXECUTE TASK <task_name>;
--   The master runner and chunk planner handle idempotency.
--
-- Monitoring
--   SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY  – last 14 days of task runs
--   CONTROL.TASK_EXECUTION_LOG            – framework-level task records
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ---------------------------------------------------------------------------
-- Helper wrapper procedure for task invocation
-- Tasks cannot pass dynamic parameters; use one thin wrapper per dataset.
-- ---------------------------------------------------------------------------

-- Claims TXT delta wrapper
CREATE OR REPLACE PROCEDURE UTIL.RUN_CLAIMS_TXT()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Task wrapper: runs claims_txt.yaml pipeline'
AS
$$
DECLARE
    v_task_exec_id STRING;
BEGIN
    v_task_exec_id := UUID_STRING();

    INSERT INTO CONTROL.TASK_EXECUTION_LOG (
        task_exec_id, task_name, yaml_name, trigger_type, status, start_ts
    )
    VALUES (v_task_exec_id, 'TASK_CLAIMS_TXT_DELTA', 'claims_txt.yaml', 'SCHEDULED', 'RUNNING', CURRENT_TIMESTAMP());

    CALL UTIL.MASTER_RUNNER_SP(
        'claims_txt.yaml',
        'datasets/claims_txt.yaml',
        'file_ingestion',
        TRUE   -- p_snowpipe_mode: Stage1 already landed by Snowpipe; run Stage2+Stage3 only
    );

    UPDATE CONTROL.TASK_EXECUTION_LOG
    SET status = 'SUCCESS', end_ts = CURRENT_TIMESTAMP()
    WHERE task_exec_id = v_task_exec_id;

    RETURN 'SUCCESS';
EXCEPTION
    WHEN OTHER THEN
        UPDATE CONTROL.TASK_EXECUTION_LOG
        SET status = 'FAILED', end_ts = CURRENT_TIMESTAMP(), error_message = SQLERRM
        WHERE task_exec_id = v_task_exec_id;
        RAISE;
END;
$$;

-- Orders CSV full-load wrapper
CREATE OR REPLACE PROCEDURE UTIL.RUN_ORDERS_CSV()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Task wrapper: runs orders_csv.yaml pipeline'
AS
$$
BEGIN
    INSERT INTO CONTROL.TASK_EXECUTION_LOG (
        task_exec_id, task_name, yaml_name, trigger_type, status, start_ts
    )
    VALUES (UUID_STRING(), 'TASK_ORDERS_CSV_FULL', 'orders_csv.yaml', 'SCHEDULED', 'RUNNING', CURRENT_TIMESTAMP());

    CALL UTIL.MASTER_RUNNER_SP(
        'orders_csv.yaml',
        'datasets/orders_csv.yaml',
        'file_ingestion',
        FALSE  -- p_snowpipe_mode: scheduled full load; run all three stages via COPY INTO
    );

    UPDATE CONTROL.TASK_EXECUTION_LOG
    SET status = 'SUCCESS', end_ts = CURRENT_TIMESTAMP()
    WHERE task_name = 'TASK_ORDERS_CSV_FULL' AND status = 'RUNNING';

    RETURN 'SUCCESS';
EXCEPTION
    WHEN OTHER THEN
        UPDATE CONTROL.TASK_EXECUTION_LOG
        SET status = 'FAILED', end_ts = CURRENT_TIMESTAMP(), error_message = SQLERRM
        WHERE task_name = 'TASK_ORDERS_CSV_FULL' AND status = 'RUNNING';
        RAISE;
END;
$$;

-- Customers Parquet delta wrapper
CREATE OR REPLACE PROCEDURE UTIL.RUN_CUSTOMERS_PARQUET()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Task wrapper: runs customers_parquet.yaml pipeline'
AS
$$
BEGIN
    INSERT INTO CONTROL.TASK_EXECUTION_LOG (
        task_exec_id, task_name, yaml_name, trigger_type, status, start_ts
    )
    VALUES (UUID_STRING(), 'TASK_CUSTOMERS_PARQUET_DELTA', 'customers_parquet.yaml', 'SCHEDULED', 'RUNNING', CURRENT_TIMESTAMP());

    CALL UTIL.MASTER_RUNNER_SP(
        'customers_parquet.yaml',
        'datasets/customers_parquet.yaml',
        'file_ingestion',
        TRUE   -- p_snowpipe_mode: Stage1 already landed by Snowpipe; run Stage2+Stage3 only
    );

    UPDATE CONTROL.TASK_EXECUTION_LOG
    SET status = 'SUCCESS', end_ts = CURRENT_TIMESTAMP()
    WHERE task_name = 'TASK_CUSTOMERS_PARQUET_DELTA' AND status = 'RUNNING';

    RETURN 'SUCCESS';
EXCEPTION
    WHEN OTHER THEN
        UPDATE CONTROL.TASK_EXECUTION_LOG
        SET status = 'FAILED', end_ts = CURRENT_TIMESTAMP(), error_message = SQLERRM
        WHERE task_name = 'TASK_CUSTOMERS_PARQUET_DELTA' AND status = 'RUNNING';
        RAISE;
END;
$$;

-- ===========================================================================
-- TASKS  (one per dataset)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Claims TXT  –  runs every hour, delta load
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK UTIL.TASK_CLAIMS_TXT_DELTA
    WAREHOUSE   = FW_WH
    SCHEDULE    = 'USING CRON 0 * * * * UTC'   -- every hour at :00
    COMMENT     = 'Hourly delta ingestion for claims TXT files'
AS
    CALL UTIL.RUN_CLAIMS_TXT();

-- ---------------------------------------------------------------------------
-- Orders CSV  –  runs daily at 02:00 UTC, full load
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK UTIL.TASK_ORDERS_CSV_FULL
    WAREHOUSE   = FW_WH
    SCHEDULE    = 'USING CRON 0 2 * * * UTC'   -- daily at 02:00
    COMMENT     = 'Daily full ingestion for orders CSV files'
AS
    CALL UTIL.RUN_ORDERS_CSV();

-- ---------------------------------------------------------------------------
-- Customers Parquet  –  runs every 4 hours, delta load
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA
    WAREHOUSE   = FW_WH
    SCHEDULE    = 'USING CRON 0 */4 * * * UTC' -- every 4 hours
    COMMENT     = 'Every-4-hours delta ingestion for customers Parquet files'
AS
    CALL UTIL.RUN_CUSTOMERS_PARQUET();

-- ---------------------------------------------------------------------------
-- Chained task example:
-- Run orders CSV full load AFTER a prerequisite data validation task
-- (Uncomment when a prerequisite task exists)
-- ---------------------------------------------------------------------------
-- CREATE OR REPLACE TASK UTIL.TASK_ORDERS_CSV_FULL_CHAINED
--     WAREHOUSE  = FW_WH
--     AFTER      UTIL.TASK_SOME_PREREQUISITE
--     COMMENT    = 'Orders CSV full load – triggered after prerequisite task'
-- AS
--     CALL UTIL.RUN_ORDERS_CSV();

-- ===========================================================================
-- Activate tasks  (tasks start in SUSPENDED state by default)
-- ===========================================================================
ALTER TASK UTIL.TASK_CLAIMS_TXT_DELTA           RESUME;
ALTER TASK UTIL.TASK_ORDERS_CSV_FULL            RESUME;
ALTER TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA    RESUME;

-- ===========================================================================
-- Suspend tasks (use when deploying updates or during maintenance)
-- ===========================================================================
-- ALTER TASK UTIL.TASK_CLAIMS_TXT_DELTA           SUSPEND;
-- ALTER TASK UTIL.TASK_ORDERS_CSV_FULL            SUSPEND;
-- ALTER TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA    SUSPEND;

-- ===========================================================================
-- Manual execution  (for ad hoc or restart scenarios)
-- ===========================================================================
-- EXECUTE TASK UTIL.TASK_CLAIMS_TXT_DELTA;
-- EXECUTE TASK UTIL.TASK_ORDERS_CSV_FULL;
-- EXECUTE TASK UTIL.TASK_CUSTOMERS_PARQUET_DELTA;

-- ===========================================================================
-- Monitor task history via account_usage (last 14 days)
-- ===========================================================================
-- SELECT name, state, scheduled_time, completed_time, error_message
-- FROM   SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
-- WHERE  database_name = 'INGESTION_FW'
-- ORDER  BY scheduled_time DESC
-- LIMIT  100;
