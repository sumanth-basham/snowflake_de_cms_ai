-- =============================================================================
-- log_writer.sql
-- Purpose : Centralised logging utility stored procedures for the framework.
--           All stages call these procedures to write consistent log entries.
--
-- Procedures
--   UTIL.LOG_RUN_START       – initialise PIPELINE_RUN_LOG row
--   UTIL.LOG_RUN_END         – update run status and end timestamp
--   UTIL.LOG_BATCH_START     – initialise BATCH_LOG row
--   UTIL.LOG_BATCH_END       – update batch status
--   UTIL.LOG_FILE_EVENT      – upsert FILE_LOG row
--   UTIL.LOG_CHUNK_EVENT     – upsert CHUNK_LOG row
--   UTIL.LOG_YAML_VALIDATION – write YAML_EXECUTION_LOG row
--   UTIL.LOG_STAGE3_ACTION   – write STAGE3_ACTION_LOG row
--   UTIL.LOG_REJECT          – write REJECT_LOG summary row
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ---------------------------------------------------------------------------
-- LOG_RUN_START
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_RUN_START(
    p_run_id        VARCHAR,
    p_yaml_name     VARCHAR,
    p_yaml_path     VARCHAR,
    p_process_type  VARCHAR,
    p_load_type     VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Insert initial row into PIPELINE_RUN_LOG at start of execution'
AS
$$
BEGIN
    INSERT INTO CONTROL.PIPELINE_RUN_LOG (
        run_id, yaml_name, yaml_file_path, process_type, load_type,
        status, start_ts
    )
    VALUES (
        :p_run_id, :p_yaml_name, :p_yaml_path, :p_process_type, :p_load_type,
        'RUNNING', CURRENT_TIMESTAMP()
    );
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_RUN_END
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_RUN_END(
    p_run_id         VARCHAR,
    p_status         VARCHAR,   -- SUCCESS | FAILED | PARTIAL
    p_rows_stage1    NUMBER,
    p_rows_valid     NUMBER,
    p_rows_rejected  NUMBER,
    p_stage1_status  VARCHAR,
    p_stage2_status  VARCHAR,
    p_stage3_status  VARCHAR,
    p_error_message  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Update PIPELINE_RUN_LOG when execution completes or fails'
AS
$$
BEGIN
    UPDATE CONTROL.PIPELINE_RUN_LOG
    SET
        status              = :p_status,
        stage1_status       = :p_stage1_status,
        stage2_status       = :p_stage2_status,
        stage3_status       = :p_stage3_status,
        end_ts              = CURRENT_TIMESTAMP(),
        duration_seconds    = DATEDIFF('second', start_ts, CURRENT_TIMESTAMP()),
        rows_loaded_stage1  = :p_rows_stage1,
        rows_valid_stage2   = :p_rows_valid,
        rows_rejected_stage2 = :p_rows_rejected,
        error_message       = :p_error_message
    WHERE run_id = :p_run_id;
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_BATCH_START
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_BATCH_START(
    p_batch_id       VARCHAR,
    p_run_id         VARCHAR,
    p_yaml_name      VARCHAR,
    p_batch_seq      NUMBER,
    p_files_found    NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Insert initial row into BATCH_LOG'
AS
$$
BEGIN
    INSERT INTO CONTROL.BATCH_LOG (
        batch_id, run_id, yaml_name, batch_sequence, files_discovered,
        status, start_ts
    )
    VALUES (
        :p_batch_id, :p_run_id, :p_yaml_name, :p_batch_seq, :p_files_found,
        'RUNNING', CURRENT_TIMESTAMP()
    );
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_BATCH_END
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_BATCH_END(
    p_batch_id       VARCHAR,
    p_status         VARCHAR,
    p_files_done     NUMBER,
    p_files_failed   NUMBER,
    p_rows_loaded    NUMBER,
    p_error_message  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Update BATCH_LOG when batch completes'
AS
$$
BEGIN
    UPDATE CONTROL.BATCH_LOG
    SET
        status          = :p_status,
        files_processed = :p_files_done,
        files_failed    = :p_files_failed,
        rows_loaded     = :p_rows_loaded,
        end_ts          = CURRENT_TIMESTAMP(),
        error_message   = :p_error_message
    WHERE batch_id = :p_batch_id;
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_FILE_EVENT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_FILE_EVENT(
    p_file_log_id    VARCHAR,
    p_run_id         VARCHAR,
    p_batch_id       VARCHAR,
    p_yaml_name      VARCHAR,
    p_file_name      VARCHAR,
    p_file_path      VARCHAR,
    p_file_size      NUMBER,
    p_status         VARCHAR,
    p_rows_loaded    NUMBER,
    p_skip_reason    VARCHAR,
    p_error_message  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Insert or update FILE_LOG row for a source file'
AS
$$
BEGIN
    MERGE INTO CONTROL.FILE_LOG AS tgt
    USING (
        SELECT
            :p_file_log_id   AS file_log_id,
            :p_run_id        AS run_id,
            :p_batch_id      AS batch_id,
            :p_yaml_name     AS yaml_name,
            :p_file_name     AS source_file_name,
            :p_file_path     AS source_file_path,
            :p_file_size     AS file_size_bytes,
            :p_status        AS status,
            :p_rows_loaded   AS rows_loaded,
            :p_skip_reason   AS skip_reason,
            :p_error_message AS error_message
    ) AS src
    ON tgt.file_log_id = src.file_log_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.status        = src.status,
            tgt.rows_loaded   = src.rows_loaded,
            tgt.skip_reason   = src.skip_reason,
            tgt.error_message = src.error_message,
            tgt.end_ts        = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (file_log_id, run_id, batch_id, yaml_name,
                source_file_name, source_file_path, file_size_bytes,
                status, rows_loaded, skip_reason, error_message, start_ts)
        VALUES (src.file_log_id, src.run_id, src.batch_id, src.yaml_name,
                src.source_file_name, src.source_file_path, src.file_size_bytes,
                src.status, src.rows_loaded, src.skip_reason, src.error_message,
                CURRENT_TIMESTAMP());
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_CHUNK_EVENT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_CHUNK_EVENT(
    p_chunk_id      VARCHAR,
    p_run_id        VARCHAR,
    p_batch_id      VARCHAR,
    p_file_log_id   VARCHAR,
    p_yaml_name     VARCHAR,
    p_chunk_seq     NUMBER,
    p_chunk_offset  NUMBER,
    p_chunk_size    NUMBER,
    p_rows_in_chunk NUMBER,
    p_rows_valid    NUMBER,
    p_rows_rejected NUMBER,
    p_status        VARCHAR,
    p_error_message VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Insert or update CHUNK_LOG row for a delta-load chunk'
AS
$$
BEGIN
    MERGE INTO CONTROL.CHUNK_LOG AS tgt
    USING (
        SELECT
            :p_chunk_id      AS chunk_id,
            :p_run_id        AS run_id,
            :p_batch_id      AS batch_id,
            :p_file_log_id   AS file_log_id,
            :p_yaml_name     AS yaml_name,
            :p_chunk_seq     AS chunk_sequence,
            :p_chunk_offset  AS chunk_offset,
            :p_chunk_size    AS chunk_size,
            :p_rows_in_chunk AS rows_in_chunk,
            :p_rows_valid    AS rows_valid,
            :p_rows_rejected AS rows_rejected,
            :p_status        AS status,
            :p_error_message AS error_message
    ) AS src
    ON tgt.chunk_id = src.chunk_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.rows_in_chunk  = src.rows_in_chunk,
            tgt.rows_valid     = src.rows_valid,
            tgt.rows_rejected  = src.rows_rejected,
            tgt.status         = src.status,
            tgt.error_message  = src.error_message,
            tgt.end_ts         = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (chunk_id, run_id, batch_id, file_log_id, yaml_name,
                chunk_sequence, chunk_offset, chunk_size,
                rows_in_chunk, rows_valid, rows_rejected,
                status, error_message, start_ts)
        VALUES (src.chunk_id, src.run_id, src.batch_id, src.file_log_id,
                src.yaml_name, src.chunk_sequence, src.chunk_offset, src.chunk_size,
                src.rows_in_chunk, src.rows_valid, src.rows_rejected,
                src.status, src.error_message, CURRENT_TIMESTAMP());
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_YAML_VALIDATION
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_YAML_VALIDATION(
    p_yaml_exec_id        VARCHAR,
    p_run_id              VARCHAR,
    p_yaml_name           VARCHAR,
    p_yaml_path           VARCHAR,
    p_yaml_content        VARIANT,
    p_schema_validation   VARCHAR,
    p_validation_errors   VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Write a YAML_EXECUTION_LOG row for every YAML load and validation event'
AS
$$
BEGIN
    INSERT INTO CONTROL.YAML_EXECUTION_LOG (
        yaml_exec_id, run_id, yaml_name, yaml_file_path,
        yaml_content, schema_validation, validation_errors, loaded_at
    )
    VALUES (
        :p_yaml_exec_id, :p_run_id, :p_yaml_name, :p_yaml_path,
        :p_yaml_content, :p_schema_validation, :p_validation_errors,
        CURRENT_TIMESTAMP()
    );
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_STAGE3_ACTION
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_STAGE3_ACTION(
    p_action_log_id  VARCHAR,
    p_run_id         VARCHAR,
    p_yaml_name      VARCHAR,
    p_seq            NUMBER,
    p_name           VARCHAR,
    p_type           VARCHAR,
    p_sql            VARCHAR,
    p_status         VARCHAR,
    p_rows_affected  NUMBER,
    p_error_message  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Write STAGE3_ACTION_LOG row for each stage3 action'
AS
$$
BEGIN
    MERGE INTO AUDIT.STAGE3_ACTION_LOG AS tgt
    USING (
        SELECT
            :p_action_log_id AS action_log_id,
            :p_run_id        AS run_id,
            :p_yaml_name     AS yaml_name,
            :p_seq           AS action_sequence,
            :p_name          AS action_name,
            :p_type          AS action_type,
            :p_sql           AS sql_executed,
            :p_status        AS status,
            :p_rows_affected AS rows_affected,
            :p_error_message AS error_message
    ) AS src
    ON tgt.action_log_id = src.action_log_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.status        = src.status,
            tgt.rows_affected = src.rows_affected,
            tgt.error_message = src.error_message,
            tgt.end_ts        = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (action_log_id, run_id, yaml_name, action_sequence,
                action_name, action_type, sql_executed,
                status, rows_affected, error_message, start_ts)
        VALUES (src.action_log_id, src.run_id, src.yaml_name, src.action_sequence,
                src.action_name, src.action_type, src.sql_executed,
                src.status, src.rows_affected, src.error_message, CURRENT_TIMESTAMP());
    RETURN 'OK';
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_REJECT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.LOG_REJECT(
    p_reject_log_id     VARCHAR,
    p_run_id            VARCHAR,
    p_batch_id          VARCHAR,
    p_chunk_id          VARCHAR,
    p_yaml_name         VARCHAR,
    p_source_table      VARCHAR,
    p_reject_table      VARCHAR,
    p_reason_code       VARCHAR,
    p_reject_count      NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Write summary reject count to REJECTS.REJECT_LOG'
AS
$$
BEGIN
    INSERT INTO REJECTS.REJECT_LOG (
        reject_log_id, run_id, batch_id, chunk_id,
        yaml_name, source_table, reject_table,
        reject_reason_code, reject_count, logged_at
    )
    VALUES (
        :p_reject_log_id, :p_run_id, :p_batch_id, :p_chunk_id,
        :p_yaml_name, :p_source_table, :p_reject_table,
        :p_reason_code, :p_reject_count, CURRENT_TIMESTAMP()
    );
    RETURN 'OK';
END;
$$;
