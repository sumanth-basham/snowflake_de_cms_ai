-- =============================================================================
-- chunk_planner.sql
-- Purpose : Chunk planning for delta loads.
--
-- Design rules (from problem statement):
--   - Chunking is always enabled internally for delta loads.
--   - YAML does not need to configure chunking.
--   - Chunk size is controlled in framework metadata (control table or constant).
--   - Failed chunks must be restartable without reprocessing successful chunks.
--   - Chunking must work whether 1 file or many files arrive.
--
-- Procedures
--   UTIL.PLAN_CHUNKS         – create chunk records for a file
--   UTIL.GET_PENDING_CHUNKS  – return chunks that need processing (restartable)
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ---------------------------------------------------------------------------
-- Framework constants table (stores configurable framework parameters)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTROL.FRAMEWORK_CONFIG (
    config_key    VARCHAR(255)  NOT NULL,
    config_value  VARCHAR(4096) NOT NULL,
    description   VARCHAR(2048),
    updated_at    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fw_config PRIMARY KEY (config_key)
)
COMMENT = 'Configurable framework parameters (chunk_size, retry limits, etc.)';

-- Default framework configuration values
INSERT INTO CONTROL.FRAMEWORK_CONFIG (config_key, config_value, description)
SELECT 'DELTA_CHUNK_SIZE',        '500000',  'Number of rows per delta chunk'
WHERE NOT EXISTS (SELECT 1 FROM CONTROL.FRAMEWORK_CONFIG WHERE config_key = 'DELTA_CHUNK_SIZE');

INSERT INTO CONTROL.FRAMEWORK_CONFIG (config_key, config_value, description)
SELECT 'MAX_CHUNK_RETRIES',       '3',       'Max retry attempts for a failed chunk'
WHERE NOT EXISTS (SELECT 1 FROM CONTROL.FRAMEWORK_CONFIG WHERE config_key = 'MAX_CHUNK_RETRIES');

INSERT INTO CONTROL.FRAMEWORK_CONFIG (config_key, config_value, description)
SELECT 'FULL_LOAD_CHUNK_ENABLED', 'false',   'Enable chunking for full loads (default: false)'
WHERE NOT EXISTS (SELECT 1 FROM CONTROL.FRAMEWORK_CONFIG WHERE config_key = 'FULL_LOAD_CHUNK_ENABLED');


-- ---------------------------------------------------------------------------
-- UTIL.PLAN_CHUNKS
-- Creates CHUNK_LOG rows for a single file.
-- Called by Stage1 for delta loads after file discovery.
--
-- For delta loads: file is divided into ceil(row_count / chunk_size) chunks.
-- For full loads:  one single chunk (chunk_sequence = 1, offset = 0).
--
-- Returns VARIANT: { "chunks_created": N, "chunk_size": M }
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.PLAN_CHUNKS(
    p_run_id      VARCHAR,
    p_batch_id    VARCHAR,
    p_file_log_id VARCHAR,
    p_yaml_name   VARCHAR,
    p_row_count   NUMBER,
    p_load_type   VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Plans and records chunks for a file into CHUNK_LOG'
AS
$$
DECLARE
    v_chunk_size    NUMBER;
    v_chunk_count   NUMBER;
    v_offset        NUMBER := 0;
    v_chunk_seq     NUMBER := 1;
    v_chunk_id      VARCHAR;
    v_actual_size   NUMBER;
BEGIN
    -- Retrieve chunk size from framework config
    SELECT config_value::NUMBER
    INTO   v_chunk_size
    FROM   CONTROL.FRAMEWORK_CONFIG
    WHERE  config_key = 'DELTA_CHUNK_SIZE';

    -- For full loads, use a single chunk unless explicitly enabled
    IF LOWER(:p_load_type) = 'full' THEN
        v_chunk_count := 1;
        v_chunk_size  := :p_row_count;
    ELSE
        -- Delta: calculate number of chunks
        v_chunk_count := CEIL(:p_row_count / NULLIF(:v_chunk_size, 0));
        IF :v_chunk_count = 0 THEN
            v_chunk_count := 1;
        END IF;
    END IF;

    -- Create one CHUNK_LOG row per chunk
    WHILE :v_chunk_seq <= :v_chunk_count DO
        v_chunk_id    := UUID_STRING();
        v_actual_size := LEAST(:v_chunk_size, :p_row_count - :v_offset);
        IF :v_actual_size <= 0 THEN
            v_actual_size := :v_chunk_size;
        END IF;

        INSERT INTO CONTROL.CHUNK_LOG (
            chunk_id, run_id, batch_id, file_log_id, yaml_name,
            chunk_sequence, chunk_offset, chunk_size,
            status, is_restartable, start_ts
        )
        VALUES (
            :v_chunk_id, :p_run_id, :p_batch_id, :p_file_log_id, :p_yaml_name,
            :v_chunk_seq, :v_offset, :v_actual_size,
            'PENDING', TRUE, CURRENT_TIMESTAMP()
        );

        v_offset    := :v_offset + :v_actual_size;
        v_chunk_seq := :v_chunk_seq + 1;
    END WHILE;

    RETURN OBJECT_CONSTRUCT(
        'chunks_created', :v_chunk_count,
        'chunk_size',     :v_chunk_size,
        'total_rows',     :p_row_count
    );
END;
$$;


-- ---------------------------------------------------------------------------
-- UTIL.GET_PENDING_CHUNKS
-- Returns PENDING (or FAILED with retry room) chunks for a file.
-- Enables restartable processing: re-run skips SUCCESS chunks.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.GET_PENDING_CHUNKS(
    p_run_id      VARCHAR,
    p_batch_id    VARCHAR,
    p_file_log_id VARCHAR
)
RETURNS TABLE(
    chunk_id       VARCHAR,
    chunk_sequence NUMBER,
    chunk_offset   NUMBER,
    chunk_size     NUMBER,
    status         VARCHAR
)
LANGUAGE SQL
COMMENT = 'Returns pending/restartable chunks for a file'
AS
$$
DECLARE
    v_max_retries NUMBER;
BEGIN
    SELECT config_value::NUMBER
    INTO   v_max_retries
    FROM   CONTROL.FRAMEWORK_CONFIG
    WHERE  config_key = 'MAX_CHUNK_RETRIES';

    RETURN TABLE(
        SELECT
            cl.chunk_id,
            cl.chunk_sequence,
            cl.chunk_offset,
            cl.chunk_size,
            cl.status
        FROM CONTROL.CHUNK_LOG cl
        WHERE cl.run_id      = :p_run_id
          AND cl.batch_id    = :p_batch_id
          AND cl.file_log_id = :p_file_log_id
          AND cl.status IN ('PENDING', 'FAILED')
        ORDER BY cl.chunk_sequence
    );
END;
$$;
