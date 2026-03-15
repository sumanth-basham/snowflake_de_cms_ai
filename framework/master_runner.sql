-- =============================================================================
-- master_runner.sql
-- Purpose : ONE reusable master execution entry point.
--           This is the single procedure that every pipeline invocation calls.
--
-- Runtime parameters
--   p_yaml_name      VARCHAR  – dataset YAML filename  (e.g. "claims_txt.yaml")
--   p_yaml_file_path VARCHAR  – path within config stage (e.g. "datasets/claims_txt.yaml")
--   p_process_type   VARCHAR  – process type (currently "file_ingestion")
--
-- Execution flow
--   1.  Generate run_id (UUID)
--   2.  Log run start (PIPELINE_RUN_LOG)
--   3.  Load and validate YAML (UTIL.LOAD_AND_VALIDATE_YAML)
--        ↳ reads YAML from stage
--        ↳ parses with Python UDF
--        ↳ validates structure against schema contract
--        ↳ fails before Stage1 if YAML is structurally invalid
--   4.  Route to process_type handler
--        → file_ingestion: execute stages in sequence
--   5.  Stage1 (UTIL.STAGE1_HANDLER)
--        ↳ discover files, infer schema, COPY INTO raw table
--        ↳ returns: batch_id, discovered_columns, raw_table
--   6.  Stage2 (UTIL.STAGE2_HANDLER)
--        ↳ validate field references (runtime, against discovered schema)
--        ↳ type conversion, null check, reject routing
--        ↳ MERGE / APPEND / OVERWRITE to STG table
--   7.  Stage3 (UTIL.STAGE3_HANDLER)
--        ↳ execute ordered sql_query actions
--        ↳ create/replace curated views and tables
--   8.  Log run end (PIPELINE_RUN_LOG)
--
-- Usage examples
--   -- Claims TXT delta load
--   CALL UTIL.MASTER_RUNNER(
--       'claims_txt.yaml',
--       'datasets/claims_txt.yaml',
--       'file_ingestion'
--   );
--
--   -- Orders CSV full load
--   CALL UTIL.MASTER_RUNNER(
--       'orders_csv.yaml',
--       'datasets/orders_csv.yaml',
--       'file_ingestion'
--   );
--
--   -- Customers Parquet delta load
--   CALL UTIL.MASTER_RUNNER(
--       'customers_parquet.yaml',
--       'datasets/customers_parquet.yaml',
--       'file_ingestion'
--   );
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.MASTER_RUNNER(
    p_yaml_name      VARCHAR,
    p_yaml_file_path VARCHAR,
    p_process_type   VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Master entry point for all pipeline executions. Accepts yaml_name, yaml_file_path, process_type.'
AS
$$
DECLARE
    -- Runtime context
    v_run_id         VARCHAR;
    v_process_type   VARCHAR;
    v_load_type      VARCHAR;

    -- Stage results
    v_yaml_result    VARIANT;
    v_yaml_config    VARIANT;
    v_stage1_ctx     VARIANT;
    v_stage2_ctx     VARIANT;
    v_stage3_ctx     VARIANT;

    -- Run-level counters
    v_rows_stage1    NUMBER := 0;
    v_rows_valid     NUMBER := 0;
    v_rows_rejected  NUMBER := 0;
    v_stage1_status  VARCHAR := 'PENDING';
    v_stage2_status  VARCHAR := 'PENDING';
    v_stage3_status  VARCHAR := 'PENDING';

    -- Final result
    v_final_result   VARIANT;
BEGIN
    -- -----------------------------------------------------------------------
    -- 1. Generate run_id
    -- -----------------------------------------------------------------------
    v_run_id       := UUID_STRING();
    v_process_type := UPPER(TRIM(:p_process_type));

    -- -----------------------------------------------------------------------
    -- 2. Validate process_type
    -- -----------------------------------------------------------------------
    IF :v_process_type NOT IN ('FILE_INGESTION') THEN
        RAISE EXCEPTION USING MESSAGE =
            'UNSUPPORTED_PROCESS_TYPE: "' || :p_process_type ||
            '". Supported values: file_ingestion';
    END IF;

    -- -----------------------------------------------------------------------
    -- 3. Log run start
    --    load_type is not yet known (comes from YAML) – set after YAML load
    -- -----------------------------------------------------------------------
    CALL UTIL.LOG_RUN_START(
        :v_run_id, :p_yaml_name, :p_yaml_file_path,
        :v_process_type, 'UNKNOWN'
    );

    -- -----------------------------------------------------------------------
    -- 4. Load and validate YAML  (FAIL-FAST before Stage1)
    -- -----------------------------------------------------------------------
    CALL UTIL.LOAD_AND_VALIDATE_YAML(
        :v_run_id, :p_yaml_name, :p_yaml_file_path
    ) INTO v_yaml_result;

    v_yaml_config := :v_yaml_result:yaml_variant;
    v_load_type   := :v_yaml_config:stage1:load_type::VARCHAR;

    -- Update run log with resolved load_type
    UPDATE CONTROL.PIPELINE_RUN_LOG
    SET load_type = :v_load_type
    WHERE run_id  = :v_run_id;

    -- -----------------------------------------------------------------------
    -- 5. Route to process_type handler
    --    Currently only FILE_INGESTION is supported.
    --    Additional process types (e.g. DB_INGESTION, API_INGESTION) can be
    --    added here by extending the IF/ELSEIF chain.
    -- -----------------------------------------------------------------------
    IF :v_process_type = 'FILE_INGESTION' THEN

        -- -------------------------------------------------------------------
        -- STAGE 1  –  Raw file ingestion and source schema discovery
        -- -------------------------------------------------------------------
        BEGIN
            CALL UTIL.STAGE1_HANDLER(
                :v_run_id, :p_yaml_name, :v_yaml_config
            ) INTO v_stage1_ctx;

            v_stage1_status := 'SUCCESS';
            v_rows_stage1   := :v_stage1_ctx:rows_loaded::NUMBER;
        EXCEPTION
            WHEN OTHER THEN
                v_stage1_status := 'FAILED';
                CALL UTIL.LOG_RUN_END(
                    :v_run_id, 'FAILED',
                    0, 0, 0,
                    :v_stage1_status, 'SKIPPED', 'SKIPPED',
                    SQLERRM
                );
                RAISE EXCEPTION USING MESSAGE =
                    'STAGE1_FAILED for run_id=' || :v_run_id ||
                    ' yaml=' || :p_yaml_name ||
                    ': ' || SQLERRM;
        END;

        -- Guard: if Stage1 loaded zero rows, skip Stage2 and Stage3
        IF :v_rows_stage1 = 0 THEN
            CALL UTIL.LOG_RUN_END(
                :v_run_id, 'SUCCESS',
                0, 0, 0,
                'SUCCESS', 'SKIPPED', 'SKIPPED',
                'Stage1 loaded 0 rows. No files matched the pattern or source is empty.'
            );

            RETURN OBJECT_CONSTRUCT(
                'run_id',    :v_run_id,
                'status',    'SUCCESS',
                'message',   'No files loaded – pipeline completed with 0 rows',
                'load_type', :v_load_type
            );
        END IF;

        -- -------------------------------------------------------------------
        -- STAGE 2  –  Standardization, validation, and target write
        -- -------------------------------------------------------------------
        BEGIN
            CALL UTIL.STAGE2_HANDLER(
                :v_run_id, :p_yaml_name, :v_yaml_config, :v_stage1_ctx
            ) INTO v_stage2_ctx;

            v_stage2_status := 'SUCCESS';
            v_rows_valid    := :v_stage2_ctx:valid_rows::NUMBER;
            v_rows_rejected := :v_stage2_ctx:rejected_rows::NUMBER;
        EXCEPTION
            WHEN OTHER THEN
                v_stage2_status := 'FAILED';
                CALL UTIL.LOG_RUN_END(
                    :v_run_id, 'FAILED',
                    :v_rows_stage1, 0, 0,
                    :v_stage1_status, :v_stage2_status, 'SKIPPED',
                    SQLERRM
                );
                RAISE EXCEPTION USING MESSAGE =
                    'STAGE2_FAILED for run_id=' || :v_run_id ||
                    ' yaml=' || :p_yaml_name ||
                    ': ' || SQLERRM;
        END;

        -- -------------------------------------------------------------------
        -- STAGE 3  –  Curated output creation and post-processing
        -- -------------------------------------------------------------------
        BEGIN
            CALL UTIL.STAGE3_HANDLER(
                :v_run_id, :p_yaml_name, :v_yaml_config, :v_stage2_ctx
            ) INTO v_stage3_ctx;

            v_stage3_status := 'SUCCESS';
        EXCEPTION
            WHEN OTHER THEN
                -- Stage3 failures are recorded but do not roll back Stage1/Stage2.
                -- The pipeline status is set to PARTIAL to distinguish from clean success.
                v_stage3_status := 'FAILED';
                CALL UTIL.LOG_RUN_END(
                    :v_run_id, 'PARTIAL',
                    :v_rows_stage1, :v_rows_valid, :v_rows_rejected,
                    :v_stage1_status, :v_stage2_status, :v_stage3_status,
                    SQLERRM
                );
                RAISE EXCEPTION USING MESSAGE =
                    'STAGE3_FAILED for run_id=' || :v_run_id ||
                    ' yaml=' || :p_yaml_name ||
                    ': ' || SQLERRM;
        END;

    END IF;

    -- -----------------------------------------------------------------------
    -- 6. Log successful run completion
    -- -----------------------------------------------------------------------
    CALL UTIL.LOG_RUN_END(
        :v_run_id, 'SUCCESS',
        :v_rows_stage1, :v_rows_valid, :v_rows_rejected,
        :v_stage1_status, :v_stage2_status, :v_stage3_status,
        NULL
    );

    -- -----------------------------------------------------------------------
    -- 7. Return run summary
    -- -----------------------------------------------------------------------
    v_final_result := OBJECT_CONSTRUCT(
        'run_id',          :v_run_id,
        'yaml_name',       :p_yaml_name,
        'process_type',    :v_process_type,
        'load_type',       :v_load_type,
        'status',          'SUCCESS',
        'rows_stage1',     :v_rows_stage1,
        'rows_valid',      :v_rows_valid,
        'rows_rejected',   :v_rows_rejected,
        'stage1_status',   :v_stage1_status,
        'stage2_status',   :v_stage2_status,
        'stage3_status',   :v_stage3_status
    );

    RETURN :v_final_result;

EXCEPTION
    WHEN OTHER THEN
        -- Catch any unhandled exception at the master level
        CALL UTIL.LOG_RUN_END(
            :v_run_id, 'FAILED',
            :v_rows_stage1, :v_rows_valid, :v_rows_rejected,
            :v_stage1_status, :v_stage2_status, :v_stage3_status,
            SQLERRM
        );
        RAISE;
END;
$$;

-- =============================================================================
-- Grant execute to FW_EXECUTOR role (service accounts and Tasks use this role)
-- =============================================================================
GRANT USAGE ON PROCEDURE UTIL.MASTER_RUNNER(VARCHAR, VARCHAR, VARCHAR)
    TO ROLE FW_EXECUTOR;
