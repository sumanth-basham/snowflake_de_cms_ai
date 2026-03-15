-- =============================================================================
-- stage3_handler.sql
-- Purpose : Stage3 – Curated output creation and post-processing actions.
--
-- Responsibilities
--   1. Iterate through the stage3 list from the YAML config
--   2. For each item with type = "sql_query", execute the value as SQL
--   3. Log each action start, end, and outcome to AUDIT.STAGE3_ACTION_LOG
--   4. Fail the entire Stage3 on first action failure (ordered execution)
--   5. Return a summary result to the master runner
--
-- Stage3 execution model (from problem statement):
--   Each item must have: name, type, value
--   name  = logical action name (used in logging and audit)
--   type  = action type; currently only "sql_query" is supported
--   value = executable SQL statement
--
-- Execution order is defined by list position in the YAML.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.STAGE3_HANDLER(
    p_run_id      VARCHAR,
    p_yaml_name   VARCHAR,
    p_yaml_config VARIANT,
    p_stage2_ctx  VARIANT   -- Result object from STAGE2_HANDLER (for context)
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Stage3: iterates and executes ordered post-processing actions (sql_query)'
AS
$$
DECLARE
    v_actions_count  INTEGER;
    v_action         VARIANT;
    v_action_name    VARCHAR;
    v_action_type    VARCHAR;
    v_action_sql     VARCHAR;
    v_action_log_id  VARCHAR;
    v_i              INTEGER := 0;
    v_actions_done   INTEGER := 0;
    v_actions_failed INTEGER := 0;

    v_result         VARIANT;
BEGIN
    v_actions_count := ARRAY_SIZE(p_yaml_config:stage3);

    -- Iterate actions in YAML list order
    WHILE :v_i < :v_actions_count DO
        v_action      := p_yaml_config:stage3[v_i];
        v_action_name := v_action:name::VARCHAR;
        v_action_type := LOWER(v_action:type::VARCHAR);
        v_action_sql  := v_action:value::VARCHAR;
        v_action_log_id := UUID_STRING();

        -- Log action start
        CALL UTIL.LOG_STAGE3_ACTION(
            :v_action_log_id, :p_run_id, :p_yaml_name,
            :v_i + 1,
            :v_action_name, :v_action_type,
            :v_action_sql,
            'RUNNING', 0, NULL
        );

        BEGIN
            -- Dispatch by action type
            IF :v_action_type = 'sql_query' THEN
                EXECUTE IMMEDIATE :v_action_sql;
                v_actions_done := :v_actions_done + 1;

                -- Log action success
                CALL UTIL.LOG_STAGE3_ACTION(
                    :v_action_log_id, :p_run_id, :p_yaml_name,
                    :v_i + 1,
                    :v_action_name, :v_action_type,
                    :v_action_sql,
                    'SUCCESS', 0, NULL
                );
            ELSE
                -- Unknown action type: fail immediately
                RAISE EXCEPTION USING MESSAGE =
                    'UNSUPPORTED_ACTION_TYPE: action "' || :v_action_name ||
                    '" has type "' || :v_action_type ||
                    '" which is not supported. Valid types: sql_query';
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_actions_failed := :v_actions_failed + 1;

                -- Log action failure
                CALL UTIL.LOG_STAGE3_ACTION(
                    :v_action_log_id, :p_run_id, :p_yaml_name,
                    :v_i + 1,
                    :v_action_name, :v_action_type,
                    :v_action_sql,
                    'FAILED', 0, SQLERRM
                );

                -- Stage3 fails on first action failure (ordered contract)
                RAISE EXCEPTION USING MESSAGE =
                    'STAGE3_ACTION_FAILED: action [' || (:v_i + 1)::VARCHAR ||
                    '] "' || :v_action_name || '" failed. Error: ' || SQLERRM;
        END;

        v_i := v_i + 1;
    END WHILE;

    v_result := OBJECT_CONSTRUCT(
        'status',          'SUCCESS',
        'actions_executed', :v_actions_done,
        'actions_failed',   :v_actions_failed,
        'total_actions',    :v_actions_count
    );

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
$$;
