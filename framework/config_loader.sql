-- =============================================================================
-- config_loader.sql
-- Purpose : YAML config loading and schema validation utilities.
--
-- Procedures
--   UTIL.LOAD_AND_VALIDATE_YAML  –  reads YAML from stage, parses it,
--                                    validates against schema contract,
--                                    logs to YAML_EXECUTION_LOG
--
-- YAML Loading Pattern (Snowflake-native)
-- ----------------------------------------
-- YAML files are stored on an internal stage (UTIL.STG_FW_CONFIGS).
-- Reading multi-line text from a stage in Snowflake Scripting:
--
--   SELECT LISTAGG($1, '\n')
--   FROM   @UTIL.STG_FW_CONFIGS/datasets/claims_txt.yaml
--   (FILE_FORMAT => (TYPE = 'CSV' FIELD_DELIMITER = NONE
--                    RECORD_DELIMITER = '\n' SKIP_HEADER = 0));
--
-- The raw text is passed to UTIL.PARSE_YAML (Python UDF) which returns
-- a VARIANT containing the full parsed YAML structure.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.LOAD_AND_VALIDATE_YAML(
    p_run_id        VARCHAR,
    p_yaml_name     VARCHAR,
    p_yaml_file_path VARCHAR  -- Path within the config stage, e.g. 'datasets/claims_txt.yaml'
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Loads a YAML from the config stage, parses it, and validates against the schema contract'
AS
$$
DECLARE
    v_yaml_exec_id      VARCHAR;
    v_raw_yaml_text     VARCHAR;
    v_yaml_variant      VARIANT;
    v_validation_result VARIANT;
    v_is_valid          BOOLEAN;
    v_errors_variant    VARIANT;

    -- Result
    v_result            VARIANT;
BEGIN
    v_yaml_exec_id := UUID_STRING();

    -- -----------------------------------------------------------------------
    -- 1. Read raw YAML text from internal stage
    --
    -- Snowflake Scripting reads stage files using positional column $1.
    -- Each row corresponds to one line of the file.
    -- LISTAGG reassembles all lines into a single string.
    --
    -- File format used:
    --   TYPE = CSV with no field delimiter (treat each line as one column)
    --   RECORD_DELIMITER = '\n'  (split on newlines)
    --   SKIP_HEADER = 0          (don't skip any lines)
    -- -----------------------------------------------------------------------
    SELECT LISTAGG($1, '\n')
    INTO   :v_raw_yaml_text
    FROM   @UTIL.STG_FW_CONFIGS/:p_yaml_file_path
    (FILE_FORMAT => (
        TYPE             = 'CSV'
        FIELD_DELIMITER  = NONE
        RECORD_DELIMITER = '\n'
        SKIP_HEADER      = 0
        EMPTY_FIELD_AS_NULL = FALSE
    ));

    IF :v_raw_yaml_text IS NULL OR LENGTH(:v_raw_yaml_text) = 0 THEN
        RAISE EXCEPTION USING MESSAGE =
            'YAML_LOAD_FAILED: file "' || :p_yaml_file_path ||
            '" is empty or not found on stage UTIL.STG_FW_CONFIGS';
    END IF;

    -- -----------------------------------------------------------------------
    -- 2. Parse YAML text to VARIANT using Python UDF
    -- -----------------------------------------------------------------------
    SELECT UTIL.PARSE_YAML(:v_raw_yaml_text)
    INTO   :v_yaml_variant;

    -- Check if parse returned an error object
    IF :v_yaml_variant:error IS NOT NULL THEN
        CALL UTIL.LOG_YAML_VALIDATION(
            :v_yaml_exec_id, :p_run_id, :p_yaml_name, :p_yaml_file_path,
            NULL, 'FAILED',
            ARRAY_CONSTRUCT(:v_yaml_variant:error::VARCHAR)::VARIANT
        );
        RAISE EXCEPTION USING MESSAGE =
            'YAML_PARSE_FAILED: ' || :v_yaml_variant:error::VARCHAR;
    END IF;

    -- -----------------------------------------------------------------------
    -- 3. Validate parsed YAML against schema contract (Python UDF)
    -- -----------------------------------------------------------------------
    SELECT UTIL.VALIDATE_YAML_SCHEMA(:v_yaml_variant)
    INTO   :v_validation_result;

    v_is_valid := :v_validation_result:valid::BOOLEAN;
    v_errors_variant := :v_validation_result:errors;

    -- -----------------------------------------------------------------------
    -- 4. Log YAML validation event
    -- -----------------------------------------------------------------------
    CALL UTIL.LOG_YAML_VALIDATION(
        :v_yaml_exec_id, :p_run_id, :p_yaml_name, :p_yaml_file_path,
        :v_yaml_variant,
        IFF(:v_is_valid, 'PASSED', 'FAILED'),
        :v_errors_variant
    );

    -- -----------------------------------------------------------------------
    -- 5. Fail-fast if schema validation failed
    -- -----------------------------------------------------------------------
    IF NOT :v_is_valid THEN
        RAISE EXCEPTION USING MESSAGE =
            'YAML_SCHEMA_VALIDATION_FAILED for "' || :p_yaml_name ||
            '". Errors: ' || ARRAY_TO_STRING(:v_errors_variant, ' | ');
    END IF;

    -- -----------------------------------------------------------------------
    -- 6. Return parsed and validated YAML VARIANT
    -- -----------------------------------------------------------------------
    v_result := OBJECT_CONSTRUCT(
        'status',         'SUCCESS',
        'yaml_exec_id',   :v_yaml_exec_id,
        'yaml_variant',   :v_yaml_variant,
        'validation',     'PASSED'
    );

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
$$;
