-- =============================================================================
-- field_validator.sql
-- Purpose : Runtime field-reference validation.
--           After Stage1 discovers source column headers, this procedure
--           validates that every column referenced in the stage2 YAML
--           section actually exists in the discovered schema.
--
-- Key design rule (from problem statement):
--   "source columns are not known before file arrival – field references in
--    stage2 cannot be fully validated statically – actual field existence
--    validation must happen at runtime after Stage1 discovers source schema"
--
-- Procedure
--   UTIL.VALIDATE_FIELD_REFERENCES  –  compare YAML field lists vs discovered columns
--
-- The procedure raises an exception on the first set of missing columns,
-- providing a clear list of what is missing. This implements fail-fast.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.VALIDATE_FIELD_REFERENCES(
    p_run_id              VARCHAR,
    p_batch_id            VARCHAR,
    p_yaml_name           VARCHAR,
    p_discovered_columns  VARIANT,   -- JSON array of discovered column names (lowercase)
    p_yaml_config         VARIANT    -- Full parsed YAML VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Validates that all stage2 field references exist in discovered source schema'
AS
$$
DECLARE
    v_missing         ARRAY := ARRAY_CONSTRUCT();
    v_errors          ARRAY := ARRAY_CONSTRUCT();
    v_field           VARCHAR;
    v_col_name        VARCHAR;
    v_dec_col         VARIANT;
    v_composite_group VARIANT;
    v_composite_col   VARCHAR;
    v_col_lower       VARCHAR;
    v_i               INTEGER;
    v_j               INTEGER;
    v_dec_count       INTEGER;
    v_composite_count INTEGER;
    v_group_count     INTEGER;

    -- Helper: normalise column name to lowercase for comparison
    v_discovered_lower VARIANT;
BEGIN
    -- Build a lowercase array of discovered columns for case-insensitive comparison
    v_discovered_lower := ARRAY_CONSTRUCT();
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_discovered_columns) DO
        v_col_lower := LOWER(:p_discovered_columns[v_i]::VARCHAR);
        v_discovered_lower := ARRAY_APPEND(:v_discovered_lower, :v_col_lower);
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 1. primary_keys
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:primary_keys) DO
        v_field := LOWER(:p_yaml_config:stage2:primary_keys[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('primary_keys: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 2. delete_column_name
    -- -----------------------------------------------------------------------
    IF (:p_yaml_config:stage2:delete_column_name IS NOT NULL) THEN
        v_field := LOWER(:p_yaml_config:stage2:delete_column_name::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('delete_column_name: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
    END IF;

    -- -----------------------------------------------------------------------
    -- 3. fields_long_conversion
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_long_conversion) DO
        v_field := LOWER(:p_yaml_config:stage2:fields_long_conversion[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_long_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 4. fields_integer_conversion
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_integer_conversion) DO
        v_field := LOWER(:p_yaml_config:stage2:fields_integer_conversion[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_integer_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 5. fields_float_conversion
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_float_conversion) DO
        v_field := LOWER(:p_yaml_config:stage2:fields_float_conversion[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_float_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 6. fields_decimal_conversion
    -- -----------------------------------------------------------------------
    v_dec_count := ARRAY_SIZE(:p_yaml_config:stage2:fields_decimal_conversion);
    v_i := 0;
    WHILE v_i < :v_dec_count DO
        v_dec_col := :p_yaml_config:stage2:fields_decimal_conversion[v_i];
        v_field := LOWER(:v_dec_col:column_name::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_decimal_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 7. fields_timestamp_conversion  (strip optional #format suffix)
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_timestamp_conversion) DO
        v_col_name := :p_yaml_config:stage2:fields_timestamp_conversion[v_i]::VARCHAR;
        -- Strip #format suffix if present
        IF CONTAINS(:v_col_name, '#') THEN
            v_col_name := SPLIT_PART(:v_col_name, '#', 1);
        END IF;
        v_field := LOWER(:v_col_name);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_timestamp_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 8. fields_date_conversion  (strip optional #format suffix)
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_date_conversion) DO
        v_col_name := :p_yaml_config:stage2:fields_date_conversion[v_i]::VARCHAR;
        IF CONTAINS(:v_col_name, '#') THEN
            v_col_name := SPLIT_PART(:v_col_name, '#', 1);
        END IF;
        v_field := LOWER(:v_col_name);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_date_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 9. fields_boolean_conversion
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_boolean_conversion) DO
        v_field := LOWER(:p_yaml_config:stage2:fields_boolean_conversion[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_boolean_conversion: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 10. fields_null_check
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_yaml_config:stage2:fields_null_check) DO
        v_field := LOWER(:p_yaml_config:stage2:fields_null_check[v_i]::VARCHAR);
        IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
            v_errors := ARRAY_APPEND(:v_errors,
                ('fields_null_check: column "' || :v_field || '" not found in source schema')::VARIANT);
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- 11. fields_composite_unique_check
    -- -----------------------------------------------------------------------
    v_composite_count := ARRAY_SIZE(:p_yaml_config:stage2:fields_composite_unique_check);
    v_i := 0;
    WHILE v_i < :v_composite_count DO
        v_composite_group := :p_yaml_config:stage2:fields_composite_unique_check[v_i];
        v_group_count := ARRAY_SIZE(:v_composite_group);
        v_j := 0;
        WHILE v_j < :v_group_count DO
            v_field := LOWER(:v_composite_group[v_j]::VARCHAR);
            IF NOT ARRAY_CONTAINS(:v_field::VARIANT, :v_discovered_lower) THEN
                v_errors := ARRAY_APPEND(:v_errors,
                    ('fields_composite_unique_check[' || :v_i || ']: column "' || :v_field || '" not found in source schema')::VARIANT);
            END IF;
            v_j := v_j + 1;
        END WHILE;
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- Write validation results to AUDIT.VALIDATION_LOG
    -- -----------------------------------------------------------------------
    IF ARRAY_SIZE(:v_errors) > 0 THEN
        INSERT INTO AUDIT.VALIDATION_LOG (
            validation_log_id, run_id, batch_id, yaml_name,
            validation_type, field_name, validation_result,
            failure_count, error_detail, evaluated_at
        )
        VALUES (
            UUID_STRING(), :p_run_id, :p_batch_id, :p_yaml_name,
            'FIELD_REFERENCE', 'MULTIPLE',
            'FAILED',
            ARRAY_SIZE(:v_errors),
            ARRAY_TO_STRING(:v_errors, ' | '),
            CURRENT_TIMESTAMP()
        );

        -- Fail-fast: raise exception with full list of missing columns
        RAISE EXCEPTION USING MESSAGE =
            'FIELD_REFERENCE_VALIDATION_FAILED: ' ||
            ARRAY_SIZE(:v_errors) ||
            ' invalid field reference(s) in ' || :p_yaml_name ||
            '. Missing columns: ' || ARRAY_TO_STRING(:v_errors, ' | ');
    END IF;

    -- All references are valid
    INSERT INTO AUDIT.VALIDATION_LOG (
        validation_log_id, run_id, batch_id, yaml_name,
        validation_type, field_name, validation_result,
        failure_count, error_detail, evaluated_at
    )
    VALUES (
        UUID_STRING(), :p_run_id, :p_batch_id, :p_yaml_name,
        'FIELD_REFERENCE', 'ALL', 'PASSED', 0, NULL, CURRENT_TIMESTAMP()
    );

    RETURN OBJECT_CONSTRUCT('valid', TRUE, 'errors', ARRAY_CONSTRUCT());
END;
$$;
