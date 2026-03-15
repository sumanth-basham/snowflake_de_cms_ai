-- =============================================================================
-- stage2_handler.sql
-- Purpose : Stage2 – Standardization, validation, and target write.
--
-- Responsibilities
--   1. Retrieve stage1 context (batch_id, discovered columns, raw table)
--   2. Validate stage2 field references against discovered schema (fail-fast)
--   3. Check schema drift between raw columns and target table
--   4. Apply schema_changes policy (ADD COLUMN or FAIL)
--   5. Generate type-conversion SELECT expressions from YAML
--   6. Build valid dataset CTE and reject dataset CTE
--   7. Insert reject rows to REJECTS.<dataset>_REJECT with reason codes
--   8. Execute write strategy (MERGE / APPEND / OVERWRITE) for valid rows
--   9. Log all validation, reject, and merge audit events
--
-- Key design separations (from problem statement):
--   merge_util    = data loading strategy (MERGE vs APPEND vs OVERWRITE)
--   schema_changes = schema evolution handling (ADD COLUMN vs FAIL)
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.STAGE2_HANDLER(
    p_run_id       VARCHAR,
    p_yaml_name    VARCHAR,
    p_yaml_config  VARIANT,
    p_stage1_ctx   VARIANT   -- Result object returned by STAGE1_HANDLER
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Stage2: type conversion, validation, reject routing, and target write'
AS
$$
DECLARE
    -- Config from YAML
    v_raw_table         VARCHAR;
    v_target_schema     VARCHAR;
    v_target_table      VARCHAR;
    v_full_target       VARCHAR;
    v_load_type         VARCHAR;
    v_merge_util        VARCHAR;
    v_schema_changes    VARCHAR;
    v_primary_keys      VARIANT;
    v_delete_col        VARCHAR;
    v_delete_val        VARCHAR;

    -- Stage1 context
    v_batch_id          VARCHAR;
    v_discovered_cols   VARIANT;

    -- Generated SQL fragments
    v_conversion_select VARCHAR;
    v_null_check_pred   VARCHAR;
    v_composite_cte     VARCHAR;

    -- Reject table
    v_reject_table      VARCHAR;
    v_reject_full       VARCHAR;

    -- Counts
    v_total_rows        NUMBER := 0;
    v_valid_rows        NUMBER := 0;
    v_rejected_rows     NUMBER := 0;

    -- Schema drift
    v_target_cols       VARIANT;
    v_drift_col         VARCHAR;
    v_drift_type        VARCHAR;
    v_drift_count       NUMBER := 0;

    -- All columns array for merge generator
    v_all_columns       VARIANT;
    v_col               VARCHAR;
    v_i                 INTEGER;

    -- Insert SQL fragments
    v_reject_sql        VARCHAR;
    v_valid_cte_sql     VARCHAR;
    v_reject_log_id     VARCHAR;

    v_result            VARIANT;
BEGIN
    -- -----------------------------------------------------------------------
    -- 1. Extract stage2 config
    -- -----------------------------------------------------------------------
    v_target_schema  := p_yaml_config:stage2:target_schema::VARCHAR;
    v_target_table   := p_yaml_config:stage2:target_table::VARCHAR;
    v_full_target    := :v_target_schema || '.' || :v_target_table;
    v_load_type      := LOWER(p_yaml_config:stage2:load_type::VARCHAR);
    v_merge_util     := LOWER(p_yaml_config:stage2:merge_util::VARCHAR);
    v_schema_changes := LOWER(p_yaml_config:stage2:schema_changes::VARCHAR);
    v_primary_keys   := p_yaml_config:stage2:primary_keys;
    v_delete_col     := p_yaml_config:stage2:delete_column_name::VARCHAR;
    v_delete_val     := p_yaml_config:stage2:delete_column_value::VARCHAR;

    -- Stage1 context
    v_batch_id        := p_stage1_ctx:batch_id::VARCHAR;
    v_raw_table       := p_stage1_ctx:raw_table::VARCHAR;
    v_discovered_cols := p_stage1_ctx:discovered_columns;

    -- -----------------------------------------------------------------------
    -- 2. Validate field references against discovered schema (FAIL-FAST)
    -- -----------------------------------------------------------------------
    CALL UTIL.VALIDATE_FIELD_REFERENCES(
        :p_run_id, :v_batch_id, :p_yaml_name,
        :v_discovered_cols, :p_yaml_config
    );

    -- -----------------------------------------------------------------------
    -- 3. Schema drift detection
    --
    -- Compare discovered raw columns against existing target table columns.
    -- If target table does not exist yet, skip drift check.
    -- -----------------------------------------------------------------------
    LET target_exists NUMBER := (
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :v_target_schema
          AND TABLE_NAME   = :v_target_table
    );

    IF :target_exists > 0 THEN
        -- Get current target table columns
        v_target_cols := ARRAY_CONSTRUCT();
        FOR col_rec IN (
            SELECT LOWER(COLUMN_NAME) AS col_name
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = :v_target_schema
              AND  TABLE_NAME   = :v_target_table
            ORDER  BY ORDINAL_POSITION
        ) DO
            v_target_cols := ARRAY_APPEND(:v_target_cols, col_rec.col_name::VARIANT);
        END FOR;

        -- Check for new columns in source that are not in target
        v_i := 0;
        WHILE :v_i < ARRAY_SIZE(:v_discovered_cols) DO
            v_col := LOWER(:v_discovered_cols[v_i]::VARCHAR);
            IF NOT ARRAY_CONTAINS(:v_col::VARIANT, :v_target_cols) THEN
                v_drift_count := :v_drift_count + 1;
                v_drift_col   := :v_col;
                v_drift_type  := 'NEW_COLUMN';

                IF :v_schema_changes = 'yes' THEN
                    -- Controlled schema evolution: ADD COLUMN as VARCHAR
                    EXECUTE IMMEDIATE
                        'ALTER TABLE ' || :v_full_target ||
                        ' ADD COLUMN ' || :v_col || ' VARCHAR(65535)';

                    INSERT INTO AUDIT.SCHEMA_DRIFT_LOG (
                        drift_id, run_id, batch_id, yaml_name,
                        target_schema, target_table, drift_type,
                        column_name, source_type, target_type,
                        resolution, resolution_detail, schema_changes_flag, detected_at
                    )
                    VALUES (
                        UUID_STRING(), :p_run_id, :v_batch_id, :p_yaml_name,
                        :v_target_schema, :v_target_table, :v_drift_type,
                        :v_col, 'VARCHAR', 'VARCHAR',
                        'ADDED',
                        'ALTER TABLE ' || :v_full_target || ' ADD COLUMN ' || :v_col || ' VARCHAR(65535)',
                        'yes', CURRENT_TIMESTAMP()
                    );
                ELSE
                    -- schema_changes: no → record and fail
                    INSERT INTO AUDIT.SCHEMA_DRIFT_LOG (
                        drift_id, run_id, batch_id, yaml_name,
                        target_schema, target_table, drift_type,
                        column_name, source_type, target_type,
                        resolution, resolution_detail, schema_changes_flag, detected_at
                    )
                    VALUES (
                        UUID_STRING(), :p_run_id, :v_batch_id, :p_yaml_name,
                        :v_target_schema, :v_target_table, :v_drift_type,
                        :v_col, 'VARCHAR', 'DOES_NOT_EXIST',
                        'FAILED',
                        'schema_changes=no: new column "' || :v_col || '" detected, failing run',
                        'no', CURRENT_TIMESTAMP()
                    );

                    RAISE EXCEPTION USING MESSAGE =
                        'SCHEMA_DRIFT_DETECTED: new column "' || :v_col ||
                        '" in source not found in target ' || :v_full_target ||
                        '. Set schema_changes: yes to enable controlled evolution.';
                END IF;
            END IF;
            v_i := v_i + 1;
        END WHILE;
    END IF;

    -- -----------------------------------------------------------------------
    -- 4. Generate conversion SELECT expression
    -- -----------------------------------------------------------------------
    CALL UTIL.GENERATE_CONVERSION_SELECT(
        :p_yaml_config, :v_discovered_cols,
        :p_run_id, :v_batch_id, 'chunk_id', 'source_file_name', 'source_file_path'
    ) INTO v_conversion_select;

    -- -----------------------------------------------------------------------
    -- 5. Build all-columns array for merge generator
    --    (payload columns + standard metadata columns)
    -- -----------------------------------------------------------------------
    v_all_columns := :v_discovered_cols;
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'run_id'::VARIANT);
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'batch_id'::VARIANT);
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'chunk_id'::VARIANT);
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'source_file_name'::VARIANT);
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'source_file_path'::VARIANT);
    v_all_columns := ARRAY_APPEND(:v_all_columns, 'batch_load_date'::VARIANT);

    -- -----------------------------------------------------------------------
    -- 6. Create target STG table if it does not exist
    --    Uses the conversion types derived from YAML.
    --    The create pattern mirrors the conversion SELECT output.
    -- -----------------------------------------------------------------------
    -- (Table creation DDL is generated dynamically from YAML conversion rules.
    --  In production this would be a separate utility call. For clarity,
    --  the first successful Stage2 run creates the table via CTAS.)
    IF :target_exists = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE TABLE IF NOT EXISTS ' || :v_full_target || ' AS\n' ||
            'SELECT\n    ' || :v_conversion_select || '\n' ||
            'FROM ' || :v_raw_table || '\n' ||
            'WHERE 1=0';  -- CTAS with no data – just creates schema
    END IF;

    -- -----------------------------------------------------------------------
    -- 7. Generate null-check reject predicate
    -- -----------------------------------------------------------------------
    CALL UTIL.GENERATE_NULL_CHECK_PREDICATE(:p_yaml_config)
    INTO v_null_check_pred;

    -- -----------------------------------------------------------------------
    -- 8. Build reject table name
    -- -----------------------------------------------------------------------
    v_reject_table := UPPER(REPLACE(:p_yaml_name, '.yaml', '')) || '_REJECT';
    v_reject_full  := 'REJECTS.' || :v_reject_table;

    -- -----------------------------------------------------------------------
    -- 9. Count total rows in raw table for this batch
    -- -----------------------------------------------------------------------
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM ' || :v_raw_table ||
        ' WHERE run_id = ''' || :p_run_id || '''' INTO v_total_rows;

    -- -----------------------------------------------------------------------
    -- 10. INSERT reject rows
    --
    -- Reject routing: rows failing null_check or type conversion errors.
    -- TRY_CAST returns NULL on conversion failure; those rows are also rejected.
    --
    -- Pattern:
    --   WITH converted AS (
    --     SELECT <conversion_select> FROM raw WHERE run_id = :run_id
    --   ),
    --   null_rejects AS (
    --     SELECT *, 'NULL_CHECK' AS reject_reason FROM converted WHERE <null_check>
    --   )
    --   INSERT INTO REJECTS.<dataset>_REJECT SELECT ... FROM null_rejects
    -- -----------------------------------------------------------------------
    v_reject_sql :=
        'WITH converted AS (\n' ||
        '    SELECT\n        ' || :v_conversion_select || '\n' ||
        '    FROM ' || :v_raw_table || '\n' ||
        '    WHERE run_id = ''' || :p_run_id || '''\n' ||
        '),\n' ||
        'null_rejects AS (\n' ||
        '    SELECT\n' ||
        '        UUID_STRING()                  AS reject_id,\n' ||
        '        ''' || :p_run_id  || '''        AS run_id,\n' ||
        '        ''' || :v_batch_id || '''        AS batch_id,\n' ||
        '        NULL                           AS chunk_id,\n' ||
        '        ''' || :p_yaml_name || '''       AS yaml_name,\n' ||
        '        source_file_name,\n' ||
        '        source_file_path,\n' ||
        '        NULL::NUMBER(18,0)             AS source_row_number,\n' ||
        '        ''NULL_CHECK violation: one or more required fields are null''  AS reject_reason,\n' ||
        '        PARSE_JSON(''["NULL_CHECK"]'')  AS reject_reason_codes,\n' ||
        '        * EXCLUDE (run_id, batch_id, chunk_id, source_file_name, source_file_path, batch_load_date)\n' ||
        '    FROM converted\n' ||
        '    WHERE ' || :v_null_check_pred || '\n' ||
        ')\n' ||
        'INSERT INTO ' || :v_reject_full || '\n' ||
        'SELECT * FROM null_rejects';

    IF :v_null_check_pred <> 'FALSE' THEN
        EXECUTE IMMEDIATE :v_reject_sql;

        SELECT COUNT(*) INTO v_rejected_rows
        FROM IDENTIFIER(:v_reject_full)
        WHERE run_id = :p_run_id AND batch_id = :v_batch_id;

        IF :v_rejected_rows > 0 THEN
            v_reject_log_id := UUID_STRING();
            CALL UTIL.LOG_REJECT(
                :v_reject_log_id, :p_run_id, :v_batch_id, NULL,
                :p_yaml_name, :v_raw_table, :v_reject_full,
                'NULL_CHECK', :v_rejected_rows
            );
        END IF;
    END IF;

    -- -----------------------------------------------------------------------
    -- 11. Build valid-rows CTE (exclude rejected rows)
    --     Also exclude soft-deleted rows from the valid set if delete_col set.
    -- -----------------------------------------------------------------------
    LET delete_filter VARCHAR := '';
    IF :v_delete_col IS NOT NULL AND LENGTH(:v_delete_col) > 0 THEN
        delete_filter := ' AND COALESCE(' || :v_delete_col || ', '''') <> ''' || :v_delete_val || '''';
    END IF;

    v_valid_cte_sql :=
        'SELECT\n    ' || :v_conversion_select || '\n' ||
        'FROM ' || :v_raw_table || '\n' ||
        'WHERE run_id = ''' || :p_run_id || '''\n' ||
        '  AND NOT (' || :v_null_check_pred || ')' ||
        :delete_filter;

    -- -----------------------------------------------------------------------
    -- 12. Execute write strategy (MERGE / APPEND / OVERWRITE)
    -- -----------------------------------------------------------------------
    CALL UTIL.EXECUTE_WRITE_STRATEGY(
        :p_run_id, :v_batch_id, :p_yaml_name,
        :v_target_schema, :v_target_table,
        :v_valid_cte_sql,
        :v_primary_keys, :v_all_columns,
        :v_delete_col, :v_delete_val,
        :v_merge_util, :v_load_type
    );

    -- Count valid rows
    v_valid_rows := :v_total_rows - :v_rejected_rows;

    -- -----------------------------------------------------------------------
    -- 13. Return Stage2 context
    -- -----------------------------------------------------------------------
    v_result := OBJECT_CONSTRUCT(
        'status',         'SUCCESS',
        'batch_id',       :v_batch_id,
        'total_rows',     :v_total_rows,
        'valid_rows',     :v_valid_rows,
        'rejected_rows',  :v_rejected_rows,
        'target_table',   :v_full_target
    );

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;
$$;
