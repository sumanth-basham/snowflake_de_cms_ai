-- =============================================================================
-- merge_generator.sql
-- Purpose : Generates MERGE, APPEND, and OVERWRITE SQL for Stage2 target writes.
--
-- Design rules (from problem statement)
--   merge_util  = "yes"  → MERGE INTO target USING source ON primary_keys
--   merge_util  = "no"   → APPEND (INSERT INTO) or OVERWRITE (TRUNCATE + INSERT)
--                           depending on load_type
--
--   load_type = "full"  + merge_util = "no"  → TRUNCATE + INSERT (overwrite)
--   load_type = "delta" + merge_util = "no"  → INSERT INTO (append)
--   load_type = "adhoc" + merge_util = "no"  → INSERT INTO (append)
--   Any         + merge_util = "yes"          → MERGE
--
-- Procedures
--   UTIL.GENERATE_MERGE_SQL       – MERGE statement builder
--   UTIL.EXECUTE_WRITE_STRATEGY   – routes to MERGE / APPEND / OVERWRITE
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ---------------------------------------------------------------------------
-- UTIL.GENERATE_MERGE_SQL
-- Builds a MERGE statement to upsert from a staging CTE into the target table.
--
-- Parameters
--   p_target_schema  VARCHAR  – target schema (STG)
--   p_target_table   VARCHAR  – target table name
--   p_source_alias   VARCHAR  – CTE or subquery alias for source data
--   p_primary_keys   VARIANT  – JSON array of PK column names
--   p_all_columns    VARIANT  – JSON array of all payload + metadata columns
--   p_delete_col     VARCHAR  – delete_column_name (or NULL if not configured)
--   p_delete_val     VARCHAR  – delete_column_value
--
-- Returns VARCHAR  –  complete MERGE SQL statement
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.GENERATE_MERGE_SQL(
    p_target_schema  VARCHAR,
    p_target_table   VARCHAR,
    p_source_alias   VARCHAR,
    p_primary_keys   VARIANT,
    p_all_columns    VARIANT,
    p_delete_col     VARCHAR,
    p_delete_val     VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generates a MERGE statement for Stage2 target upsert'
AS
$$
DECLARE
    v_join_clause    VARCHAR := '';
    v_update_clause  VARCHAR := '';
    v_insert_cols    VARCHAR := '';
    v_insert_vals    VARCHAR := '';
    v_merge_sql      VARCHAR;
    v_col            VARCHAR;
    v_pk             VARCHAR;
    v_i              INTEGER := 0;
    v_col_count      INTEGER;
    v_pk_count       INTEGER;
    v_is_pk          BOOLEAN;
    v_pk_lower       VARIANT;
    v_col_lower      VARCHAR;
BEGIN
    -- Build lowercase PK set for O(n) lookup
    v_pk_lower := ARRAY_CONSTRUCT();
    v_pk_count := ARRAY_SIZE(:p_primary_keys);
    v_i := 0;
    WHILE :v_i < :v_pk_count DO
        v_pk_lower := ARRAY_APPEND(:v_pk_lower,
            LOWER(:p_primary_keys[v_i]::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Build ON clause  (tgt.pk1 = src.pk1 AND tgt.pk2 = src.pk2 ...)
    v_i := 0;
    WHILE :v_i < :v_pk_count DO
        v_pk := :p_primary_keys[v_i]::VARCHAR;
        IF :v_i = 0 THEN
            v_join_clause := 'tgt.' || :v_pk || ' = src.' || :v_pk;
        ELSE
            v_join_clause := :v_join_clause || ' AND tgt.' || :v_pk || ' = src.' || :v_pk;
        END IF;
        v_i := v_i + 1;
    END WHILE;

    -- Build UPDATE SET clause (skip PK columns) and INSERT cols/vals
    v_col_count := ARRAY_SIZE(:p_all_columns);
    v_i := 0;
    WHILE :v_i < :v_col_count DO
        v_col       := :p_all_columns[v_i]::VARCHAR;
        v_col_lower := LOWER(:v_col);
        v_is_pk     := ARRAY_CONTAINS(:v_col_lower::VARIANT, :v_pk_lower);

        -- INSERT columns/values include everything
        IF :v_i = 0 THEN
            v_insert_cols := :v_col;
            v_insert_vals := 'src.' || :v_col;
        ELSE
            v_insert_cols := :v_insert_cols || ', ' || :v_col;
            v_insert_vals := :v_insert_vals || ', src.' || :v_col;
        END IF;

        -- UPDATE SET excludes PKs and run_id / batch_id (lineage preserved)
        IF NOT :v_is_pk
           AND :v_col_lower NOT IN ('run_id', 'batch_id', 'chunk_id', 'source_file_name',
                                    'source_file_path', 'batch_load_date') THEN
            IF LENGTH(:v_update_clause) = 0 THEN
                v_update_clause := 'tgt.' || :v_col || ' = src.' || :v_col;
            ELSE
                v_update_clause := :v_update_clause || ',\n        tgt.' || :v_col || ' = src.' || :v_col;
            END IF;
        END IF;

        v_i := v_i + 1;
    END WHILE;

    -- Include batch_load_date in UPDATE so we always track last-modified
    v_update_clause := :v_update_clause || ',\n        tgt.batch_load_date = src.batch_load_date';

    -- -----------------------------------------------------------------------
    -- Build the MERGE statement
    -- Includes:
    --   WHEN MATCHED AND delete_flag = delete_value → DELETE (soft-delete)
    --   WHEN MATCHED                                 → UPDATE
    --   WHEN NOT MATCHED                             → INSERT
    -- -----------------------------------------------------------------------
    v_merge_sql :=
        'MERGE INTO ' || :p_target_schema || '.' || :p_target_table || ' AS tgt' || '\n' ||
        'USING ' || :p_source_alias || ' AS src' || '\n' ||
        'ON (' || :v_join_clause || ')' || '\n';

    -- Soft-delete clause (only if delete_column_name is configured)
    IF :p_delete_col IS NOT NULL AND LENGTH(:p_delete_col) > 0 THEN
        v_merge_sql := :v_merge_sql ||
            'WHEN MATCHED AND src.' || :p_delete_col ||
            ' = ''' || :p_delete_val || ''' THEN DELETE' || '\n';
    END IF;

    v_merge_sql := :v_merge_sql ||
        'WHEN MATCHED THEN' || '\n' ||
        '    UPDATE SET' || '\n' ||
        '        ' || :v_update_clause || '\n' ||
        'WHEN NOT MATCHED THEN' || '\n' ||
        '    INSERT (' || :v_insert_cols || ')' || '\n' ||
        '    VALUES (' || :v_insert_vals || ')';

    RETURN :v_merge_sql;
END;
$$;


-- ---------------------------------------------------------------------------
-- UTIL.EXECUTE_WRITE_STRATEGY
-- Executes the correct write strategy for Stage2 based on merge_util and
-- load_type.
--
-- merge_util = "yes"              → MERGE
-- merge_util = "no" + full        → TRUNCATE + INSERT
-- merge_util = "no" + delta/adhoc → INSERT (append)
--
-- Logs merge audit record to AUDIT.MERGE_AUDIT_LOG.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.EXECUTE_WRITE_STRATEGY(
    p_run_id          VARCHAR,
    p_batch_id        VARCHAR,
    p_yaml_name       VARCHAR,
    p_target_schema   VARCHAR,
    p_target_table    VARCHAR,
    p_source_cte_sql  VARCHAR,   -- Full CTE SQL that produces the validated dataset
    p_primary_keys    VARIANT,
    p_all_columns     VARIANT,
    p_delete_col      VARCHAR,
    p_delete_val      VARCHAR,
    p_merge_util      VARCHAR,   -- "yes" | "no"
    p_load_type       VARCHAR    -- "full" | "delta" | "adhoc"
)
RETURNS OBJECT
LANGUAGE SQL
COMMENT = 'Executes the write strategy (MERGE / APPEND / OVERWRITE) for Stage2'
AS
$$
DECLARE
    v_strategy       VARCHAR;
    v_merge_sql      VARCHAR;
    v_result_object  OBJECT;
    v_rows_inserted  NUMBER := 0;
    v_rows_updated   NUMBER := 0;
    v_rows_deleted   NUMBER := 0;
    v_merge_audit_id VARCHAR;
BEGIN
    v_merge_audit_id := UUID_STRING();

    -- Determine strategy
    IF LOWER(:p_merge_util) = 'yes' THEN
        v_strategy := 'MERGE_UPSERT';
    ELSEIF LOWER(:p_load_type) = 'full' THEN
        v_strategy := 'OVERWRITE';
    ELSE
        v_strategy := 'APPEND';
    END IF;

    IF :v_strategy = 'MERGE_UPSERT' THEN
        -- Generate MERGE SQL using the CTE as the source
        CALL UTIL.GENERATE_MERGE_SQL(
            :p_target_schema, :p_target_table,
            'valid_src',
            :p_primary_keys, :p_all_columns,
            :p_delete_col, :p_delete_val
        ) INTO v_merge_sql;

        -- Wrap CTE around MERGE:
        --   WITH valid_src AS (<source_cte_sql>) <merge_sql>
        v_merge_sql := 'WITH valid_src AS (\n' || :p_source_cte_sql || '\n)\n' || :v_merge_sql;

        EXECUTE IMMEDIATE :v_merge_sql;

        -- Snowflake does not return DML row counts from EXECUTE IMMEDIATE.
        -- Row counts must be queried from RESULT_SCAN of the last query ID.
        -- Pattern documented in Stage2 handler comments.

    ELSEIF :v_strategy = 'OVERWRITE' THEN
        -- Full load: truncate then insert
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || :p_target_schema || '.' || :p_target_table;

        EXECUTE IMMEDIATE
            'INSERT INTO ' || :p_target_schema || '.' || :p_target_table || '\n' ||
            :p_source_cte_sql;

    ELSE  -- APPEND
        EXECUTE IMMEDIATE
            'INSERT INTO ' || :p_target_schema || '.' || :p_target_table || '\n' ||
            :p_source_cte_sql;
    END IF;

    -- Log merge audit
    INSERT INTO AUDIT.MERGE_AUDIT_LOG (
        merge_audit_id, run_id, batch_id, yaml_name,
        target_schema, target_table, merge_strategy,
        primary_keys, executed_at
    )
    VALUES (
        :v_merge_audit_id, :p_run_id, :p_batch_id, :p_yaml_name,
        :p_target_schema, :p_target_table, :v_strategy,
        ARRAY_TO_STRING(:p_primary_keys, ','), CURRENT_TIMESTAMP()
    );

    RETURN OBJECT_CONSTRUCT(
        'strategy',       :v_strategy,
        'merge_audit_id', :v_merge_audit_id,
        'status',         'SUCCESS'
    );
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO AUDIT.MERGE_AUDIT_LOG (
            merge_audit_id, run_id, batch_id, yaml_name,
            target_schema, target_table, merge_strategy,
            primary_keys, executed_at
        )
        VALUES (
            :v_merge_audit_id, :p_run_id, :p_batch_id, :p_yaml_name,
            :p_target_schema, :p_target_table, :v_strategy,
            ARRAY_TO_STRING(:p_primary_keys, ','), CURRENT_TIMESTAMP()
        );
        RAISE;
END;
$$;
