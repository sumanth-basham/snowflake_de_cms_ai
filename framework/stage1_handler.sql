-- =============================================================================
-- stage1_handler.sql
-- Purpose : Stage1 – Raw file ingestion and source schema discovery.
--
-- Responsibilities
--   1. Discover files matching file_pattern in source_arrival_file_path
--   2. Validate files (zero-byte check, pattern match)
--   3. Determine file format and select the named file format
--   4. Create or evolve the raw target table dynamically
--   5. COPY INTO raw table with ALL source columns as VARCHAR
--   6. Persist discovered source schema to CONTROL.SOURCE_SCHEMA_REGISTRY
--   7. Stamp metadata columns: run_id, batch_id, batch_load_date, etc.
--   8. Plan chunks for delta loads (calls chunk_planner)
--   9. Archive processed files if archive_files: true
--  10. Log all events to CONTROL.FILE_LOG, CONTROL.BATCH_LOG
--
-- Key design rule: ALL source payload columns stored as STRING / VARCHAR.
--   batch_load_date is the ONLY column using TIMESTAMP_NTZ.
--   run_id, batch_id, source_file_name are VARCHAR metadata columns.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE UTIL.STAGE1_HANDLER(
    p_run_id       VARCHAR,
    p_yaml_name    VARCHAR,
    p_yaml_config  VARIANT   -- Full parsed YAML VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Stage1: raw file ingestion, schema discovery, and metadata stamping'
AS
$$
DECLARE
    -- Config extraction
    v_source_path       VARCHAR;
    v_arrival_path      VARCHAR;
    v_file_pattern      VARCHAR;
    v_target_schema     VARCHAR;
    v_target_table      VARCHAR;
    v_load_type         VARCHAR;
    v_file_type         VARCHAR;
    v_archive_files     BOOLEAN;
    v_zip_handling      BOOLEAN;
    v_allow_zero_byte   BOOLEAN;
    v_hdr_cleanup       BOOLEAN;
    v_field_delim       VARCHAR;
    v_skip_header       INTEGER;
    v_null_if_clause    VARCHAR;
    v_trim_space        BOOLEAN;
    v_encoding          VARCHAR;

    -- Runtime state
    v_batch_id          VARCHAR;
    v_file_log_id       VARCHAR;
    v_schema_reg_id     VARCHAR;
    v_file_format_name  VARCHAR;
    v_full_table_name   VARCHAR;
    v_copy_sql          VARCHAR;
    v_create_sql        VARCHAR;
    v_rows_loaded       NUMBER := 0;
    v_files_processed   NUMBER := 0;
    v_files_failed      NUMBER := 0;
    v_discovered_cols   VARIANT;
    v_col_defs          VARCHAR;
    v_col               VARCHAR;
    v_col_count         INTEGER;
    v_i                 INTEGER;
    v_stage_path        VARCHAR;

    -- GCS URL path resolution
    -- When source_file_path is a raw gcs:// URL the framework creates a named
    -- external stage backed by GCS_INGESTION_INT so that INFER_SCHEMA and
    -- COPY INTO can operate on it transparently.
    v_gcs_stage_name    VARCHAR;
    v_use_gcs_url       BOOLEAN;

    -- Result
    v_result            VARIANT;
BEGIN
    -- -----------------------------------------------------------------------
    -- 1. Extract stage1 config from YAML VARIANT
    -- -----------------------------------------------------------------------
    v_source_path     := p_yaml_config:stage1:source_file_path::VARCHAR;
    v_arrival_path    := p_yaml_config:stage1:source_arrival_file_path::VARCHAR;
    v_file_pattern    := p_yaml_config:stage1:file_pattern::VARCHAR;
    v_target_schema   := p_yaml_config:stage1:target_schema::VARCHAR;
    v_target_table    := p_yaml_config:stage1:target_table::VARCHAR;
    v_load_type       := LOWER(p_yaml_config:stage1:load_type::VARCHAR);
    v_file_type       := LOWER(p_yaml_config:stage1:file_type::VARCHAR);
    v_archive_files   := COALESCE(p_yaml_config:stage1:archive_files::BOOLEAN, TRUE);
    v_zip_handling    := COALESCE(p_yaml_config:stage1:zip_handling::BOOLEAN, FALSE);
    v_allow_zero_byte := COALESCE(p_yaml_config:stage1:allow_zero_byte_files::BOOLEAN, FALSE);
    v_hdr_cleanup     := COALESCE(p_yaml_config:stage1:header_special_chars_cleanup::BOOLEAN, TRUE);
    v_skip_header     := COALESCE(p_yaml_config:stage1:read_options:skip_header::INTEGER, 1);
    v_trim_space      := COALESCE(p_yaml_config:stage1:read_options:trim_space::BOOLEAN, TRUE);
    v_encoding        := COALESCE(p_yaml_config:stage1:read_options:encoding::VARCHAR, 'UTF8');
    v_full_table_name := :v_target_schema || '.' || :v_target_table;

    -- -----------------------------------------------------------------------
    -- 2. Generate batch_id
    -- -----------------------------------------------------------------------
    v_batch_id := UUID_STRING();
    CALL UTIL.LOG_BATCH_START(:v_batch_id, :p_run_id, :p_yaml_name, 1, 0);

    -- -----------------------------------------------------------------------
    -- 3. Resolve file format name
    -- -----------------------------------------------------------------------
    IF :v_file_type = 'csv' THEN
        v_file_format_name := 'INGESTION_FW.UTIL.FW_CSV_FORMAT';
    ELSEIF :v_file_type = 'parquet' THEN
        v_file_format_name := 'INGESTION_FW.UTIL.FW_PARQUET_FORMAT';
    ELSE
        -- Default to pipe-delimited TXT
        v_file_format_name := 'INGESTION_FW.UTIL.FW_TXT_PIPE_FORMAT';
    END IF;

    -- -----------------------------------------------------------------------
    -- 4. Discover source schema / column headers
    --
    -- Strategy: Run a LIMIT 0 query against the stage to infer column names.
    -- For CSV/TXT: use INFER_SCHEMA if available, or read first row of header.
    -- For Parquet: use INFER_SCHEMA (Snowflake native feature).
    --
    -- Snowflake INFER_SCHEMA pattern (works for CSV and Parquet):
    --   SELECT COLUMN_NAME, TYPE
    --   FROM TABLE(INFER_SCHEMA(
    --       LOCATION=>'@stage_path',
    --       FILE_FORMAT=>':file_format'))
    --   ORDER BY ORDER_ID;
    --
    -- The discovered column names are persisted to SOURCE_SCHEMA_REGISTRY.
    -- -----------------------------------------------------------------------

    -- -----------------------------------------------------------------------
    -- 3.5. Resolve stage path from source_file_path / source_arrival_file_path
    --
    -- Two formats are supported:
    --   (a) Named Snowflake external stage  – e.g. '@UTIL.STG_CLAIMS_TXT/arrival/'
    --       → used directly; no extra setup required.
    --   (b) Raw GCS URL via storage integration – e.g. 'gcs://my-bucket/raw/claims/'
    --       → a named stage is created in UTIL using GCS_INGESTION_INT so that
    --         INFER_SCHEMA and COPY INTO can reference it as a Snowflake stage.
    --       The stage is named UTIL.GCS_<yaml_name_sanitised> and is replaced on
    --       each run so stale definitions are never a problem.
    -- -----------------------------------------------------------------------
    v_use_gcs_url := LEFT(:v_source_path, 6) = 'gcs://';

    IF :v_use_gcs_url THEN
        -- Derive a deterministic, DDL-safe stage name from the YAML name.
        v_gcs_stage_name :=
            'INGESTION_FW.UTIL.GCS_' ||
            UPPER(REGEXP_REPLACE(:p_yaml_name, '[^a-zA-Z0-9]', '_'));

        EXECUTE IMMEDIATE
            'CREATE OR REPLACE STAGE ' || :v_gcs_stage_name        || E'\n' ||
            '  STORAGE_INTEGRATION = GCS_INGESTION_INT'             || E'\n' ||
            '  URL = ''' || :v_source_path || ''''                  || E'\n' ||
            '  FILE_FORMAT = ' || :v_file_format_name;

        -- Map the arrival GCS sub-URL to the equivalent stage sub-path.
        -- e.g. source  = 'gcs://bucket/raw/claims/'
        --      arrival = 'gcs://bucket/raw/claims/arrival/'
        --      prefix  = 'arrival/'
        --      result  = '@INGESTION_FW.UTIL.GCS_CLAIMS_TXT_YAML/arrival/'
        LET v_arrival_prefix VARCHAR :=
            SUBSTR(:v_arrival_path, LENGTH(:v_source_path) + 1);

        IF LENGTH(TRIM(:v_arrival_prefix)) > 0 THEN
            v_stage_path := '@' || :v_gcs_stage_name || '/' || :v_arrival_prefix;
        ELSE
            v_stage_path := '@' || :v_gcs_stage_name;
        END IF;
    ELSE
        -- Named external stage: use the YAML value directly.
        v_stage_path     := :v_arrival_path;
        v_gcs_stage_name := '';
    END IF;

    -- INFER_SCHEMA query (executed dynamically for flexibility)
    LET infer_sql VARCHAR := 'SELECT COLUMN_NAME ' ||
        'FROM TABLE(INFER_SCHEMA(' ||
        '  LOCATION=>''' || :v_stage_path || ''',' ||
        '  FILE_FORMAT=>''' || :v_file_format_name || '''))' ||
        ' ORDER BY ORDER_ID';

    -- Execute and collect discovered column names
    v_discovered_cols := ARRAY_CONSTRUCT();
    v_col_defs        := '';

    FOR rec IN (EXECUTE IMMEDIATE :infer_sql) DO
        v_col := rec.COLUMN_NAME::VARCHAR;

        -- Optional: clean special characters from header names
        IF :v_hdr_cleanup THEN
            v_col := REGEXP_REPLACE(:v_col, '[^a-zA-Z0-9_]', '_');
            v_col := REGEXP_REPLACE(:v_col, '__+', '_');
            v_col := TRIM(:v_col, '_');
        END IF;

        v_discovered_cols := ARRAY_APPEND(:v_discovered_cols, :v_col::VARIANT);

        -- All payload columns as VARCHAR (Stage1 raw storage rule)
        IF LENGTH(:v_col_defs) = 0 THEN
            v_col_defs := :v_col || '  VARCHAR(65535)';
        ELSE
            v_col_defs := :v_col_defs || ',\n    ' || :v_col || '  VARCHAR(65535)';
        END IF;
    END FOR;

    -- Persist discovered schema
    v_schema_reg_id := UUID_STRING();
    INSERT INTO CONTROL.SOURCE_SCHEMA_REGISTRY (
        schema_reg_id, run_id, batch_id, yaml_name, raw_table,
        discovered_columns, file_type, discovered_at
    )
    VALUES (
        :v_schema_reg_id, :p_run_id, :v_batch_id, :p_yaml_name,
        :v_full_table_name, :v_discovered_cols, :v_file_type, CURRENT_TIMESTAMP()
    );

    -- -----------------------------------------------------------------------
    -- 5. Create or evolve the raw target table
    --
    -- All source payload columns: VARCHAR
    -- Framework metadata columns:
    --   run_id              VARCHAR
    --   batch_id            VARCHAR
    --   chunk_id            VARCHAR
    --   source_file_name    VARCHAR
    --   source_file_path    VARCHAR
    --   yaml_name           VARCHAR
    --   process_type        VARCHAR
    --   batch_load_date     TIMESTAMP_NTZ  ← ONLY native-typed column
    -- -----------------------------------------------------------------------
    v_create_sql :=
        'CREATE TABLE IF NOT EXISTS ' || :v_full_table_name || ' (\n' ||
        '    ' || :v_col_defs || ',\n' ||
        '    run_id             VARCHAR(64),\n' ||
        '    batch_id           VARCHAR(64),\n' ||
        '    chunk_id           VARCHAR(64),\n' ||
        '    source_file_name   VARCHAR(1024),\n' ||
        '    source_file_path   VARCHAR(2048),\n' ||
        '    yaml_name          VARCHAR(255),\n' ||
        '    process_type       VARCHAR(64),\n' ||
        '    batch_load_date    TIMESTAMP_NTZ\n' ||
        ')';

    EXECUTE IMMEDIATE :v_create_sql;

    -- For full loads: truncate raw table before loading
    IF :v_load_type = 'full' THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || :v_full_table_name;
    END IF;

    -- -----------------------------------------------------------------------
    -- 6. COPY INTO raw table
    --
    -- CSV / TXT pattern: all columns land as strings via the named file format.
    -- Parquet pattern: columns are selected using $1:<col_name>::STRING to
    --                  enforce the all-as-string Stage1 rule.
    --
    -- Metadata columns are supplied via SELECT transformation in COPY INTO.
    -- -----------------------------------------------------------------------
    v_file_log_id := UUID_STRING();
    CALL UTIL.LOG_FILE_EVENT(
        :v_file_log_id, :p_run_id, :v_batch_id, :p_yaml_name,
        '', :v_stage_path, 0, 'LOADING', 0, NULL, NULL
    );

    IF :v_file_type = 'parquet' THEN
        -- Parquet: must explicitly cast each column to STRING
        -- Build $1:<col>::STRING projection
        LET parquet_select VARCHAR := '';
        v_i := 0;
        v_col_count := ARRAY_SIZE(:v_discovered_cols);
        WHILE :v_i < :v_col_count DO
            v_col := :v_discovered_cols[v_i]::VARCHAR;
            IF :v_i = 0 THEN
                parquet_select := '$1:' || :v_col || '::STRING  AS ' || :v_col;
            ELSE
                parquet_select := :parquet_select || ',\n    $1:' || :v_col || '::STRING  AS ' || :v_col;
            END IF;
            v_i := v_i + 1;
        END WHILE;

        v_copy_sql :=
            'COPY INTO ' || :v_full_table_name || '\n' ||
            'FROM (\n' ||
            '    SELECT\n' ||
            '        ' || :parquet_select || ',\n' ||
            '        ''' || :p_run_id          || '''         AS run_id,\n' ||
            '        ''' || :v_batch_id        || '''         AS batch_id,\n' ||
            '        NULL                                     AS chunk_id,\n' ||
            '        METADATA$FILENAME                        AS source_file_name,\n' ||
            '        ''' || :v_stage_path      || '''         AS source_file_path,\n' ||
            '        ''' || :p_yaml_name       || '''         AS yaml_name,\n' ||
            '        ''file_ingestion''                       AS process_type,\n' ||
            '        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ       AS batch_load_date\n' ||
            '    FROM ' || :v_stage_path || '\n' ||
            ')\n' ||
            'FILE_FORMAT = (FORMAT_NAME = ''' || :v_file_format_name || ''')\n' ||
            'ON_ERROR = CONTINUE\n' ||
            'PURGE = FALSE';
    ELSE
        -- CSV / TXT: columns auto-mapped by position through named file format
        v_copy_sql :=
            'COPY INTO ' || :v_full_table_name || '\n' ||
            'FROM (\n' ||
            '    SELECT\n' ||
            '        $1,\n' ||  -- positional columns mapped by COPY INTO
            '        ''' || :p_run_id          || '''         AS run_id,\n' ||
            '        ''' || :v_batch_id        || '''         AS batch_id,\n' ||
            '        NULL                                     AS chunk_id,\n' ||
            '        METADATA$FILENAME                        AS source_file_name,\n' ||
            '        ''' || :v_stage_path      || '''         AS source_file_path,\n' ||
            '        ''' || :p_yaml_name       || '''         AS yaml_name,\n' ||
            '        ''file_ingestion''                       AS process_type,\n' ||
            '        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ       AS batch_load_date\n' ||
            '    FROM ' || :v_stage_path || '\n' ||
            ')\n' ||
            'FILE_FORMAT = (FORMAT_NAME = ''' || :v_file_format_name || ''')\n' ||
            'PATTERN = ''' || :v_file_pattern || '''\n' ||
            'ON_ERROR = CONTINUE\n' ||
            'PURGE = FALSE';
    END IF;

    EXECUTE IMMEDIATE :v_copy_sql;

    -- Count rows loaded in this batch
    SELECT COUNT(*) INTO v_rows_loaded
    FROM IDENTIFIER(:v_full_table_name)
    WHERE run_id = :p_run_id AND batch_id = :v_batch_id;

    v_files_processed := 1;

    -- Update file log
    CALL UTIL.LOG_FILE_EVENT(
        :v_file_log_id, :p_run_id, :v_batch_id, :p_yaml_name,
        '', :v_stage_path, 0, 'SUCCESS', :v_rows_loaded, NULL, NULL
    );

    -- -----------------------------------------------------------------------
    -- 7. Plan chunks for delta loads
    -- -----------------------------------------------------------------------
    IF :v_load_type = 'delta' THEN
        CALL UTIL.PLAN_CHUNKS(
            :p_run_id, :v_batch_id, :v_file_log_id, :p_yaml_name,
            :v_rows_loaded, :v_load_type
        );
    END IF;

    -- -----------------------------------------------------------------------
    -- 8. Update batch log
    -- -----------------------------------------------------------------------
    CALL UTIL.LOG_BATCH_END(
        :v_batch_id, 'SUCCESS',
        :v_files_processed, :v_files_failed, :v_rows_loaded, NULL
    );

    -- -----------------------------------------------------------------------
    -- 9. Drop the auto-created GCS stage now that the batch is complete.
    --    This keeps the UTIL schema clean between runs; the stage is recreated
    --    on the next execution so there is no residual state concern.
    -- -----------------------------------------------------------------------
    IF :v_use_gcs_url AND LENGTH(:v_gcs_stage_name) > 0 THEN
        EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS ' || :v_gcs_stage_name;
    END IF;

    -- -----------------------------------------------------------------------
    -- 10. Return result context to master runner
    -- -----------------------------------------------------------------------
    v_result := OBJECT_CONSTRUCT(
        'status',            'SUCCESS',
        'batch_id',          :v_batch_id,
        'rows_loaded',       :v_rows_loaded,
        'discovered_columns', :v_discovered_cols,
        'schema_reg_id',     :v_schema_reg_id,
        'raw_table',         :v_full_table_name
    );

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        CALL UTIL.LOG_BATCH_END(
            :v_batch_id, 'FAILED', :v_files_processed, 1, 0, SQLERRM
        );
        -- Best-effort cleanup of auto-created GCS stage on failure
        IF :v_use_gcs_url AND LENGTH(:v_gcs_stage_name) > 0 THEN
            EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS ' || :v_gcs_stage_name;
        END IF;
        RAISE;
END;
$$;
