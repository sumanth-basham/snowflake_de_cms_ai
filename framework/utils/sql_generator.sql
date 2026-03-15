-- =============================================================================
-- sql_generator.sql
-- Purpose : Generates type-conversion SQL expressions from YAML configuration.
--           All conversion expressions operate on VARCHAR source columns
--           from the Stage1 raw table.
--
-- Key design decisions
--   • Date/timestamp fields may carry an optional "#format" suffix
--     e.g.  "order_date#MM/dd/yyyy"
--     The format is extracted and used in TO_DATE / TO_TIMESTAMP calls.
--   • Boolean normalization handles: TRUE/FALSE, true/false, 1/0, Y/N, YES/NO
--   • Decimal conversion uses YAML precision/scale metadata
--   • Invalid conversions route to reject instead of silently coercing
--
-- Procedures
--   UTIL.GENERATE_CONVERSION_SELECT  –  builds the SELECT clause for Stage2
--   UTIL.GENERATE_REJECT_PREDICATE   –  builds the WHERE clause for reject routing
--   UTIL.GENERATE_NULL_CHECK_PREDICATE – builds null-check filter
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

-- ---------------------------------------------------------------------------
-- UTIL.GENERATE_CONVERSION_SELECT
-- Returns a VARCHAR containing the SELECT column list for the Stage2
-- INSERT INTO ... SELECT ... transformation.
--
-- Each source column appears once; columns with conversion rules get a
-- TRY_CAST / TO_DATE / TO_TIMESTAMP wrapper. Columns without rules
-- are passed through as VARCHAR (preserving the Stage1 string value).
--
-- Parameters
--   p_yaml_config        VARIANT  – full parsed YAML
--   p_discovered_columns VARIANT  – JSON array of raw column names
--   p_run_id             VARCHAR
--   p_batch_id           VARCHAR
--   p_chunk_id           VARCHAR
--   p_source_file_name   VARCHAR
--   p_source_file_path   VARCHAR
--
-- Returns VARCHAR  –  comma-separated SELECT expressions + metadata columns
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.GENERATE_CONVERSION_SELECT(
    p_yaml_config        VARIANT,
    p_discovered_columns VARIANT,
    p_run_id             VARCHAR,
    p_batch_id           VARCHAR,
    p_chunk_id           VARCHAR,
    p_source_file_name   VARCHAR,
    p_source_file_path   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generates the SELECT expression list for Stage2 type conversion'
AS
$$
DECLARE
    v_select_parts  VARCHAR := '';
    v_col           VARCHAR;
    v_col_lower     VARCHAR;
    v_expr          VARCHAR;
    v_i             INTEGER := 0;
    v_j             INTEGER;
    v_dec_count     INTEGER;
    v_dec_item      VARIANT;
    v_ts_entry      VARCHAR;
    v_dt_entry      VARCHAR;
    v_col_name      VARCHAR;
    v_format        VARCHAR;
    v_precision     INTEGER;
    v_scale         INTEGER;
    v_is_long       BOOLEAN;
    v_is_int        BOOLEAN;
    v_is_float      BOOLEAN;
    v_is_decimal    BOOLEAN;
    v_is_timestamp  BOOLEAN;
    v_is_date       BOOLEAN;
    v_is_boolean    BOOLEAN;
    v_ts_format     VARCHAR := '';
    v_dt_format     VARCHAR := '';

    -- Build lookup sets (VARIANT arrays) for each conversion type
    v_long_cols     VARIANT;
    v_int_cols      VARIANT;
    v_float_cols    VARIANT;
    v_bool_cols     VARIANT;
    v_null_cols     VARIANT;
    -- Decimal: use a VARIANT object for O(1) lookup: { col_name: "precision,scale" }
    v_decimal_map   VARIANT;
    -- Timestamp/date: maps col_name -> format_mask
    v_ts_map        VARIANT;
    v_dt_map        VARIANT;
BEGIN
    -- Build normalised lowercase column lookup arrays
    v_long_cols   := ARRAY_CONSTRUCT();
    v_int_cols    := ARRAY_CONSTRUCT();
    v_float_cols  := ARRAY_CONSTRUCT();
    v_bool_cols   := ARRAY_CONSTRUCT();
    v_decimal_map := OBJECT_CONSTRUCT();
    v_ts_map      := OBJECT_CONSTRUCT();
    v_dt_map      := OBJECT_CONSTRUCT();

    -- Populate long conversion set
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_long_conversion) DO
        v_long_cols := ARRAY_APPEND(:v_long_cols,
            LOWER(p_yaml_config:stage2:fields_long_conversion[v_i]::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Populate integer conversion set
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_integer_conversion) DO
        v_int_cols := ARRAY_APPEND(:v_int_cols,
            LOWER(p_yaml_config:stage2:fields_integer_conversion[v_i]::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Populate float conversion set
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_float_conversion) DO
        v_float_cols := ARRAY_APPEND(:v_float_cols,
            LOWER(p_yaml_config:stage2:fields_float_conversion[v_i]::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Populate boolean conversion set
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_boolean_conversion) DO
        v_bool_cols := ARRAY_APPEND(:v_bool_cols,
            LOWER(p_yaml_config:stage2:fields_boolean_conversion[v_i]::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Build decimal map: col_name -> "precision,scale"
    v_dec_count := ARRAY_SIZE(p_yaml_config:stage2:fields_decimal_conversion);
    v_i := 0;
    WHILE v_i < :v_dec_count DO
        v_dec_item  := p_yaml_config:stage2:fields_decimal_conversion[v_i];
        v_col_name  := LOWER(v_dec_item:column_name::VARCHAR);
        v_precision := v_dec_item:precision::INTEGER;
        v_scale     := v_dec_item:scale::INTEGER;
        v_decimal_map := OBJECT_INSERT(:v_decimal_map, :v_col_name,
                         (:v_precision::VARCHAR || ',' || :v_scale::VARCHAR)::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Build timestamp map: col_name -> format_mask (or empty string for AUTO)
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_timestamp_conversion) DO
        v_ts_entry := p_yaml_config:stage2:fields_timestamp_conversion[v_i]::VARCHAR;
        IF CONTAINS(:v_ts_entry, '#') THEN
            v_col_name := LOWER(SPLIT_PART(:v_ts_entry, '#', 1));
            v_ts_format := SPLIT_PART(:v_ts_entry, '#', 2);
        ELSE
            v_col_name  := LOWER(:v_ts_entry);
            v_ts_format := 'AUTO';
        END IF;
        v_ts_map := OBJECT_INSERT(:v_ts_map, :v_col_name, :v_ts_format::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- Build date map: col_name -> format_mask
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(p_yaml_config:stage2:fields_date_conversion) DO
        v_dt_entry := p_yaml_config:stage2:fields_date_conversion[v_i]::VARCHAR;
        IF CONTAINS(:v_dt_entry, '#') THEN
            v_col_name := LOWER(SPLIT_PART(:v_dt_entry, '#', 1));
            v_dt_format := SPLIT_PART(:v_dt_entry, '#', 2);
        ELSE
            v_col_name  := LOWER(:v_dt_entry);
            v_dt_format := 'AUTO';
        END IF;
        v_dt_map := OBJECT_INSERT(:v_dt_map, :v_col_name, :v_dt_format::VARIANT);
        v_i := v_i + 1;
    END WHILE;

    -- -----------------------------------------------------------------------
    -- Iterate discovered columns and build SELECT expressions
    -- -----------------------------------------------------------------------
    v_i := 0;
    WHILE v_i < ARRAY_SIZE(:p_discovered_columns) DO
        v_col       := p_discovered_columns[v_i]::VARCHAR;
        v_col_lower := LOWER(:v_col);

        v_is_long      := ARRAY_CONTAINS(:v_col_lower::VARIANT, :v_long_cols);
        v_is_int       := ARRAY_CONTAINS(:v_col_lower::VARIANT, :v_int_cols);
        v_is_float     := ARRAY_CONTAINS(:v_col_lower::VARIANT, :v_float_cols);
        v_is_boolean   := ARRAY_CONTAINS(:v_col_lower::VARIANT, :v_bool_cols);
        v_is_decimal   := (:v_decimal_map[:v_col_lower] IS NOT NULL);
        v_is_timestamp := (:v_ts_map[:v_col_lower] IS NOT NULL);
        v_is_date      := (:v_dt_map[:v_col_lower] IS NOT NULL);

        -- Build the conversion expression based on type precedence:
        -- decimal > long > integer > float > timestamp > date > boolean > passthrough
        IF :v_is_decimal THEN
            v_precision := SPLIT_PART(:v_decimal_map[:v_col_lower]::VARCHAR, ',', 1)::INTEGER;
            v_scale     := SPLIT_PART(:v_decimal_map[:v_col_lower]::VARCHAR, ',', 2)::INTEGER;
            v_expr := 'TRY_CAST(' || :v_col || ' AS NUMBER(' ||
                       :v_precision::VARCHAR || ',' || :v_scale::VARCHAR || '))  AS ' || :v_col;

        ELSEIF :v_is_long THEN
            v_expr := 'TRY_CAST(' || :v_col || ' AS BIGINT)  AS ' || :v_col;

        ELSEIF :v_is_int THEN
            v_expr := 'TRY_CAST(' || :v_col || ' AS INTEGER)  AS ' || :v_col;

        ELSEIF :v_is_float THEN
            v_expr := 'TRY_CAST(' || :v_col || ' AS DOUBLE)  AS ' || :v_col;

        ELSEIF :v_is_timestamp THEN
            v_ts_format := :v_ts_map[:v_col_lower]::VARCHAR;
            IF :v_ts_format = 'AUTO' THEN
                v_expr := 'TRY_TO_TIMESTAMP_NTZ(' || :v_col || ')  AS ' || :v_col;
            ELSE
                -- Convert Java-style format to Snowflake format
                v_expr := 'TRY_TO_TIMESTAMP_NTZ(' || :v_col || ', ''' || :v_ts_format || ''')  AS ' || :v_col;
            END IF;

        ELSEIF :v_is_date THEN
            v_dt_format := :v_dt_map[:v_col_lower]::VARCHAR;
            IF :v_dt_format = 'AUTO' THEN
                v_expr := 'TRY_TO_DATE(' || :v_col || ')  AS ' || :v_col;
            ELSE
                v_expr := 'TRY_TO_DATE(' || :v_col || ', ''' || :v_dt_format || ''')  AS ' || :v_col;
            END IF;

        ELSEIF :v_is_boolean THEN
            -- Normalise multiple boolean representations to Snowflake BOOLEAN
            v_expr :=
                'CASE UPPER(TRIM(' || :v_col || ')) ' ||
                    'WHEN ''TRUE''  THEN TRUE ' ||
                    'WHEN ''FALSE'' THEN FALSE ' ||
                    'WHEN ''1''     THEN TRUE ' ||
                    'WHEN ''0''     THEN FALSE ' ||
                    'WHEN ''Y''     THEN TRUE ' ||
                    'WHEN ''N''     THEN FALSE ' ||
                    'WHEN ''YES''   THEN TRUE ' ||
                    'WHEN ''NO''    THEN FALSE ' ||
                    'ELSE NULL ' ||
                'END  AS ' || :v_col;

        ELSE
            -- No conversion: pass through as VARCHAR
            v_expr := :v_col;
        END IF;

        IF :v_i = 0 THEN
            v_select_parts := :v_expr;
        ELSE
            v_select_parts := :v_select_parts || ',\n    ' || :v_expr;
        END IF;

        v_i := v_i + 1;
    END WHILE;

    -- Append framework metadata columns
    v_select_parts := :v_select_parts || ',
    ''' || :p_run_id          || ''' AS run_id,
    ''' || :p_batch_id        || ''' AS batch_id,
    ''' || :p_chunk_id        || ''' AS chunk_id,
    ''' || :p_source_file_name || ''' AS source_file_name,
    ''' || :p_source_file_path || ''' AS source_file_path,
    CURRENT_TIMESTAMP()            AS batch_load_date';

    RETURN :v_select_parts;
END;
$$;


-- ---------------------------------------------------------------------------
-- UTIL.GENERATE_NULL_CHECK_PREDICATE
-- Builds a WHERE clause that is TRUE when any null-checked column is NULL.
-- Rows matching this predicate go to the reject table.
--
-- Example output:
--   (claim_id IS NULL OR member_id IS NULL OR service_date IS NULL)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.GENERATE_NULL_CHECK_PREDICATE(
    p_yaml_config VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generates a WHERE predicate for null-check reject routing'
AS
$$
DECLARE
    v_parts VARCHAR := '';
    v_col   VARCHAR;
    v_i     INTEGER := 0;
    v_count INTEGER;
BEGIN
    v_count := ARRAY_SIZE(p_yaml_config:stage2:fields_null_check);
    IF :v_count = 0 THEN
        RETURN 'FALSE';  -- no null checks configured: nothing to reject
    END IF;

    WHILE :v_i < :v_count DO
        v_col := p_yaml_config:stage2:fields_null_check[v_i]::VARCHAR;
        IF :v_i = 0 THEN
            v_parts := :v_col || ' IS NULL';
        ELSE
            v_parts := :v_parts || ' OR ' || :v_col || ' IS NULL';
        END IF;
        v_i := v_i + 1;
    END WHILE;

    RETURN '(' || :v_parts || ')';
END;
$$;


-- ---------------------------------------------------------------------------
-- UTIL.GENERATE_COMPOSITE_UNIQUE_PREDICATE
-- Builds reject predicate for composite unique violations.
-- Uses window function ROW_NUMBER to detect duplicates.
--
-- For each composite group [col_a, col_b], generates:
--   ROW_NUMBER() OVER (PARTITION BY col_a, col_b ORDER BY batch_load_date DESC) > 1
-- Rows where this is > 1 are duplicates → reject.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE UTIL.GENERATE_COMPOSITE_UNIQUE_PREDICATE(
    p_yaml_config VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generates CTE expressions for composite unique violation detection'
AS
$$
DECLARE
    v_groups     VARIANT;
    v_group      VARIANT;
    v_group_count INTEGER;
    v_col_count  INTEGER;
    v_parts      VARCHAR := '';
    v_partition  VARCHAR;
    v_col        VARCHAR;
    v_i          INTEGER := 0;
    v_j          INTEGER;
    v_cte_parts  VARCHAR := '';
BEGIN
    v_groups := p_yaml_config:stage2:fields_composite_unique_check;
    v_group_count := ARRAY_SIZE(:v_groups);

    IF :v_group_count = 0 THEN
        RETURN '';  -- no composite unique checks
    END IF;

    -- Build one window expression per group
    v_i := 0;
    WHILE :v_i < :v_group_count DO
        v_group     := :v_groups[v_i];
        v_col_count := ARRAY_SIZE(:v_group);
        v_partition := '';
        v_j := 0;
        WHILE :v_j < :v_col_count DO
            v_col := :v_group[v_j]::VARCHAR;
            IF :v_j = 0 THEN
                v_partition := :v_col;
            ELSE
                v_partition := :v_partition || ', ' || :v_col;
            END IF;
            v_j := v_j + 1;
        END WHILE;

        IF :v_i = 0 THEN
            v_cte_parts :=
                'ROW_NUMBER() OVER (PARTITION BY ' || :v_partition ||
                ' ORDER BY batch_load_date DESC) AS _dup_rn_' || :v_i::VARCHAR;
        ELSE
            v_cte_parts := :v_cte_parts ||
                ',\n    ROW_NUMBER() OVER (PARTITION BY ' || :v_partition ||
                ' ORDER BY batch_load_date DESC) AS _dup_rn_' || :v_i::VARCHAR;
        END IF;

        v_i := v_i + 1;
    END WHILE;

    RETURN :v_cte_parts;
END;
$$;
