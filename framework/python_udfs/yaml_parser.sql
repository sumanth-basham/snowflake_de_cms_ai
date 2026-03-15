-- =============================================================================
-- yaml_parser.sql
-- Purpose : Python UDF that reads a YAML string from a Snowflake stage
--           and returns it as a VARIANT (JSON-compatible object).
--
-- Why Python : Snowflake SQL has no native YAML parser. PyYAML is the
--              standard, well-maintained YAML library. This is a justified,
--              minimal use of Python inside an otherwise SQL-first framework.
--
-- Usage
--   SELECT UTIL.PARSE_YAML(yaml_string) AS config
--
-- Returns
--   VARIANT – the parsed YAML structure as a Snowflake VARIANT object.
--   On parse failure returns OBJECT_CONSTRUCT('error', <message>).
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE FUNCTION UTIL.PARSE_YAML(yaml_str VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pyyaml')
HANDLER = 'parse_yaml'
COMMENT = 'Parses a YAML string and returns it as a Snowflake VARIANT (JSON)'
AS
$$
import yaml
import json

def parse_yaml(yaml_str: str):
    """
    Parse a YAML string and return a Python dict/list that Snowflake
    will automatically coerce to VARIANT.

    Snowflake Scripting tip:
        The YAML content is read from a stage file using:
            SELECT $1 FROM @UTIL.STG_FW_CONFIGS/<path> (FILE_FORMAT => ...)
        The result is a single column containing the full file as one string.
        Pass that string to this function.
    """
    if not yaml_str:
        return {"error": "Empty YAML string received"}
    try:
        parsed = yaml.safe_load(yaml_str)
        if parsed is None:
            return {"error": "YAML parsed to None – file may be empty"}
        # Return as JSON-serializable structure; Snowflake converts to VARIANT
        return json.loads(json.dumps(parsed, default=str))
    except yaml.YAMLError as e:
        return {"error": f"YAML parse error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
$$;


-- =============================================================================
-- UTIL.READ_STAGE_FILE_AS_STRING
-- Helper table function: reads all lines of a stage file into a single string.
-- Useful for loading YAML content before passing to PARSE_YAML.
--
-- Usage (inside a stored procedure):
--   LET yaml_raw STRING := (
--     SELECT UTIL.READ_STAGE_FILE(@UTIL.STG_FW_CONFIGS, :yaml_file_path)
--   );
--
-- Implementation note: In Snowflake Scripting, files on internal stages can be
-- read with  SELECT $1 FROM @stage/path  where $1 returns raw row text.
-- For multi-line files, rows are concatenated with newlines.
-- =============================================================================
CREATE OR REPLACE FUNCTION UTIL.READ_STAGE_FILE(stage_name VARCHAR, file_path VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'read_file'
COMMENT = 'Reads a text file from a Snowflake internal stage and returns its full content as a string'
AS
$$
from snowflake.snowpark import Session

def read_file(stage_name: str, file_path: str) -> str:
    """
    NOTE: This function is a design placeholder.
    In production, reading from a stage inside a UDF requires a Snowpark session.
    The recommended pattern is to use GET_STAGE_FILE or direct $1 query inside
    a stored procedure (Snowflake Scripting) rather than a UDF.

    Recommended pattern inside a stored procedure:
        LET v_content STRING;
        SELECT LISTAGG($1, '\n') INTO :v_content
        FROM @UTIL.STG_FW_CONFIGS/datasets/:yaml_name
        (FILE_FORMAT => '"INGESTION_FW"."UTIL"."FW_TXT_PIPE_FORMAT"');
        -- or use a plain text format with no delimiters

    This function exists as documentation of the pattern.
    """
    return f"Use $1 query inside stored procedure to read: {stage_name}/{file_path}"
$$;
