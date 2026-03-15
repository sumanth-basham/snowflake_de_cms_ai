"""
yaml_loader_sp.py
=================
Snowpark Python Stored Procedure: YAML Loading and Schema Validation.

Purpose
-------
This module is deployed as a Snowpark Python stored procedure inside Snowflake.
It handles the YAML loading and structural validation step that would otherwise
require a SQL+Python UDF approach.

Why Snowpark Python for this component?
────────────────────────────────────────
• YAML parsing requires the `pyyaml` library — no SQL-native equivalent.
• JSON Schema validation requires `jsonschema` — no SQL-native equivalent.
• Python gives the cleanest, most maintainable YAML→VARIANT conversion path.
• Once deployed as a Snowpark SP, it runs inside the Snowflake warehouse;
  no external Python compute is needed.
• Python reads the YAML file from a Snowflake internal stage using
  Snowpark's get_stream() API, which is cleaner than the $1 line-by-line
  approach in SQL.

What this SP does
-----------------
1. Read the YAML file from the internal config stage.
2. Parse it with PyYAML.
3. Validate the parsed structure against the JSON Schema contract.
4. Return the validated config as a Snowflake VARIANT (dict → JSON).
5. Log the validation event to CONTROL.YAML_EXECUTION_LOG.

SQL vs Snowpark boundary
────────────────────────
• This SP: YAML loading, structural validation, logging.
• SQL / Snowflake Scripting SPs: COPY INTO, MERGE, type conversions,
  reject inserts, INFER_SCHEMA discovery, Stage2/Stage3 execution.

Deployment
----------
Deploy this stored procedure by running:

    snowsql -q "
    CREATE OR REPLACE PROCEDURE UTIL.LOAD_AND_VALIDATE_YAML_SP(
        p_run_id        VARCHAR,
        p_yaml_name     VARCHAR,
        p_yaml_file_path VARCHAR
    )
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml', 'jsonschema')
    HANDLER = 'yaml_loader_sp.load_and_validate_yaml'
    IMPORTS = ('@UTIL.STG_FW_CONFIGS/yaml_loader_sp.py');
    "

Or use SnowSQL with the @stage deployment pattern:

    PUT file://framework/snowpark/yaml_loader_sp.py
        @UTIL.STG_FW_CONFIGS/
        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"""

from __future__ import annotations

import io
import json
import uuid
from datetime import datetime, timezone

import yaml
from jsonschema import Draft7Validator
from snowflake.snowpark import Session

# ---------------------------------------------------------------------------
# Framework JSON Schema contract (mirrors configs/schema.yaml)
# Keeping this embedded means the SP is self-contained; no external file read
# is needed for the schema definition itself.
# ---------------------------------------------------------------------------
FRAMEWORK_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Dataset YAML Validation Contract",
    "type": "object",
    "required": ["stage1", "stage2", "stage3"],
    "additionalProperties": False,
    "properties": {
        "stage1": {
            "type": "object",
            "required": [
                "source_file_path", "source_arrival_file_path", "file_pattern",
                "target_schema", "target_table", "load_type", "file_type", "read_options",
            ],
            "additionalProperties": False,
            "properties": {
                "source_file_path":             {"type": "string"},
                "source_arrival_file_path":     {"type": "string"},
                "file_pattern":                 {"type": "string"},
                "target_schema":                {"type": "string"},
                "target_table":                 {"type": "string"},
                "load_type":  {"type": "string", "enum": ["full", "delta", "adhoc"]},
                "file_type":  {"type": "string", "enum": ["csv", "txt", "parquet"]},
                "archive_files":                {"type": "boolean"},
                "zip_handling":                 {"type": "boolean"},
                "allow_zero_byte_files":        {"type": "boolean"},
                "header_special_chars_cleanup": {"type": "boolean"},
                "read_options": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "field_delimiter":               {"type": "string"},
                        "record_delimiter":              {"type": "string"},
                        "skip_header":      {"type": "integer", "minimum": 0},
                        "field_optionally_enclosed_by":  {"type": "string"},
                        "escape_unenclosed_field":        {"type": "string"},
                        "null_if":          {"type": "array", "items": {"type": "string"}},
                        "trim_space":       {"type": "boolean"},
                        "encoding":         {"type": "string"},
                        "snappy_compression": {"type": "boolean"},
                        "binary_as_text":   {"type": "boolean"},
                    },
                },
            },
        },
        "stage2": {
            "type": "object",
            "required": ["load_type", "target_schema", "target_table", "merge_util", "schema_changes"],
            "additionalProperties": False,
            "properties": {
                "load_type":      {"type": "string", "enum": ["full", "delta", "adhoc"]},
                "target_schema":  {"type": "string"},
                "target_table":   {"type": "string"},
                "primary_keys":   {"type": "array", "items": {"type": "string"}},
                "merge_util":     {"type": "string", "enum": ["yes", "no"]},
                "schema_changes": {"type": "string", "enum": ["yes", "no"]},
                "delete_column_name":  {"type": "string"},
                "delete_column_value": {"type": "string"},
                "fields_long_conversion":    {"type": "array", "items": {"type": "string"}},
                "fields_integer_conversion": {"type": "array", "items": {"type": "string"}},
                "fields_float_conversion":   {"type": "array", "items": {"type": "string"}},
                "fields_decimal_conversion": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["column_name", "precision", "scale"],
                        "additionalProperties": False,
                        "properties": {
                            "column_name": {"type": "string"},
                            "precision":   {"type": "integer", "minimum": 1, "maximum": 38},
                            "scale":       {"type": "integer", "minimum": 0, "maximum": 38},
                        },
                    },
                },
                "fields_timestamp_conversion": {"type": "array", "items": {"type": "string"}},
                "fields_date_conversion":      {"type": "array", "items": {"type": "string"}},
                "fields_boolean_conversion":   {"type": "array", "items": {"type": "string"}},
                "fields_null_check":           {"type": "array", "items": {"type": "string"}},
                "fields_composite_unique_check": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 2,
                    },
                },
            },
        },
        "stage3": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["name", "type", "value"],
                "additionalProperties": False,
                "properties": {
                    "name":  {"type": "string"},
                    "type":  {"type": "string", "enum": ["sql_query"]},
                    "value": {"type": "string"},
                },
            },
        },
    },
}


def _validate_schema(data: dict) -> list[str]:
    """Run JSON Schema validation; return list of error messages (empty = valid)."""
    validator = Draft7Validator(FRAMEWORK_SCHEMA)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    return [
        f"{' -> '.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in errors
    ]


def _log_yaml_validation(
    session: Session,
    yaml_exec_id: str,
    run_id: str,
    yaml_name: str,
    yaml_file_path: str,
    yaml_content_json: str,
    validation_status: str,
    errors_json: str,
) -> None:
    """Write a row to CONTROL.YAML_EXECUTION_LOG."""
    session.sql(
        """
        INSERT INTO CONTROL.YAML_EXECUTION_LOG (
            yaml_exec_id, run_id, yaml_name, yaml_file_path,
            yaml_content, schema_validation, validation_errors, loaded_at
        )
        SELECT
            :1, :2, :3, :4,
            PARSE_JSON(:5), :6, PARSE_JSON(:7),
            CURRENT_TIMESTAMP()
        """,
        params=[
            yaml_exec_id, run_id, yaml_name, yaml_file_path,
            yaml_content_json, validation_status, errors_json,
        ],
    ).collect()


def load_and_validate_yaml(
    session: Session,
    p_run_id: str,
    p_yaml_name: str,
    p_yaml_file_path: str,
) -> dict:
    """
    Snowpark Python Stored Procedure handler.

    Parameters
    ----------
    session          : Snowpark Session (injected by Snowflake at runtime)
    p_run_id         : Pipeline run UUID
    p_yaml_name      : Dataset YAML filename (e.g. 'claims_txt.yaml')
    p_yaml_file_path : Path within the config stage (e.g. 'datasets/claims_txt.yaml')

    Returns
    -------
    dict  →  Snowflake VARIANT
        {
          "status":       "SUCCESS" | "FAILED",
          "yaml_exec_id": "<uuid>",
          "yaml_variant": { ... },   # parsed YAML as JSON object
          "validation":   "PASSED" | "FAILED",
          "errors":       []         # list of error strings on failure
        }
    """
    yaml_exec_id = str(uuid.uuid4())

    # ------------------------------------------------------------------
    # 1. Read YAML file from internal Snowflake stage using Snowpark
    #    get_stream() reads a file from a named stage as a file-like object.
    # ------------------------------------------------------------------
    stage_path = f"@INGESTION_FW.UTIL.STG_FW_CONFIGS/{p_yaml_file_path}"
    try:
        file_stream = session.file.get_stream(stage_path)
        raw_text = io.TextIOWrapper(file_stream, encoding="utf-8").read()
    except Exception as exc:
        error_msg = f"YAML_LOAD_FAILED: cannot read '{stage_path}': {exc}"
        _log_yaml_validation(
            session, yaml_exec_id, p_run_id, p_yaml_name, p_yaml_file_path,
            "null", "FAILED", json.dumps([error_msg]),
        )
        return {
            "status": "FAILED",
            "yaml_exec_id": yaml_exec_id,
            "yaml_variant": None,
            "validation": "FAILED",
            "errors": [error_msg],
        }

    # ------------------------------------------------------------------
    # 2. Parse YAML text with PyYAML
    # ------------------------------------------------------------------
    try:
        parsed = yaml.safe_load(raw_text)
        if parsed is None:
            raise ValueError("YAML parsed to None — file may be empty")
    except Exception as exc:
        error_msg = f"YAML_PARSE_FAILED: {exc}"
        _log_yaml_validation(
            session, yaml_exec_id, p_run_id, p_yaml_name, p_yaml_file_path,
            "null", "FAILED", json.dumps([error_msg]),
        )
        return {
            "status": "FAILED",
            "yaml_exec_id": yaml_exec_id,
            "yaml_variant": None,
            "validation": "FAILED",
            "errors": [error_msg],
        }

    # Convert to JSON-safe dict (handles datetime objects, etc.)
    yaml_json_str = json.dumps(parsed, default=str)
    yaml_dict = json.loads(yaml_json_str)

    # ------------------------------------------------------------------
    # 3. Validate structure against the JSON Schema contract
    # ------------------------------------------------------------------
    errors = _validate_schema(yaml_dict)
    validation_status = "PASSED" if not errors else "FAILED"

    # ------------------------------------------------------------------
    # 4. Log the validation event
    # ------------------------------------------------------------------
    _log_yaml_validation(
        session, yaml_exec_id, p_run_id, p_yaml_name, p_yaml_file_path,
        yaml_json_str, validation_status, json.dumps(errors),
    )

    # ------------------------------------------------------------------
    # 5. Fail-fast on schema validation errors
    # ------------------------------------------------------------------
    if errors:
        raise RuntimeError(
            f"YAML_SCHEMA_VALIDATION_FAILED for '{p_yaml_name}'. "
            f"Errors: {' | '.join(errors)}"
        )

    return {
        "status": "SUCCESS",
        "yaml_exec_id": yaml_exec_id,
        "yaml_variant": yaml_dict,
        "validation": "PASSED",
        "errors": [],
    }
