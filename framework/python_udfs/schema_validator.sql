-- =============================================================================
-- schema_validator.sql
-- Purpose : Python UDF that validates a parsed YAML VARIANT against the
--           framework schema contract (configs/schema.yaml).
--
-- Why Python : jsonschema is the industry-standard library for JSON Schema
--              validation. No equivalent exists in SQL.
--
-- Usage
--   SELECT UTIL.VALIDATE_YAML_SCHEMA(:yaml_variant) AS result
--   -- result VARIANT: { "valid": true } or { "valid": false, "errors": [...] }
--
-- The schema contract itself is embedded in this UDF as a Python dict
-- (mirrors configs/schema.yaml) so it is always in sync with deployment.
-- =============================================================================

USE DATABASE INGESTION_FW;
USE SCHEMA UTIL;

CREATE OR REPLACE FUNCTION UTIL.VALIDATE_YAML_SCHEMA(yaml_variant VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('jsonschema')
HANDLER = 'validate_yaml_schema'
COMMENT = 'Validates a parsed YAML VARIANT against the framework schema contract'
AS
$$
import json
from jsonschema import Draft7Validator, ValidationError

# ---------------------------------------------------------------------------
# Framework schema contract (mirrors configs/schema.yaml)
# This is the authoritative structural validation spec.
# Source-column existence is NOT validated here – that happens at runtime
# after Stage1 discovers file headers.
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
                "target_schema", "target_table", "load_type", "file_type", "read_options"
            ],
            "additionalProperties": False,
            "properties": {
                "source_file_path":         {"type": "string", "pattern": "^(@|gcs://)"},
                "source_arrival_file_path": {"type": "string", "pattern": "^(@|gcs://)"},
                "file_pattern":             {"type": "string"},
                "target_schema":            {"type": "string"},
                "target_table":             {"type": "string"},
                "load_type":   {"type": "string", "enum": ["full", "delta", "adhoc"]},
                "file_type":   {"type": "string", "enum": ["csv", "txt", "parquet"]},
                "archive_files":            {"type": "boolean"},
                "zip_handling":             {"type": "boolean"},
                "allow_zero_byte_files":    {"type": "boolean"},
                "header_special_chars_cleanup": {"type": "boolean"},
                "read_options": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "field_delimiter":              {"type": "string"},
                        "record_delimiter":             {"type": "string"},
                        "skip_header":      {"type": "integer", "minimum": 0},
                        "field_optionally_enclosed_by": {"type": "string"},
                        "escape_unenclosed_field":       {"type": "string"},
                        "null_if":          {"type": "array", "items": {"type": "string"}},
                        "trim_space":       {"type": "boolean"},
                        "encoding":         {"type": "string"},
                        "snappy_compression": {"type": "boolean"},
                        "binary_as_text":   {"type": "boolean"}
                    }
                }
            }
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
                "fields_long_conversion":      {"type": "array", "items": {"type": "string"}},
                "fields_integer_conversion":   {"type": "array", "items": {"type": "string"}},
                "fields_float_conversion":     {"type": "array", "items": {"type": "string"}},
                "fields_decimal_conversion": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["column_name", "precision", "scale"],
                        "additionalProperties": False,
                        "properties": {
                            "column_name": {"type": "string"},
                            "precision":   {"type": "integer", "minimum": 1, "maximum": 38},
                            "scale":       {"type": "integer", "minimum": 0, "maximum": 38}
                        }
                    }
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
                        "minItems": 2
                    }
                }
            }
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
                    "value": {"type": "string"}
                }
            }
        }
    }
}


def validate_yaml_schema(yaml_variant) -> dict:
    """
    Validate a parsed YAML structure against the framework schema contract.

    Parameters
    ----------
    yaml_variant : dict | list | None
        The YAML structure as a Python object (Snowflake passes VARIANT as dict).

    Returns
    -------
    dict
        { "valid": True } on success.
        { "valid": False, "errors": ["<path>: <message>", ...] } on failure.
    """
    if yaml_variant is None:
        return {"valid": False, "errors": ["Input YAML variant is null"]}

    # Snowflake passes VARIANT as a Python dict/list
    if isinstance(yaml_variant, str):
        try:
            yaml_variant = json.loads(yaml_variant)
        except json.JSONDecodeError as e:
            return {"valid": False, "errors": [f"Cannot decode input: {str(e)}"]}

    validator = Draft7Validator(FRAMEWORK_SCHEMA)
    errors = sorted(validator.iter_errors(yaml_variant), key=lambda e: list(e.path))

    if not errors:
        return {"valid": True, "errors": []}

    error_messages = []
    for err in errors:
        path = " -> ".join(str(p) for p in err.absolute_path) or "<root>"
        error_messages.append(f"{path}: {err.message}")

    return {"valid": False, "errors": error_messages}
$$;
