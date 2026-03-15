"""
validate_yaml.py
================
Unit tests for YAML structure validation.

These tests validate that:
  1. Valid dataset YAML files pass the schema contract (configs/schema.yaml).
  2. Invalid YAML structures are correctly rejected with meaningful errors.
  3. The schema_validator Python UDF logic works as expected in isolation.

The schema validation logic from framework/python_udfs/schema_validator.sql
is replicated here in pure Python for local testing — no Snowflake connection
is required.

Run with:
    pip install pyyaml jsonschema pytest
    pytest tests/validate_yaml.py -v
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent
CONFIGS_DIR = REPO_ROOT / "configs"
DATASETS_DIR = CONFIGS_DIR / "datasets"

# ---------------------------------------------------------------------------
# Framework schema contract (mirrors framework/python_udfs/schema_validator.sql)
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
                        "field_delimiter":              {"type": "string"},
                        "record_delimiter":             {"type": "string"},
                        "skip_header":      {"type": "integer", "minimum": 0},
                        "field_optionally_enclosed_by": {"type": "string"},
                        "escape_unenclosed_field":       {"type": "string"},
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


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def validate(data: dict) -> list[str]:
    """Run jsonschema validation and return a list of error messages."""
    validator = Draft7Validator(FRAMEWORK_SCHEMA)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    return [
        f"{' -> '.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in errors
    ]


def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ===========================================================================
# Tests: sample dataset YAML files (should all pass)
# ===========================================================================
class TestValidDatasetYamls:
    """All dataset YAML files in configs/datasets/ must pass the schema."""

    @pytest.mark.parametrize(
        "yaml_file",
        list(DATASETS_DIR.glob("*.yaml")) if DATASETS_DIR.exists() else [],
    )
    def test_dataset_yaml_is_valid(self, yaml_file: Path):
        data = load_yaml(yaml_file)
        errors = validate(data)
        assert errors == [], (
            f"{yaml_file.name} has {len(errors)} validation error(s):\n"
            + "\n".join(errors)
        )

    def test_claims_txt_stage_sections_present(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        assert "stage1" in data
        assert "stage2" in data
        assert "stage3" in data

    def test_claims_txt_load_type_is_delta(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        assert data["stage1"]["load_type"] == "delta"
        assert data["stage2"]["load_type"] == "delta"

    def test_claims_txt_stage3_is_list_with_name_type_value(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        for item in data["stage3"]:
            assert "name" in item
            assert "type" in item
            assert "value" in item
            assert item["type"] == "sql_query"

    def test_orders_csv_load_type_is_full(self):
        data = load_yaml(DATASETS_DIR / "orders_csv.yaml")
        assert data["stage1"]["load_type"] == "full"
        assert data["stage2"]["load_type"] == "full"

    def test_orders_csv_merge_util_is_yes(self):
        data = load_yaml(DATASETS_DIR / "orders_csv.yaml")
        assert data["stage2"]["merge_util"] == "yes"

    def test_orders_csv_primary_keys_present(self):
        data = load_yaml(DATASETS_DIR / "orders_csv.yaml")
        assert len(data["stage2"]["primary_keys"]) > 0

    def test_customers_parquet_file_type(self):
        data = load_yaml(DATASETS_DIR / "customers_parquet.yaml")
        assert data["stage1"]["file_type"] == "parquet"

    def test_customers_parquet_schema_changes_yes(self):
        data = load_yaml(DATASETS_DIR / "customers_parquet.yaml")
        assert data["stage2"]["schema_changes"] == "yes"

    def test_decimal_conversion_has_precision_and_scale(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        for dec in data["stage2"].get("fields_decimal_conversion", []):
            assert "column_name" in dec
            assert "precision" in dec
            assert "scale" in dec
            assert 1 <= dec["precision"] <= 38
            assert 0 <= dec["scale"] <= 38

    def test_composite_unique_check_has_at_least_two_columns(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        for group in data["stage2"].get("fields_composite_unique_check", []):
            assert isinstance(group, list)
            assert len(group) >= 2

    def test_date_format_syntax(self):
        """Date fields may use 'col#format' encoding – value must be a string."""
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        for entry in data["stage2"].get("fields_date_conversion", []):
            assert isinstance(entry, str), f"Expected string, got {type(entry)}: {entry}"
            if "#" in entry:
                col, fmt = entry.split("#", 1)
                assert len(col) > 0, "Column name before '#' must not be empty"
                assert len(fmt) > 0, "Format mask after '#' must not be empty"

    def test_timestamp_format_syntax(self):
        data = load_yaml(DATASETS_DIR / "customers_parquet.yaml")
        for entry in data["stage2"].get("fields_timestamp_conversion", []):
            assert isinstance(entry, str)
            if "#" in entry:
                col, fmt = entry.split("#", 1)
                assert len(col) > 0
                assert len(fmt) > 0


# ===========================================================================
# Tests: schema validation (invalid YAML should fail)
# ===========================================================================
class TestSchemaValidationRejectsInvalidYaml:

    def test_missing_stage1(self):
        data = {"stage2": {}, "stage3": []}
        errors = validate(data)
        assert any("stage1" in e for e in errors), f"Expected stage1 error, got: {errors}"

    def test_missing_stage2(self):
        data = {"stage1": {}, "stage3": []}
        errors = validate(data)
        assert any("stage2" in e for e in errors), f"Expected stage2 error, got: {errors}"

    def test_missing_stage3(self):
        data = {"stage1": {}, "stage2": {}}
        errors = validate(data)
        assert any("stage3" in e for e in errors), f"Expected stage3 error, got: {errors}"

    def test_invalid_load_type(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage1"]["load_type"] = "streaming"   # invalid
        errors = validate(data)
        assert any("load_type" in e for e in errors), f"Expected load_type error, got: {errors}"

    def test_invalid_file_type(self):
        data = load_yaml(DATASETS_DIR / "orders_csv.yaml")
        data["stage1"]["file_type"] = "excel"        # invalid
        errors = validate(data)
        assert any("file_type" in e for e in errors), f"Expected file_type error, got: {errors}"

    def test_invalid_merge_util(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage2"]["merge_util"] = "true"        # should be "yes" or "no"
        errors = validate(data)
        assert any("merge_util" in e for e in errors), f"Expected merge_util error, got: {errors}"

    def test_invalid_schema_changes(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage2"]["schema_changes"] = "maybe"  # invalid
        errors = validate(data)
        assert any("schema_changes" in e for e in errors), \
            f"Expected schema_changes error, got: {errors}"

    def test_decimal_missing_precision(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage2"]["fields_decimal_conversion"] = [
            {"column_name": "billed_amount", "scale": 2}  # missing precision
        ]
        errors = validate(data)
        assert len(errors) > 0, "Expected validation error for missing precision"

    def test_decimal_precision_out_of_range(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage2"]["fields_decimal_conversion"] = [
            {"column_name": "billed_amount", "precision": 99, "scale": 2}  # > 38
        ]
        errors = validate(data)
        assert len(errors) > 0, "Expected validation error for precision > 38"

    def test_stage3_empty_list(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage3"] = []   # minItems: 1
        errors = validate(data)
        assert len(errors) > 0, "Expected validation error for empty stage3 list"

    def test_stage3_item_missing_value(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage3"] = [{"name": "test", "type": "sql_query"}]  # missing value
        errors = validate(data)
        assert len(errors) > 0, "Expected validation error for missing stage3 value"

    def test_stage3_unknown_action_type(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage3"] = [
            {"name": "test", "type": "python_script", "value": "print('x')"}
        ]
        errors = validate(data)
        assert any("type" in e for e in errors), \
            f"Expected type error for unsupported action type, got: {errors}"

    def test_unknown_top_level_key(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage4"] = {}  # additionalProperties: false
        errors = validate(data)
        assert len(errors) > 0, "Expected error for unknown top-level key 'stage4'"

    def test_composite_unique_check_single_item(self):
        data = load_yaml(DATASETS_DIR / "claims_txt.yaml")
        data["stage2"]["fields_composite_unique_check"] = [["claim_id"]]  # minItems: 2
        errors = validate(data)
        assert len(errors) > 0, "Expected error for composite group with only 1 column"


# ===========================================================================
# Tests: YAML format-mask parsing logic
# ===========================================================================
class TestFormatMaskParsing:
    """Validate the '#format' syntax parsing logic used in sql_generator."""

    def parse_format_entry(self, entry: str) -> tuple[str, str]:
        """Replicate the SPLIT_PART logic from sql_generator.sql."""
        if "#" in entry:
            parts = entry.split("#", 1)
            return parts[0], parts[1]
        return entry, "AUTO"

    def test_date_with_format(self):
        col, fmt = self.parse_format_entry("order_date#MM/dd/yyyy")
        assert col == "order_date"
        assert fmt == "MM/dd/yyyy"

    def test_date_without_format(self):
        col, fmt = self.parse_format_entry("service_date")
        assert col == "service_date"
        assert fmt == "AUTO"

    def test_timestamp_iso_format(self):
        col, fmt = self.parse_format_entry("created_at#yyyy-MM-dd'T'HH:mm:ss")
        assert col == "created_at"
        assert fmt == "yyyy-MM-dd'T'HH:mm:ss"

    def test_timestamp_without_format(self):
        col, fmt = self.parse_format_entry("ingestion_ts")
        assert col == "ingestion_ts"
        assert fmt == "AUTO"


# ===========================================================================
# Tests: Boolean normalization values
# ===========================================================================
class TestBooleanNormalization:
    """Validate that the boolean normalization CASE expression covers all cases."""

    ACCEPTED_TRUE  = {"TRUE", "true", "1", "Y", "y", "YES", "yes"}
    ACCEPTED_FALSE = {"FALSE", "false", "0", "N", "n", "NO", "no"}

    def normalize(self, raw: str) -> bool | None:
        """Replicate Snowflake CASE UPPER(TRIM(col)) normalization."""
        val = raw.strip().upper()
        if val in {"TRUE", "1", "Y", "YES"}:
            return True
        if val in {"FALSE", "0", "N", "NO"}:
            return False
        return None

    @pytest.mark.parametrize("raw", list(ACCEPTED_TRUE))
    def test_truthy_values(self, raw: str):
        assert self.normalize(raw) is True, f"Expected True for '{raw}'"

    @pytest.mark.parametrize("raw", list(ACCEPTED_FALSE))
    def test_falsy_values(self, raw: str):
        assert self.normalize(raw) is False, f"Expected False for '{raw}'"

    def test_unknown_value_returns_none(self):
        assert self.normalize("MAYBE") is None
        assert self.normalize("") is None
        assert self.normalize("2") is None
