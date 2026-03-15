"""
master_runner_sp.py
===================
Snowpark Python Stored Procedure: Master Pipeline Controller.

Purpose
───────
This module is the GCP/Snowpark version of the master runner. It is
deployed as a Snowpark Python stored procedure inside Snowflake.

Python acts as the **controller and orchestrator**; SQL performs all
warehouse operations (COPY INTO, MERGE, INFER_SCHEMA, reject inserts, etc.).

Why Snowpark Python for the master runner?
──────────────────────────────────────────
• Python provides cleaner conditional logic, exception handling, and
  state management than Snowflake Scripting for a multi-stage pipeline.
• Python can call session.sql(...) to execute any SQL, including
  CALL <stored_procedure>() for Stage1/Stage2/Stage3.
• Python makes it easy to chain stage results, pass context between stages,
  and implement restartable retry logic.
• The YAML loading and validation (yaml_loader_sp.py) is also in Python,
  so the master runner can directly use the parsed result dict.
• No Snowflake Scripting VARIANT juggling is needed for Python-to-Python calls.

SQL vs Python boundary (opinionated)
─────────────────────────────────────
Python controller does:
    ✓ Load and validate YAML (via yaml_loader_sp.load_and_validate_yaml)
    ✓ Generate dynamic SQL strings from YAML configuration
    ✓ Call session.sql(...) to execute SQL SPs (STAGE1, STAGE2, STAGE3)
    ✓ Manage stage execution order and error handling
    ✓ Write run-level log entries
    ✓ Pass context (batch_id, discovered columns) between stages

SQL / Snowflake Scripting SPs do:
    ✓ COPY INTO (Stage1 raw landing)
    ✓ INFER_SCHEMA (schema discovery)
    ✓ MERGE / INSERT / TRUNCATE (Stage2 write strategy)
    ✓ Type conversions (TRY_CAST, TRY_TO_DATE, CASE WHEN boolean)
    ✓ Reject inserts
    ✓ Stage3 CREATE OR REPLACE VIEW / TABLE
    ✓ Chunk planning and tracking

GCS-specific behaviour
───────────────────────
• YAML source_file_path and source_arrival_file_path values are Snowflake
  external stage paths (e.g. '@UTIL.STG_CLAIMS_TXT/arrival/'), backed by the
  GCS_INGESTION_INT storage integration.  Raw gcs:// URLs are never used in
  the YAML; the stage definition (ddl/02_external_stages.sql) encapsulates
  the GCS bucket URL and storage integration binding.
• The framework passes the stage path directly to COPY INTO and INFER_SCHEMA,
  so no URL resolution is needed at runtime.
• Snowpipe-ingested files arrive via @UTIL.STG_* stages. The p_snowpipe_mode
  flag skips Stage1 when set to True.

Deployment SQL
──────────────
    PUT file://framework/snowpark/master_runner_sp.py
        @UTIL.STG_FW_CONFIGS/
        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

    PUT file://framework/snowpark/yaml_loader_sp.py
        @UTIL.STG_FW_CONFIGS/
        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

    CREATE OR REPLACE PROCEDURE UTIL.MASTER_RUNNER_SP(
        p_yaml_name       VARCHAR,
        p_yaml_file_path  VARCHAR,
        p_process_type    VARCHAR,
        p_snowpipe_mode   BOOLEAN DEFAULT FALSE
    )
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python', 'pyyaml', 'jsonschema')
    HANDLER = 'master_runner_sp.master_runner'
    IMPORTS = (
        '@UTIL.STG_FW_CONFIGS/master_runner_sp.py',
        '@UTIL.STG_FW_CONFIGS/yaml_loader_sp.py'
    );

Usage examples
──────────────
    -- Standard execution (Stage1 + Stage2 + Stage3)
    CALL UTIL.MASTER_RUNNER_SP(
        'claims_txt.yaml',
        'datasets/claims_txt.yaml',
        'file_ingestion',
        FALSE
    );

    -- Post-Snowpipe mode (Stage1 already complete; run Stage2 + Stage3 only)
    CALL UTIL.MASTER_RUNNER_SP(
        'claims_txt.yaml',
        'datasets/claims_txt.yaml',
        'file_ingestion',
        TRUE
    );
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from snowflake.snowpark import Session

# Import the YAML loader from the co-deployed module.
# In Snowpark, IMPORTS files are available in sys.path via the _udf_code directory.
import yaml_loader_sp as yl


# ---------------------------------------------------------------------------
# Supported process types
# ---------------------------------------------------------------------------
SUPPORTED_PROCESS_TYPES = {"FILE_INGESTION"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_run_start(
    session: Session,
    run_id: str,
    yaml_name: str,
    yaml_file_path: str,
    process_type: str,
    load_type: str,
) -> None:
    session.sql(
        """
        INSERT INTO CONTROL.PIPELINE_RUN_LOG (
            run_id, yaml_name, yaml_file_path, process_type, load_type,
            status, start_ts
        )
        VALUES (:1, :2, :3, :4, :5, 'RUNNING', CURRENT_TIMESTAMP())
        """,
        params=[run_id, yaml_name, yaml_file_path, process_type, load_type],
    ).collect()


def _log_run_end(
    session: Session,
    run_id: str,
    status: str,
    rows_stage1: int,
    rows_valid: int,
    rows_rejected: int,
    stage1_status: str,
    stage2_status: str,
    stage3_status: str,
    error_message: str | None,
) -> None:
    session.sql(
        """
        UPDATE CONTROL.PIPELINE_RUN_LOG
        SET
            status               = :1,
            stage1_status        = :2,
            stage2_status        = :3,
            stage3_status        = :4,
            end_ts               = CURRENT_TIMESTAMP(),
            duration_seconds     = DATEDIFF('second', start_ts, CURRENT_TIMESTAMP()),
            rows_loaded_stage1   = :5,
            rows_valid_stage2    = :6,
            rows_rejected_stage2 = :7,
            error_message        = :8
        WHERE run_id = :9
        """,
        params=[
            status, stage1_status, stage2_status, stage3_status,
            rows_stage1, rows_valid, rows_rejected, error_message,
            run_id,
        ],
    ).collect()


def _call_sql_sp(session: Session, sp_call: str) -> dict:
    """
    Execute a CALL statement and return the result as a Python dict.

    Snowflake stored procedures return a single VARIANT column.
    session.sql(CALL ...).collect()[0][0] gives the JSON string.
    """
    rows = session.sql(sp_call).collect()
    if rows and rows[0][0]:
        raw = rows[0][0]
        if isinstance(raw, str):
            return json.loads(raw)
        return dict(raw)
    return {}


def _build_stage1_call(run_id: str, yaml_name: str, yaml_json: str) -> str:
    """
    Build the CALL SQL for UTIL.STAGE1_HANDLER.
    The YAML config is passed as a PARSE_JSON(...) VARIANT literal.
    """
    # Escape single quotes in the JSON string
    safe_json = yaml_json.replace("'", "''")
    return (
        f"CALL UTIL.STAGE1_HANDLER("
        f"'{run_id}', "
        f"'{yaml_name}', "
        f"PARSE_JSON('{safe_json}')"
        f")"
    )


def _build_stage2_call(
    run_id: str, yaml_name: str, yaml_json: str, stage1_ctx_json: str
) -> str:
    safe_yaml = yaml_json.replace("'", "''")
    safe_ctx = stage1_ctx_json.replace("'", "''")
    return (
        f"CALL UTIL.STAGE2_HANDLER("
        f"'{run_id}', "
        f"'{yaml_name}', "
        f"PARSE_JSON('{safe_yaml}'), "
        f"PARSE_JSON('{safe_ctx}')"
        f")"
    )


def _build_stage3_call(
    run_id: str, yaml_name: str, yaml_json: str, stage2_ctx_json: str
) -> str:
    safe_yaml = yaml_json.replace("'", "''")
    safe_ctx = stage2_ctx_json.replace("'", "''")
    return (
        f"CALL UTIL.STAGE3_HANDLER("
        f"'{run_id}', "
        f"'{yaml_name}', "
        f"PARSE_JSON('{safe_yaml}'), "
        f"PARSE_JSON('{safe_ctx}')"
        f")"
    )


# ---------------------------------------------------------------------------
# Main handler (called by Snowflake when the SP is invoked)
# ---------------------------------------------------------------------------

def master_runner(
    session: Session,
    p_yaml_name: str,
    p_yaml_file_path: str,
    p_process_type: str,
    p_snowpipe_mode: bool = False,
) -> dict:
    """
    Snowpark Python Master Pipeline Controller.

    Parameters
    ----------
    session          : Snowpark Session (injected by Snowflake)
    p_yaml_name      : Dataset YAML filename  (e.g. 'claims_txt.yaml')
    p_yaml_file_path : Config stage path      (e.g. 'datasets/claims_txt.yaml')
    p_process_type   : 'file_ingestion'
    p_snowpipe_mode  : When True, Stage1 is skipped (Snowpipe already landed
                       data into RAW); only Stage2 + Stage3 run.

    Returns
    -------
    dict  →  Snowflake VARIANT  (run summary)
    """
    run_id = str(uuid.uuid4())
    process_type = p_process_type.strip().upper()

    # -----------------------------------------------------------------------
    # 1. Validate process type
    # -----------------------------------------------------------------------
    if process_type not in SUPPORTED_PROCESS_TYPES:
        raise ValueError(
            f"UNSUPPORTED_PROCESS_TYPE: '{p_process_type}'. "
            f"Supported: {sorted(SUPPORTED_PROCESS_TYPES)}"
        )

    # -----------------------------------------------------------------------
    # 2. Load and validate YAML  (Python controller calls Python loader)
    # -----------------------------------------------------------------------
    yaml_result = yl.load_and_validate_yaml(
        session, run_id, p_yaml_name, p_yaml_file_path
    )
    if yaml_result["status"] != "SUCCESS":
        # YAML validation failed — no run log needed (logged by yaml_loader_sp)
        return {
            "run_id": run_id,
            "status": "FAILED",
            "yaml_name": p_yaml_name,
            "error": yaml_result.get("errors", ["YAML load failed"]),
        }

    yaml_dict: dict = yaml_result["yaml_variant"]
    yaml_json_str: str = json.dumps(yaml_dict)
    load_type: str = yaml_dict["stage1"]["load_type"]

    # -----------------------------------------------------------------------
    # 3. Log run start
    # -----------------------------------------------------------------------
    _log_run_start(session, run_id, p_yaml_name, p_yaml_file_path, process_type, load_type)

    # Execution state tracking
    rows_stage1: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    stage1_status = "SKIPPED" if p_snowpipe_mode else "PENDING"
    stage2_status = "PENDING"
    stage3_status = "PENDING"
    stage1_ctx: dict = {}
    stage2_ctx: dict = {}

    try:
        if process_type == "FILE_INGESTION":

            # ---------------------------------------------------------------
            # STAGE 1  –  Raw ingestion and source schema discovery
            # Skipped when p_snowpipe_mode=True (Snowpipe already loaded data)
            # ---------------------------------------------------------------
            if not p_snowpipe_mode:
                stage1_call = _build_stage1_call(run_id, p_yaml_name, yaml_json_str)
                stage1_ctx = _call_sql_sp(session, stage1_call)
                stage1_status = stage1_ctx.get("status", "FAILED")

                if stage1_status != "SUCCESS":
                    raise RuntimeError(
                        f"STAGE1_FAILED: {stage1_ctx.get('error', 'unknown error')}"
                    )

                rows_stage1 = int(stage1_ctx.get("rows_loaded", 0))

                # Early exit if no rows loaded
                if rows_stage1 == 0:
                    _log_run_end(
                        session, run_id, "SUCCESS", 0, 0, 0,
                        stage1_status, "SKIPPED", "SKIPPED",
                        "Stage1 loaded 0 rows. No files matched or source is empty.",
                    )
                    return {
                        "run_id": run_id,
                        "status": "SUCCESS",
                        "message": "No files loaded — pipeline completed with 0 rows",
                        "load_type": load_type,
                    }
            else:
                # In Snowpipe mode, build a minimal stage1_ctx from the most
                # recent SOURCE_SCHEMA_REGISTRY entry for this dataset.
                rows = session.sql(
                    """
                    SELECT TOP 1
                        schema_reg_id,
                        batch_id,
                        raw_table,
                        discovered_columns
                    FROM CONTROL.SOURCE_SCHEMA_REGISTRY
                    WHERE yaml_name = :1
                    ORDER BY discovered_at DESC
                    """,
                    params=[p_yaml_name],
                ).collect()
                if rows:
                    r = rows[0]
                    stage1_ctx = {
                        "status": "SUCCESS",
                        "batch_id": r["BATCH_ID"],
                        "raw_table": r["RAW_TABLE"],
                        "discovered_columns": json.loads(r["DISCOVERED_COLUMNS"]),
                    }
                    stage1_status = "SUCCESS"
                else:
                    raise RuntimeError(
                        "SNOWPIPE_MODE: No source schema registry entry found "
                        f"for '{p_yaml_name}'. Ensure Snowpipe has loaded at "
                        "least one file and schema discovery has been run."
                    )

            # ---------------------------------------------------------------
            # STAGE 2  –  Standardisation, validation, and target write
            # ---------------------------------------------------------------
            stage2_call = _build_stage2_call(
                run_id, p_yaml_name, yaml_json_str, json.dumps(stage1_ctx)
            )
            stage2_ctx = _call_sql_sp(session, stage2_call)
            stage2_status = stage2_ctx.get("status", "FAILED")

            if stage2_status != "SUCCESS":
                raise RuntimeError(
                    f"STAGE2_FAILED: {stage2_ctx.get('error', 'unknown error')}"
                )

            rows_valid = int(stage2_ctx.get("valid_rows", 0))
            rows_rejected = int(stage2_ctx.get("rejected_rows", 0))

            # ---------------------------------------------------------------
            # STAGE 3  –  Curated output creation and post-processing
            # ---------------------------------------------------------------
            stage3_call = _build_stage3_call(
                run_id, p_yaml_name, yaml_json_str, json.dumps(stage2_ctx)
            )
            stage3_ctx = _call_sql_sp(session, stage3_call)
            stage3_status = stage3_ctx.get("status", "FAILED")

            if stage3_status != "SUCCESS":
                # Stage3 failures are recorded as PARTIAL (Stage1+Stage2 data is safe)
                _log_run_end(
                    session, run_id, "PARTIAL",
                    rows_stage1, rows_valid, rows_rejected,
                    stage1_status, stage2_status, stage3_status,
                    stage3_ctx.get("error", "Stage3 failed"),
                )
                raise RuntimeError(
                    f"STAGE3_FAILED: {stage3_ctx.get('error', 'unknown error')}"
                )

    except Exception as exc:
        error_str = str(exc)
        _log_run_end(
            session, run_id,
            "PARTIAL" if "STAGE3_FAILED" in error_str else "FAILED",
            rows_stage1, rows_valid, rows_rejected,
            stage1_status, stage2_status, stage3_status,
            error_str,
        )
        raise

    # -----------------------------------------------------------------------
    # 4. Log successful completion
    # -----------------------------------------------------------------------
    _log_run_end(
        session, run_id, "SUCCESS",
        rows_stage1, rows_valid, rows_rejected,
        stage1_status, stage2_status, stage3_status,
        None,
    )

    return {
        "run_id": run_id,
        "yaml_name": p_yaml_name,
        "process_type": process_type,
        "load_type": load_type,
        "snowpipe_mode": p_snowpipe_mode,
        "status": "SUCCESS",
        "rows_stage1": rows_stage1,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "stage1_status": stage1_status,
        "stage2_status": stage2_status,
        "stage3_status": stage3_status,
    }
