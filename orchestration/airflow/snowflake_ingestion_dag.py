"""
snowflake_ingestion_dag.py
==========================
Airflow DAG for orchestrating the Snowflake metadata-driven file ingestion
framework.

Purpose
-------
External DAG orchestration (Apache Airflow) option for the framework.
The DAG calls UTIL.MASTER_RUNNER via the SnowflakeOperator, passing
yaml_name, yaml_file_path, and process_type as the only parameters.

All pipeline logic (Stage1→Stage2→Stage3, chunking, logging, schema
validation) remains inside Snowflake. The DAG is a thin orchestration shell.

Design principles
-----------------
• One reusable DAG template — parameterised per dataset via Airflow Variables
  or DAG-level config.
• All business logic stays in Snowflake stored procedures.
• DAG only handles scheduling, retry, alerting, and dependency ordering.
• No ETL code in Python — only CALL statements.

Why DAG in addition to Snowflake Tasks?
---------------------------------------
Snowflake Tasks alone are sufficient for simple, isolated schedules.
Airflow (or any external DAG tool) adds value when:
  - Cross-system dependencies exist (e.g. wait for S3 files, then ingest)
  - Complex branching / conditional logic is needed
  - Centralised monitoring across Snowflake + other platforms is required
  - You need dynamic DAG generation from a dataset registry

Recommendation (opinionated)
-----------------------------
Use Snowflake Tasks for fully Snowflake-owned pipelines (preferred for
production-grade Snowflake-native deployments).
Use Airflow when cross-system orchestration or team-level DAG governance is
already established.
Hybrid: use Airflow to detect/trigger, delegate all compute to Snowflake.

Requirements
------------
    pip install apache-airflow apache-airflow-providers-snowflake
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Airflow connection ID pointing to the Snowflake target account.
# Configure this in Airflow Admin → Connections with:
#   Conn Type : Snowflake
#   Account   : <org>-<account>
#   Database  : INGESTION_FW
#   Schema    : UTIL
#   Warehouse : FW_WH
#   Role      : FW_EXECUTOR
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_ingestion_fw"

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email_on_retry": False,
}


# ---------------------------------------------------------------------------
# Helper: build a SnowflakeOperator that calls MASTER_RUNNER
# ---------------------------------------------------------------------------
def make_ingest_task(
    task_id: str,
    yaml_name: str,
    yaml_file_path: str,
    process_type: str = "file_ingestion",
    dag: DAG | None = None,
) -> SnowflakeOperator:
    """
    Create a SnowflakeOperator task that calls UTIL.MASTER_RUNNER.

    Parameters
    ----------
    task_id        : Airflow task ID (unique within the DAG)
    yaml_name      : Dataset YAML filename  (e.g. "claims_txt.yaml")
    yaml_file_path : Path within the config stage (e.g. "datasets/claims_txt.yaml")
    process_type   : Process type string (default: "file_ingestion")
    dag            : Parent DAG instance

    Returns
    -------
    SnowflakeOperator configured for the given dataset.
    """
    sql = f"""
        CALL INGESTION_FW.UTIL.MASTER_RUNNER(
            '{yaml_name}',
            '{yaml_file_path}',
            '{process_type}'
        );
    """
    return SnowflakeOperator(
        task_id=task_id,
        sql=sql,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        dag=dag,
    )


# ===========================================================================
# DAG 1: Claims TXT – Delta load (hourly)
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__claims_txt_delta",
    description="Hourly delta ingestion for claims TXT files via MASTER_RUNNER",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",    # every hour at :00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "claims", "delta"],
) as dag_claims:

    start = EmptyOperator(task_id="start")

    ingest_claims = make_ingest_task(
        task_id="ingest_claims_txt",
        yaml_name="claims_txt.yaml",
        yaml_file_path="datasets/claims_txt.yaml",
        dag=dag_claims,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> ingest_claims >> end


# ===========================================================================
# DAG 2: Orders CSV – Full load (daily at 02:00 UTC)
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__orders_csv_full",
    description="Daily full ingestion for orders CSV files via MASTER_RUNNER",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",    # daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "orders", "full"],
) as dag_orders:

    start_orders = EmptyOperator(task_id="start")

    ingest_orders = make_ingest_task(
        task_id="ingest_orders_csv",
        yaml_name="orders_csv.yaml",
        yaml_file_path="datasets/orders_csv.yaml",
        dag=dag_orders,
    )

    end_orders = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_orders >> ingest_orders >> end_orders


# ===========================================================================
# DAG 3: Customers Parquet – Delta load (every 4 hours)
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__customers_parquet_delta",
    description="Every-4-hours delta ingestion for customers Parquet files via MASTER_RUNNER",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */4 * * *",  # every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "customers", "delta"],
) as dag_customers:

    start_cust = EmptyOperator(task_id="start")

    ingest_customers = make_ingest_task(
        task_id="ingest_customers_parquet",
        yaml_name="customers_parquet.yaml",
        yaml_file_path="datasets/customers_parquet.yaml",
        dag=dag_customers,
    )

    end_cust = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_cust >> ingest_customers >> end_cust


# ===========================================================================
# DAG 4: Master pipeline – run all datasets in dependency order
# Demonstrates cross-dataset dependency: claims must complete before
# customer summary refresh.
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__master_pipeline",
    description="Orchestrates all dataset ingestions with dependency ordering",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",    # daily at 03:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "master"],
) as dag_master:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # Run orders and customers in parallel (no dependency between them)
    run_orders = make_ingest_task(
        task_id="ingest_orders_csv",
        yaml_name="orders_csv.yaml",
        yaml_file_path="datasets/orders_csv.yaml",
        dag=dag_master,
    )

    run_customers = make_ingest_task(
        task_id="ingest_customers_parquet",
        yaml_name="customers_parquet.yaml",
        yaml_file_path="datasets/customers_parquet.yaml",
        dag=dag_master,
    )

    # Claims runs after orders completes (downstream dependency example)
    run_claims = make_ingest_task(
        task_id="ingest_claims_txt",
        yaml_name="claims_txt.yaml",
        yaml_file_path="datasets/claims_txt.yaml",
        dag=dag_master,
    )

    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependency graph:
    #   pipeline_start → run_orders   ─┐
    #                                   ├→ run_claims → pipeline_end
    #   pipeline_start → run_customers ─┘
    pipeline_start >> [run_orders, run_customers] >> run_claims >> pipeline_end
