"""
snowflake_ingestion_dag.py
==========================
Airflow DAG for orchestrating the GCP + Snowflake metadata-driven file
ingestion framework.

Purpose
-------
External DAG orchestration (Apache Airflow) option for the GCP platform.
The DAG calls UTIL.MASTER_RUNNER_SP (Snowpark Python SP) via the
SnowflakeOperator, passing yaml_name, yaml_file_path, and process_type.

All pipeline logic (Stage1→Stage2→Stage3, chunking, logging, schema
validation) remains inside Snowflake. The DAG is a thin orchestration shell.

GCP-specific design additions
──────────────────────────────
• PubSubPullSensor detects when GCS files have been processed by Snowpipe
  before triggering Stage2 + Stage3 (post-Snowpipe mode).
• For scheduled batch loads (orders CSV), a standard cron trigger is used
  without Pub/Sub sensing (Tasks inside Snowflake handle the schedule).

Design principles
-----------------
• One reusable DAG template — parameterised per dataset.
• All business logic stays in Snowflake stored procedures (MASTER_RUNNER_SP).
• DAG handles scheduling, Pub/Sub sensing, retry, alerting, dependency ordering.
• No ETL code in Python — only CALL statements and cloud-native sensors.

Why DAG in addition to Snowflake Tasks?
---------------------------------------
Snowflake Tasks alone are sufficient for simple, isolated schedules.
Airflow adds value on GCP when:
  - GCS Pub/Sub event sensing is needed before triggering Snowflake execution
  - Cross-system dependencies exist (e.g. Cloud Composer → Dataflow → Snowflake)
  - Centralised monitoring across GCP + Snowflake platforms is required
  - Dynamic DAG generation from a GCS-backed dataset registry is needed

Recommendation (opinionated for GCP)
-------------------------------------
Use Snowflake Tasks for fully Snowflake-owned pipelines.
Use Airflow / Cloud Composer when:
  - GCS Pub/Sub event sensing is the trigger (hybrid: sense in Airflow, execute in Snowflake)
  - Cross-GCP-service dependencies exist
Hybrid: Airflow senses GCS events / manages dependencies; delegates all
compute to Snowflake MASTER_RUNNER_SP.

Requirements
------------
    pip install apache-airflow apache-airflow-providers-snowflake
    pip install apache-airflow-providers-google  # for PubSubPullSensor
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Airflow connection ID pointing to the Snowflake target account.
# Configure in Airflow Admin → Connections:
#   Conn Type : Snowflake
#   Account   : <org>-<account>
#   Database  : INGESTION_FW
#   Schema    : UTIL
#   Warehouse : FW_WH
#   Role      : FW_EXECUTOR
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_ingestion_fw"

# GCP connection ID (for PubSubPullSensor)
# Configure in Airflow Admin → Connections:
#   Conn Type : Google Cloud
#   Project   : my-gcp-project
# ---------------------------------------------------------------------------
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "my-gcp-project"
PUBSUB_SUBSCRIPTION = "snowflake-ingestion-sub"

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
# Helper: build a SnowflakeOperator that calls MASTER_RUNNER_SP (Snowpark)
# ---------------------------------------------------------------------------
def make_ingest_task(
    task_id: str,
    yaml_name: str,
    yaml_file_path: str,
    process_type: str = "file_ingestion",
    snowpipe_mode: bool = False,
    dag: DAG | None = None,
) -> SnowflakeOperator:
    """
    Create a SnowflakeOperator that calls UTIL.MASTER_RUNNER_SP.

    snowpipe_mode=True  → skip Stage1 (Snowpipe already loaded data to RAW).
    snowpipe_mode=False → run all three stages.
    """
    snowpipe_flag = "TRUE" if snowpipe_mode else "FALSE"
    sql = f"""
        CALL INGESTION_FW.UTIL.MASTER_RUNNER_SP(
            '{yaml_name}',
            '{yaml_file_path}',
            '{process_type}',
            {snowpipe_flag}
        );
    """
    return SnowflakeOperator(
        task_id=task_id,
        sql=sql,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        dag=dag,
    )


# ===========================================================================
# DAG 1: Claims TXT – Event-driven delta load via GCS Pub/Sub + Snowpipe
# ──────────────────────────────────────────────────────────────────────────
# Flow:
#   PubSubPullSensor (detects GCS file arrival messages)
#     → Stage2 + Stage3 via MASTER_RUNNER_SP(snowpipe_mode=True)
#
# Snowpipe auto-lands files into RAW as they arrive.
# This DAG runs Stage2 + Stage3 once Pub/Sub confirms file arrival.
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__claims_txt_delta_gcs",
    description="Event-driven delta ingestion for claims TXT via GCS Pub/Sub + Snowpipe",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",   # poll every 15 min; sensor exits early if no messages
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "claims", "delta", "gcp", "pubsub", "snowpipe"],
) as dag_claims:

    start = EmptyOperator(task_id="start")

    # Sense for GCS file-arrival Pub/Sub messages from the claims bucket
    sense_claims_files = PubSubPullSensor(
        task_id="sense_gcs_claims_files",
        project_id=GCP_PROJECT,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=10,
        ack_messages=True,          # ACK so messages are not re-delivered
        gcp_conn_id=GCP_CONN_ID,
        poke_interval=60,           # seconds between Pub/Sub polls
        timeout=300,                # give up after 5 min (no new files)
        mode="poke",
        dag=dag_claims,
    )

    # Stage2 + Stage3 only (Snowpipe already ran Stage1)
    run_stage2_stage3 = make_ingest_task(
        task_id="run_stage2_stage3_claims",
        yaml_name="claims_txt.yaml",
        yaml_file_path="datasets/claims_txt.yaml",
        snowpipe_mode=True,         # skip Stage1
        dag=dag_claims,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag_claims,
    )

    start >> sense_claims_files >> run_stage2_stage3 >> end


# ===========================================================================
# DAG 2: Orders CSV – Scheduled daily full load  (no Pub/Sub sensing needed)
# ──────────────────────────────────────────────────────────────────────────
# Orders arrive as a predictable daily batch drop. A Pub/Sub sensor adds no
# value here; a cron schedule is cleaner.
# MASTER_RUNNER_SP runs all three stages (Stage1 via COPY INTO, not Snowpipe).
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__orders_csv_full_gcs",
    description="Daily full ingestion for orders CSV from GCS via MASTER_RUNNER_SP",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",    # daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "orders", "full", "gcp"],
) as dag_orders:

    start_orders = EmptyOperator(task_id="start")

    ingest_orders = make_ingest_task(
        task_id="ingest_orders_csv",
        yaml_name="orders_csv.yaml",
        yaml_file_path="datasets/orders_csv.yaml",
        snowpipe_mode=False,
        dag=dag_orders,
    )

    end_orders = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag_orders,
    )

    start_orders >> ingest_orders >> end_orders


# ===========================================================================
# DAG 3: Customers Parquet – Event-driven delta load via Pub/Sub + Snowpipe
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__customers_parquet_delta_gcs",
    description="Event-driven delta ingestion for customers Parquet via GCS Pub/Sub + Snowpipe",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */4 * * *",  # every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "customers", "delta", "gcp", "pubsub", "snowpipe"],
) as dag_customers:

    start_cust = EmptyOperator(task_id="start")

    sense_customers = PubSubPullSensor(
        task_id="sense_gcs_customers_files",
        project_id=GCP_PROJECT,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=10,
        ack_messages=True,
        gcp_conn_id=GCP_CONN_ID,
        poke_interval=60,
        timeout=300,
        mode="poke",
        dag=dag_customers,
    )

    run_customers = make_ingest_task(
        task_id="run_stage2_stage3_customers",
        yaml_name="customers_parquet.yaml",
        yaml_file_path="datasets/customers_parquet.yaml",
        snowpipe_mode=True,
        dag=dag_customers,
    )

    end_cust = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag_customers,
    )

    start_cust >> sense_customers >> run_customers >> end_cust


# ===========================================================================
# DAG 4: Master pipeline – all datasets in dependency order
# ===========================================================================
with DAG(
    dag_id="snowflake_ingest__master_pipeline_gcs",
    description="Orchestrates all GCS dataset ingestions with dependency ordering",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "master", "gcp"],
) as dag_master:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    run_orders_full = make_ingest_task(
        task_id="ingest_orders_csv",
        yaml_name="orders_csv.yaml",
        yaml_file_path="datasets/orders_csv.yaml",
        snowpipe_mode=False,
        dag=dag_master,
    )

    run_customers_delta = make_ingest_task(
        task_id="run_stage2_stage3_customers",
        yaml_name="customers_parquet.yaml",
        yaml_file_path="datasets/customers_parquet.yaml",
        snowpipe_mode=True,
        dag=dag_master,
    )

    run_claims_delta = make_ingest_task(
        task_id="run_stage2_stage3_claims",
        yaml_name="claims_txt.yaml",
        yaml_file_path="datasets/claims_txt.yaml",
        snowpipe_mode=True,
        dag=dag_master,
    )

    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag_master,
    )

    # Dependency graph (orders and customers in parallel, claims downstream):
    #   pipeline_start → run_orders_full     ─┐
    #                                          ├→ run_claims_delta → pipeline_end
    #   pipeline_start → run_customers_delta ─┘
    pipeline_start >> [run_orders_full, run_customers_delta] >> run_claims_delta >> pipeline_end
