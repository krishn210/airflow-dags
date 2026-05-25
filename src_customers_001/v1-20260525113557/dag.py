# =============================================================================
# PHI Agentic Data Platform — Generated Airflow DAG
# =============================================================================
# Source:       src_customers_001 / Customer Master Data
# Domain:       customer
# Generated at: 2026-05-18T20:19:19.857056Z
# DO NOT EDIT MANUALLY.
# =============================================================================


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


# Operational args driven by volume_class=MEDIUM
DEFAULT_ARGS = {
    "owner":             "phi-platform",
    "depends_on_past":   False,
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "sla":               timedelta(minutes=60),
}

GCP_PROJECT    = Variable.get("phi_gcp_project",    default_var="anz-cloud-migration")
GCP_REGION     = Variable.get("phi_gcp_region",     default_var="us-central1")
TEMPLATE_PATH  = Variable.get(
    "phi_template_src_customers_001",
    default_var=f"gs://anz-cloud-migration-generated-code/src_customers_001/template_metadata"
)
SOURCE_GCS     = Variable.get(
    "phi_source_path_src_customers_001",
    default_var="gs://anz-cloud-migration-raw-ingestion/customers/"
)
BQ_DATASET     = "phi_bronze_customer"
BQ_TABLE       = "src_customers_001_raw"
SOURCE_ID      = "src_customers_001"


with DAG(
    dag_id=f"phi_src_customers_001_customer_ingestion",
    default_args=DEFAULT_ARGS,
    description=(
        "PHI ingestion pipeline: Customer Master Data → "
        "phi_bronze_customer.src_customers_001_raw"
    ),
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      # Prevent overlapping runs
    tags=["phi", "customer", "ingestion", "src_customers_001"],
) as dag:

    # ── 1. Wait for source file (batch sources only) ────────────────────
    wait_for_source = GCSObjectExistenceSensor(
        task_id="wait_for_source_file",
        bucket=SOURCE_GCS.replace("gs://", "").split("/")[0],
        object="/".join(SOURCE_GCS.replace("gs://", "").split("/")[1:]),
        timeout=3600,          # Wait up to 1 hour for file arrival
        poke_interval=120,     # Check every 2 minutes
        mode="reschedule",     # Release worker slot while waiting
    )

    # ── 2. Launch Dataflow Flex Template ───────────────────────────────
    run_id = "{{ ds_nodash }}-{{ run_id[:8] }}"

    launch_pipeline = DataflowStartFlexTemplateOperator(
        task_id="launch_dataflow_pipeline",
        project_id=GCP_PROJECT,
        location=GCP_REGION,
        body={
            "launchParameter": {
                "jobName":         f"phi-src_customers_001-{{ds_nodash}}",
                "containerSpecGcsPath": TEMPLATE_PATH,
                "parameters": {
                    "source_path": SOURCE_GCS,
                    "bq_project":  GCP_PROJECT,
                    "bq_dataset":  BQ_DATASET,
                    "bq_table":    BQ_TABLE,
                    "run_id":      run_id,
                },
                "environment": {
                    "numWorkers":    3,
                    "maxWorkers":    6,
                    "serviceAccountEmail": (
                        f"phi-dataflow-sa@{GCP_PROJECT}.iam.gserviceaccount.com"
                    ),
                    "subnetwork": f"regions/{GCP_REGION}/subnetworks/default",
                },
            }
        },
        do_xcom_push=True,
    )

    # ── 3. Data quality gate — verify rows landed in BQ ─────────────────
    dq_row_count_check = BigQueryCheckOperator(
        task_id="dq_row_count_check",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
        """,
        use_legacy_sql=False,
    )

    # ── 4. DLQ spike check — alert if too many rows failed ──────────────
    dlq_spike_check = BigQueryCheckOperator(
        task_id="dlq_spike_check",
        sql=f"""
            WITH total AS (
                SELECT COUNT(*) AS cnt
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
                WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
            ),
            dlq AS (
                SELECT COUNT(*) AS cnt
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_dlq`
                WHERE DATE(TIMESTAMP(timestamp)) = '{{ ds }}'
            )
            SELECT dlq.cnt / NULLIF(total.cnt + dlq.cnt, 0) < 0.05
            FROM total, dlq
        """,
        use_legacy_sql=False,
    )

    # ── 5. SLA miss / failure alert ─────────────────────────────────────
    alert_on_failure = SlackWebhookOperator(
        task_id="alert_on_failure",
        http_conn_id="phi_slack_webhook",
        message=(
            f":x: PHI Pipeline FAILED\n"
            f"*Source:* Customer Master Data (`src_customers_001`)\n"
            f"*Domain:* customer\n"
            f"*Date:* {{ ds }}\n"
            f"*DAG:* {{ dag.dag_id }}\n"
            "Please check Dataflow console and DLQ table."
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── DAG dependency chain ─────────────────────────────────────────────
    (
        wait_for_source
        >> launch_pipeline
        >> [dq_row_count_check, dlq_spike_check]
        >> alert_on_failure
    )
