# =============================================================================
# PHI Agentic Data Platform — Generated Airflow DAG
# =============================================================================
# Source:       src_customers_001 / Customer Master Data
# Domain:       customer
# Generated at: 2026-05-31T18:55:43.310539+00:00
# DO NOT EDIT MANUALLY.
# =============================================================================


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePythonJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)
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
SOURCE_GCS     = Variable.get(
    "phi_source_path_src_customers_001",
    default_var="gs://anz-cloud-migration-raw-ingestion/customers/"
)
BQ_DATASET     = "phi_bronze_customer"
BQ_TABLE       = "src_customers_001_raw"
SOURCE_ID      = "src_customers_001"

# GCS locations of generated artifacts and local staging paths.
PIPELINE_BUCKET        = f"{GCP_PROJECT}-generated-code"
PIPELINE_GCS_KEY       = "src_customers_001/v1/pipeline_src_customers_001.py"
# Append execution timestamp so concurrent Airflow workers don't collide on the same temp path.
PIPELINE_LOCAL         = f"/tmp/pipeline_src_customers_001_v1_{{ ts_nodash }}.py"
REQUIREMENTS_GCS_PATH  = f"gs://{GCP_PROJECT}-generated-code/src_customers_001/v1/requirements.txt"


with DAG(
    dag_id="phi_src_customers_001_customer_ingestion",
    default_args=DEFAULT_ARGS,
    description="PHI ingestion pipeline: Customer Master Data → phi_bronze_customer.src_customers_001_raw",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["phi", "customer", "ingestion", "src_customers_001"],
) as dag:

    # ── 1. Wait for source file (file-based sources only) ────────────
    wait_for_source = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_source_file",
        bucket=SOURCE_GCS.replace("gs://", "").split("/")[0],
        prefix="/".join(SOURCE_GCS.replace("gs://", "").split("/")[1:]),
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    # ── 2a. Download pipeline.py from GCS to the Airflow worker ─────
    # DataflowCreatePythonJobOperator needs a local file path.
    download_pipeline = GCSToLocalFilesystemOperator(
        task_id="download_pipeline_py",
        bucket=PIPELINE_BUCKET,
        object_name=PIPELINE_GCS_KEY,
        filename=PIPELINE_LOCAL,
    )

    # ── 2b. Submit pipeline to Dataflow (direct Python, no container) ─
    # Runs pipeline.py with --runner=DataflowRunner. The Dataflow
    # service stages the script and dependencies, spins up workers,
    # and executes the job — identical service to Flex Templates.
    run_id = "{{ ts_nodash }}"

    launch_pipeline = DataflowCreatePythonJobOperator(
        task_id="launch_dataflow_pipeline",
        py_file=PIPELINE_LOCAL,
        job_name=f"phi-src_customers_001-{{ ds_nodash }}",
        options={
            "source_path":           SOURCE_GCS,
            "bq_project":            GCP_PROJECT,
            "bq_dataset":            BQ_DATASET,
            "bq_table":              BQ_TABLE,
            "run_id":                run_id,
            "region":                GCP_REGION,
            "temp_location":         f"gs://{GCP_PROJECT}-dataflow-temp/temp",
            "staging_location":      f"gs://{GCP_PROJECT}-dataflow-temp/staging",
            "requirements_file":     REQUIREMENTS_GCS_PATH,
            "num_workers":           3,
            "max_num_workers":       6,
            "machine_type":          "e2-standard-4",
            "service_account_email": f"phi-dataflow-sa@{GCP_PROJECT}.iam.gserviceaccount.com",
            "subnetwork":            f"regions/{GCP_REGION}/subnetworks/default",
        },
        py_requirements=[
            "apache-beam[gcp]>=2.50.0",
            "google-cloud-lineage",
        ],
        py_system_site_packages=False,
        gcp_conn_id="google_cloud_default",
        project_id=GCP_PROJECT,
        location=GCP_REGION,
    )

    # ── 3. Data quality gate — verify rows landed in BQ ──────────────
    # NOTE: _PARTITIONTIME only works on date-partitioned tables.
    # If this table has no partition, replace the WHERE clause with a
    # column-based filter (e.g. WHERE DATE(created_at) = '{{ ds }}').
    dq_row_count_check = BigQueryCheckOperator(
        task_id="dq_row_count_check",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE DATE(_PARTITIONTIME) = '{{ ds }}'
        """,
        use_legacy_sql=False,
    )

    # ── 4. DLQ spike check — alert if > 5 % of rows failed ───────────
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
                WHERE DATE(timestamp) = '{{ ds }}'
            )
            SELECT dlq.cnt / NULLIF(total.cnt + dlq.cnt, 0) < 0.05
            FROM total, dlq
        """,
        use_legacy_sql=False,
    )

    # ── 5. SLA miss / failure alert ────────────────────────────────────
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

    # ── DAG dependency chain ────────────────────────────────────────────
    (
        wait_for_source >> download_pipeline >> launch_pipeline
        >> [dq_row_count_check, dlq_spike_check]
        >> alert_on_failure
    )
