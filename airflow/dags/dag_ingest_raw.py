from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from waba_common import DEFAULT_ARGS, START_DATE, countries_arg, countries_param, spark_job

ALL_COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]


def has_new_files(countries):
    hook = S3Hook(aws_conn_id="minio_waba")
    bucket = Variable.get("raw_bucket", default_var="raw-landing")
    selected = ALL_COUNTRIES if countries == "ALL" else countries.split(",")
    prefixes = ["referentials/"] + [f"{c}/" for c in selected]
    found = sum(len(hook.list_keys(bucket_name=bucket, prefix=p) or []) for p in prefixes)
    print(f"{found} fichier(s) en attente dans {bucket}")
    return found > 0


with DAG(
    dag_id="dag_ingest_raw",
    description="Détecte les nouveaux fichiers MinIO et les charge en raw puis bronze",
    schedule="*/10 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=countries_param(),
    tags=["waba", "bronze"],
) as dag:
    detect = ShortCircuitOperator(
        task_id="detect_new_files",
        python_callable=has_new_files,
        op_kwargs={"countries": countries_arg()},
    )
    ingest = spark_job("ingest_raw", "ingest_raw.py", ["--countries", countries_arg()])
    bronze = spark_job("raw_to_bronze", "raw_to_bronze.py", ["--countries", countries_arg()])
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="dag_bronze_to_silver",
        conf={"countries": countries_arg()},
    )

    detect >> ingest >> bronze >> trigger_silver
