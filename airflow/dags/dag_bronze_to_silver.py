from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from waba_common import DEFAULT_ARGS, START_DATE, countries_arg, countries_param, spark_job

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Nettoyage, dédoublonnage, enrichissement et conversion EUR (Silver)",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=countries_param(),
    tags=["waba", "silver"],
) as dag:
    silver = spark_job("bronze_to_silver", "bronze_to_silver.py", ["--countries", countries_arg()])
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="dag_silver_to_gold",
        conf={"countries": countries_arg()},
    )

    silver >> trigger_gold
