from airflow import DAG

from waba_common import DEFAULT_ARGS, START_DATE, countries_arg, countries_param, spark_job

with DAG(
    dag_id="dag_silver_to_gold",
    description="Calcul des 7 KPIs financiers et réglementaires (Gold)",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=countries_param(),
    tags=["waba", "gold"],
) as dag:
    spark_job("silver_to_gold", "silver_to_gold.py", ["--countries", countries_arg()])
