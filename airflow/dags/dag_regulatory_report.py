from airflow import DAG

from waba_common import DEFAULT_ARGS, START_DATE, countries_arg, countries_param, spark_job

with DAG(
    dag_id="dag_regulatory_report",
    description="Agrégats BCEAO / CIMA de la journée J, produits à J+1 00h30 UTC",
    schedule="30 0 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=countries_param(),
    tags=["waba", "regulatory"],
) as dag:
    refresh_gold = spark_job("refresh_gold", "silver_to_gold.py",
                             ["--countries", countries_arg(), "--kpis", "npl_ratio_by_country"])
    report = spark_job("regulatory_report", "regulatory_report.py",
                       ["--report-date", "{{ ds }}", "--countries", countries_arg()])

    refresh_gold >> report
