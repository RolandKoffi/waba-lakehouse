import logging
from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = "/opt/jobs"
JARS = ",".join(f"/opt/spark-jars/{j}" for j in [
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    "iceberg-aws-bundle-1.6.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
])

log = logging.getLogger("waba.alerts")


def notify_failure(context):
    ti = context["task_instance"]
    log.error(
        '{"alert":"task_failed","dag":"%s","task":"%s","run_id":"%s","try":%s,"log_url":"%s"}',
        ti.dag_id, ti.task_id, context["run_id"], ti.try_number, ti.log_url,
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
    "email_on_failure": False,
}

START_DATE = datetime(2026, 1, 1)


def countries_param():
    return {"countries": Param("ALL", type="string",
                               description="Liste de pays séparés par des virgules (ex: CI,SN) ou ALL")}


def countries_arg():
    return "{{ params.countries }}"


def spark_job(task_id, script, args=None, cores="2"):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_waba",
        application=f"{JOBS}/{script}",
        application_args=args or [],
        jars=JARS,
        name=f"waba_{task_id}",
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.cores.max": cores,
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
        },
        env_vars={"PYTHONPATH": JOBS},
        execution_timeout=timedelta(hours=1),
    )
