from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="healthcheck",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup"],
)
def healthcheck():
    @task
    def ping() -> str:
        return "airflow 3 is up"

    ping()


healthcheck()