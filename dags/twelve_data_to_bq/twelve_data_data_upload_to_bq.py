from pathlib import Path

import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Variable, dag, task

# GLOBAL VARIABLES
ENV_NAME = Variable.get("env_name", default="dev")
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET_ID = "external__google_sheets"
BQ_TABLE_ID = "stocks_cost_basis"

if ENV_NAME == "dev":
    BQ_PROJECT_ID = "gedas-analytics-dev"
elif ENV_NAME == "prod":
    BQ_PROJECT_ID = "gedas-analytics-prod"
else:
    raise ValueError(f"Unknown env_name={ENV_NAME!r}; expected 'dev' or 'prod'")


@dag(
    dag_id="twelve_data_data_upload_to_bq",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Vilnius"),
    schedule=None,
    catchup=False,
)
def twelve_data_data_upload_to_bq():

    @task
    def get_symbols_from_bq() -> list[str]:

        """Get the symbols from the BigQuery table"""

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        client = hook.get_client(project_id=BQ_PROJECT_ID)
        symbols_df = client.query(
            f"""
            SELECT DISTINCT ticker AS symbol
            FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
            WHERE ticker IS NOT NULL
            """
        ).to_dataframe()
        
        symbols = symbols_df["symbol"].tolist()

        return symbols

    get_symbols_from_bq()

twelve_data_data_upload_to_bq()