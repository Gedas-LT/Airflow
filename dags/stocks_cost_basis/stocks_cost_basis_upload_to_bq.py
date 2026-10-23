import json
from pathlib import Path

import pandas as pd
import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.sdk import Variable, dag, task
from google.cloud import bigquery

from modules.google_bigquery import upload_dataframe

# GLOBAL VARIABLES
ENV_NAME = Variable.get("env_name", default="dev")
GSHEET_SPREADSHEET_ID = "1pZuPIh4AHspm5I-bYLOPk0TRPO9BkFVnjBZ1bgroAT0"
GSHEET_RANGE = "Data"
BQ_DATASET_ID = "external__google_sheets"
BQ_TABLE_ID = "stocks_cost_basis"
GCP_CONN_ID = "google_cloud_default"
CONFIG_PATH = Path(__file__).with_name("config.json")

if ENV_NAME == "dev":
    BQ_PROJECT_ID = "gedas-analytics-dev"
elif ENV_NAME == "prod":
    BQ_PROJECT_ID = "gedas-analytics-prod"
else:
    raise ValueError(f"Unknown env_name={ENV_NAME!r}; expected 'dev' or 'prod'")


@dag(
    dag_id="stocks_cost_basis_upload_to_bq",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Vilnius"),
    schedule=None if ENV_NAME == "dev" else "0 3 * * *",
    catchup=False,
)
def stocks_cost_basis_upload_to_bq():

    @task
    def get_sheets_data() -> list[dict]:
        
        """Get the data from the Google Sheet and convert it to a list of dictionaries"""
        
        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            config = json.load(config_file)

        hook = GSheetsHook(gcp_conn_id=GCP_CONN_ID)
        values = hook.get_values(
            spreadsheet_id=GSHEET_SPREADSHEET_ID,
            range_=GSHEET_RANGE,
        )
        if not values:
            raise ValueError("Google Sheet returned no rows")

        header, *rows = values
        dataframe = pd.DataFrame(rows, columns=header)
        dataframe.columns = dataframe.columns.str.strip()
        dataframe = dataframe.rename(columns=config["sheet_to_bq_columns"])
        for col in ("quantity", "cost_basis"):
            dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce")
        
        # XCOM can only handle JSON, so we need to convert the dataframe to a list of dictionaries
        return dataframe.to_dict(orient="records")

    @task
    def upload_to_bq(records: list[dict]) -> None:
        
        """Upload the data to the BigQuery table"""
        
        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            config = json.load(config_file)

        dataframe = pd.DataFrame(records)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        project_id = BQ_PROJECT_ID
        schema = config["table"]["schema"]
        write_disposition = config["table"]["write_disposition"]
        gcp_conn_id = GCP_CONN_ID

        upload_dataframe(
            dataframe=dataframe,
            table_id=table_id,
            project_id=project_id,
            schema=schema,
            write_disposition=write_disposition,
            gcp_conn_id=GCP_CONN_ID
        )
        #print(f"Loaded {len(dataframe)} rows into {table_id}")

    upload_to_bq(get_sheets_data())


stocks_cost_basis_upload_to_bq()
