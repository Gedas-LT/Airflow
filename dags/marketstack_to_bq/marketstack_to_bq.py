import json
from pathlib import Path

import pandas as pd
import pendulum
import requests
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Variable, dag, task

from modules.google_bigquery import delete_from_table, get_client, upload_dataframe

# GLOBAL VARIABLES
ENV_NAME = Variable.get("env_name", default="dev")
GCP_CONN_ID = "google_cloud_default"
SHEETS_BQ_DATASET_ID = "external__google_sheets"
COST_BASIS_BQ_TABLE_ID = "stocks_cost_basis"
MARKETSTACK_BQ_DATASET_ID = "raw__marketstack"
EOD_BQ_TABLE_ID = "stocks_eod_prices"
MARKETSTACK_API_KEY = Variable.get("marketstack_api_key")
CONFIG_PATH = Path(__file__).with_name("config.json")

if ENV_NAME == "dev":
    BQ_PROJECT_ID = "gedas-analytics-dev"
elif ENV_NAME == "prod":
    BQ_PROJECT_ID = "gedas-analytics-prod"
else:
    raise ValueError(f"Unknown env_name={ENV_NAME!r}; expected 'dev' or 'prod'")


@dag(
    dag_id="marketstack_data_upload_to_bq",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Vilnius"),
    schedule=None,
    catchup=False,
)
def marketstack_data_upload_to_bq():

    @task
    def get_symbols_from_bq() -> list[str]:

        """Get the symbols from the BigQuery table"""

        client = get_client(project_id=BQ_PROJECT_ID, gcp_conn_id=GCP_CONN_ID)
        symbols_df = client.query(
            f"""
            SELECT DISTINCT ticker AS symbol
            FROM `{BQ_PROJECT_ID}.{SHEETS_BQ_DATASET_ID}.{COST_BASIS_BQ_TABLE_ID}`
            WHERE ticker IS NOT NULL
            """
        ).to_dataframe()
        
        symbols = symbols_df["symbol"].tolist()

        return symbols

    @task
    def get_marketstack_data(symbols: list[str]) -> list[dict]:

        """Get the data from the Marketstack API
        
        Keyword arguments:
        symbols -- list of symbols to get data for
        """

        params = {
            "access_key": MARKETSTACK_API_KEY,
            "symbols": ",".join(symbols),
            "limit": max(len(symbols), 1),
        }
        
        response = requests.get("https://api.marketstack.com/v2/eod/latest", params=params)
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])

        dataframe = pd.DataFrame(payload["data"])[
            ["date", "name", "symbol", "close", "price_currency", "exchange_code"]
        ]
        
        dataframe["date"] = pd.to_datetime(dataframe["date"], utc=True).dt.date
        dataframe["exchange_code"] = dataframe["exchange_code"].astype(object).fillna(None)
        dataframe = dataframe.rename(columns={"close": "close_price"})

        # XCOM can only handle JSON, so we need to convert the dataframe to a list of dictionaries
        return dataframe.to_dict(orient="records")

    @task
    def upload_dataframe_to_bq(records: list[dict]) -> None:

        """Upload the dataframe to the BigQuery table"""

        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            config = json.load(config_file)

        dataframe = pd.DataFrame(records)
        table_id = f"{BQ_PROJECT_ID}.{MARKETSTACK_BQ_DATASET_ID}.{EOD_BQ_TABLE_ID}"
        project_id = BQ_PROJECT_ID
        schema = config["table"]["schema"]
        write_disposition = config["table"]["write_disposition"]

        date_value = dataframe["date"].iloc[0]
        where_clause = f"WHERE date = '{date_value}'"
        
        delete_from_table(
            table_id=table_id,
            project_id=project_id,
            where_clause=where_clause,
            gcp_conn_id=GCP_CONN_ID,
        )

        upload_dataframe(
            dataframe=dataframe,
            table_id=table_id,
            project_id=project_id,
            schema=schema,
            write_disposition=write_disposition,
            gcp_conn_id=GCP_CONN_ID,
        )

    symbols = get_symbols_from_bq()
    dataframe = get_marketstack_data(symbols)
    upload_dataframe_to_bq(dataframe)

marketstack_data_upload_to_bq()