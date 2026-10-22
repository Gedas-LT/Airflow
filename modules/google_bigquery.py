from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


def upload_dataframe(
    dataframe,
    table_id: str,
    project_id: str,
    schema: list,
    write_disposition: str,
    gcp_conn_id: str = "google_cloud_default",
) -> None:
    """Upload a dataframe to a BigQuery table.
    
    Keyword arguments:
    dataframe -- The dataframe to upload
    table_id -- The ID of the BigQuery table
    project_id -- The ID of the BigQuery project
    schema -- The schema of the BigQuery table
    write_disposition -- The write disposition of the BigQuery table
    gcp_conn_id -- The ID of the GCP connection
    """

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client = hook.get_client(project_id=project_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False,
        write_disposition=write_disposition,
    )
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
