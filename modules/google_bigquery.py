from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def delete_from_table(
    table_id: str,
    project_id: str,
    where_clause: str,
    gcp_conn_id: str = "google_cloud_default",
) -> None:
    """Delete rows from a BigQuery table.

    Keyword arguments:
    table_id -- The ID of the BigQuery table
    project_id -- The ID of the BigQuery project
    where_clause -- Full WHERE clause, e.g. WHERE date = '2026-10-21'
    gcp_conn_id -- The ID of the GCP connection
    """

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client = hook.get_client(project_id=project_id)

    try:
        client.get_table(table_id)
    except NotFound:
        return

    client.query(f"DELETE FROM `{table_id}` {where_clause}").result()


def get_client(project_id: str, gcp_conn_id: str = "google_cloud_default") -> bigquery.Client:
    """Get a BigQuery client.

    Keyword arguments:
    project_id -- The ID of the BigQuery project
    gcp_conn_id -- The ID of the GCP connection
    """

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client = hook.get_client(project_id=project_id)
    
    return client


def upload_dataframe(
    dataframe,
    table_id: str,
    project_id: str,
    schema: list,
    write_disposition: str,
    create_disposition: str = "CREATE_IF_NEEDED",
    gcp_conn_id: str = "google_cloud_default",
) -> None:
    """Upload a dataframe to a BigQuery table.

    Keyword arguments:
    dataframe -- The dataframe to upload
    table_id -- The ID of the BigQuery table
    project_id -- The ID of the BigQuery project
    create_disposition -- The create disposition of the BigQuery table
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
        create_disposition=create_disposition,
    )
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
