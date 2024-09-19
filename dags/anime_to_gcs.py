from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2023, 3, 15),
    schedule=None,
    catchup=False,
    # doc_md=__doc__,
    default_args={"owner": "henson", "retries": 3},
    tags=["anime"],
    max_active_runs=1,
    params=None
)
def anime_to_gcs():
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name="{{ var.value.bucket_name }}",
        resource=None,
        storage_class="REGIONAL",
        location="ASIA-SOUTHEAST1",
        project_id="{{ var.value.project_id }}",
        # labels="anime",
        gcp_conn_id="google_cloud_default",
        # impersonation_chain="None",
    )

    transfer_csv = LocalFilesystemToGCSOperator(
        task_id="transfer_csv",
        src="include/datasets/anime/{{ds_nodash}}.csv",
        dst="raw/anime/{{ds_nodash}}.csv",
        bucket="{{ var.value.bucket_name }}",
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv",
        # gzip="False",
        # impersonation_chain="None",
    )

    create_bucket >> transfer_csv

anime_to_gcs()