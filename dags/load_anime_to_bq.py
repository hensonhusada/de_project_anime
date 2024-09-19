from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator

DATASET_NAME = "project_anime"

@dag(
    start_date=datetime(2024, 8, 25),
    schedule=None,
    catchup=False,
    # doc_md=__doc__,
    default_args={"owner": "henson", "retries": 3},
    tags=["anime"],
)
def load_anime_to_bq():
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        project_id="{{ var.value.project_id }}",
        dataset_id=DATASET_NAME,
        location="ASIA-SOUTHEAST1",
        gcp_conn_id="google_cloud_default",
        if_exists="ignore"
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        destination_project_dataset_table=f"{DATASET_NAME}.anime",
        gcp_conn_id="google_cloud_default",
        bucket="{{ var.value.bucket_name }}",
        skip_leading_rows=1,
        autodetect=True,
        source_objects=["raw/anime/*"],
        schema_fields=[
            {"name": "MAL_ID", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "Name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Score", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Genres", "type": "STRING", "mode": "NULLABLE"},
            {"name": "English_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Japanese_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Episodes", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Aired", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Premiered", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Producers", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Licensors", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Studios", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Duration", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Rating", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Ranked", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Popularity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Members", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Favorites", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Watching", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Completed", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "On_Hold", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Dropped", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Plan_to_Watch", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Score_10", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_9", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_8", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_7", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_6", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_5", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_4", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_3", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_2", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Score_1", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    create_dataset >> create_external_table

load_anime_to_bq()