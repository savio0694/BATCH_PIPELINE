import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash_operator import BashOperator


PROJECT_ID ='finaldemo-349008'
GCS_BUCKET = 'landingbucket0694'
DEST_BUCKET='cleanbucket0694'
FILENAME = "test_file.json"
SQL_QUERY = "select * from customers limit 10 ;"
PYTHON_FILE="gs://sparkbucketbucket0694/spark_script.py"
REGION="europe-west2"
CLUSTER_NAME = "cluster-c2a9"
DATASET_NAME = 'final_demo_data'
DATA_EXPORT_BUCKET_NAME = 'cleanbucket0694'
TABLE = "customer"


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYTHON_FILE},
}

with DAG(
    dag_id="postgres_to_bigquery_dag",
    
    start_date=datetime.datetime(2022, 5, 14),
    schedule_interval="@once",
    catchup=False,
) as dag:
    extract_data = PostgresToGCSOperator(
        task_id="extract_data", sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, gzip=False,postgres_conn_id="postgres_remote"
    )

    upload_data_to_gcs = PostgresToGCSOperator(
        task_id="upload_data_to_gcs",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
        postgres_conn_id="postgres_remote",
    )

    submit_job_to_spark = DataprocSubmitJobOperator(
        task_id="submit_job_to_spark", 
        job=PYSPARK_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    copy_single_file = GCSToGCSOperator(
            task_id="copy_single_file",
            source_bucket=GCS_BUCKET,
            source_objects=['test_file.json'],
            destination_bucket=GCS_BUCKET,
            destination_object='BACKUP/dest.json',
            
        )

    rename_parquet = BashOperator(
    task_id='rename_parquet',
    bash_command='python /opt/airflow/dags/rename_script.py',
 )
    


    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=DATA_EXPORT_BUCKET_NAME,
        source_objects=["file.parquet"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE}",
        source_format='parquet',
        write_disposition='WRITE_TRUNCATE',
    )

    extract_data>>upload_data_to_gcs>>submit_job_to_spark>>copy_single_file>>rename_parquet>>gcs_to_bigquery
