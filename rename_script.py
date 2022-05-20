import os
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/credentials/keys.json'

bucket_name = "cleanbucket0694"

def list_blobs(bucket_name):
    
    
    storage_client = storage.Client()

    
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        if blob.name.split(".")[-1]=='parquet':

             return str(blob.name)

bucket_name = "cleanbucket0694"
destination_bucket_name="cleanbucket0694"
blob_name = list_blobs(bucket_name)
destination_blob_name = "file.parquet"

def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
   
    

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)