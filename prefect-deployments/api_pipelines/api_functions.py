"""
API FUNCTIONS

Collection of common API functions used in data pipelines
"""

from google.cloud import secretmanager #pip install google-cloud-secret-manager
import json
from prefect import task
from google.cloud import storage
import datetime as dt
import pandas as pd
import pandas_gbq

@task(name='Pipeline Initialisation', task_run_name='Pipeline Initialisation')
def init(filename: str)->dict:
    """
    Loads the pipeline settings into the data bus

    Inputs:
    - filename->Str:    name of the json file containing the initial databus

    Outputs:
    - data_bus->dict:   A dictionary version of the json file contents
    """
    with open(f'{filename}.json') as f:
        data_bus = json.loads(f.read())

    return data_bus


@task(name='Get API Key', task_run_name='Get API Key')
def get_api_key(data_bus: dict)->dict:
    """
    Loads an API key from the GCP Secrets Manager. Note that this assumes the latest verison is required.

    Data bus requirements:
    - gcp_project->str:     name of the GCP project
    - api-key-secret->str:  name of the secret to be gathered.

    Data bus modifications:
    - api_key->str:         the secret value retrieved
    """
    gcp_project = data_bus['gcp_project']
    api_key_secret = data_bus['api-key-secret']

    secrets_client = secretmanager.SecretManagerServiceClient()
    secrets_payload = secrets_client.access_secret_version(
        name=f'projects/{gcp_project}/secrets/{api_key_secret}/versions/latest'
        ).payload
    data_bus['api_key'] = secrets_payload.data.decode('UTF-8')

    return data_bus


@task(name='Load the raw API response to GCP bucket', task_run_name='Load the raw API response to GCP bucket')
def load_raw_to_bucket(data_bus: dict)->dict:
    """
    Loads the raw API responses to a bucket as a json file

    Data bus requirements:
    - storage_raw_folder->str:  name of the folder to dump the raw API responses to
    - storage_bucket->str:      name of the bucket containing the storage folder
    - api_response->Any:        the value of the API response. Converted to str before processing.

    Data bus modifications:
    n/a

    """
    timestamp = dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    storage_raw_folder = data_bus['storage_raw_folder']
    filename = f'{storage_raw_folder}/{timestamp}.json'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bus['storage_bucket'])
    blob = bucket.blob(filename)

    blob.upload_from_string(str(data_bus['api_response']))

    return data_bus


@task(name='Load the processed dataframe as a CSV to bucket', task_run_name='Load the processed dataframe as a CSV to bucket')
def load_df_to_bucket(data_bus: dict)->dict:
    """
    Loads a processed dataframe to a storage bucket as a CSV file

    Data bus requirements:
    - storage_processed_folder->str:    
                                name of the folder to dump the processed dataframe CSVs to
    - storage_bucket->str:      name of the bucket containing the storage folder
    - df->dataframe:            the processed dataframe to upload

    Data bus modifications:
    n/a
    """
    timestamp = dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    storage_processed_folder = data_bus['storage_processed_folder']
    filename = f'{storage_processed_folder}/{timestamp}.csv'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bus['storage_bucket'])
    blob = bucket.blob(filename)

    blob.upload_from_string(data_bus['df'].to_csv(), 'text/csv')
    return data_bus


@task(name='Load the processed dataframe to BigQuery', task_run_name='Load the processed dataframe to BigQuery')
def load_df_to_bigquery(data_bus: dict)->dict:
    """
    Loads a processed dataframe to BigQuery
    
    Data bus requirements:
    - bq_dataset->str:      name of the dataset containing the output table
    - bq_tablename->str:    name of the table to be written to
    - df->dataframe:        the processed dataframe
    - gcp_project->str:     the gcp project we're working with
    - schema->dict:         a BigQuery table schema to use
    
    Data bus modifications:
    n/a
    """
    table_id = data_bus['bq_dataset'] + '.' + data_bus['bq_tablename']

    pandas_gbq.to_gbq(
        data_bus['df'],
        table_id,
        project_id=data_bus['gcp_project'],
        if_exists='replace',
        table_schema=data_bus['schema']
    )
    return data_bus