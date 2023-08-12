"""
FOOTFALL PIPELINE

1. Loads raw footfall data from the York open data site
2. Copies the CSV file to a GCP bucket
5. Uploads the dataframe to BigQuery

Pipeline stages use a dictionary, data_bus, to communicate between functions. All relevant data is saved
to the data_bus and each stage returns the modified data bus for the next one to use.
"""
import pandas as pd
import pandas_gbq #pip install pandas-gbq
from prefect import flow, task #pip install prefect
from google.cloud import storage #pip install google-cloud-storage
import json


@task(name='Download data',task_run_name='Download data', retries=3)
def download_data(data_bus):
    """
    Retrieves a raw CSV files from the given api url and loads it to a dataframe. 
    
    Data bus requirements:
    - api_url->String:          Url for the raw CSV file
    """

    data_bus['df'] = pd.read_csv(data_bus['api_url'])
    data_bus['dfs_raw'] = True
    return data_bus


@task(name='Load dataframes to bucket',task_run_name='Load dataframes to bucket')
def load_to_bucket(data_bus):
    """
    Loads a pandas dataframe as a CSV to a GCP bucket

    Input:
    - raw->Boolean: Whether to load the raw or processed dataframe

    Data bus requirements:
    - gcp_bucket->String:   The name of the bucket in which to load the CSV
    - gcp_bucket_folder_raw->String: 
                            Folder name within the bucket for raw files
    - gcp_bucket_folder_processed->String: 
                            Folder name within the bucket for processed files
    """
    # If we're operating pre-processing, load to the raw folder otherwise load to processed.
    if data_bus['dfs_raw'] == True:
        sink_folder = data_bus['gcp_bucket_folder_raw']
    else:
        sink_folder = data_bus['gcp_bucket_folder_processed']

    # Load the dataframe as a CSV to the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bus['gcp_bucket'])
    blob = bucket.blob(sink_folder + '/' + data_bus['csv_filename'])
    blob.upload_from_string(data_bus['df'].to_csv(), 'text/csv')

    return data_bus



@task(name='Clean raw dataframes',task_run_name='Clean raw dataframes')
def clean_df(data_bus):
    """
    DATA CLEANING
    Cleans a raw pandas dataframe which was loaded from the API download. See individual 
    comments describing the transformation steps.

    Data bus requirements:
    - dfs->Dictionary:      A dictionary containing values of dataframes and keys of the filenames.

    """
    df = data_bus['df'].copy()

    # 
    df['city'] = 'York'

    # Set the date field
    df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)

    # Rename fields to desired output
    df = df.rename(
        columns={'Date': 'timestamp',
         'LocationName': 'footfall_location',
         'InCount': 'total_footfall'}
    )

    # The InCount column contains nulls instead of 0
    df = df.fillna(0)
    
    data_bus['df'] = df[['city','footfall_location','timestamp','total_footfall']]
    data_bus['dfs_raw'] = False
    return data_bus


@task(name='Load processed dataframes to BigQuery',task_run_name='Load processed dataframes to BigQuery')
def load_df_to_bigquery(data_bus):
    """
    Loads the processed dataframes to BigQuery
    
    Data bus requirements:
    - bq_dataset->String:   Dataset to load the sink table
    - bq_table->String:     Sink table name
    - dfs->Dictionary:      A dictionary containing values of dataframes and keys of the filenames.
    - gcp_project->String:  GCP Project name
    - schema->List:         BigQuery table schema
    
    """
    table_id = data_bus['bq_dataset'] + '.' + data_bus['bq_table']

    # Upload to BigQuery
    pandas_gbq.to_gbq(
        data_bus['df'],
        table_id,
        project_id=data_bus['gcp_project'],
        if_exists='replace',
        table_schema=data_bus['schema']
    )
    return data_bus


@flow(name='Footfall Pipeline', flow_run_name='Footfall Pipeline')
def run_pipeline():
    """Runs the full pipeline."""
    # Load initial settings into data_bus
    with open('footfall_york_settings.json') as f:
        data_bus = json.load(f)

    # List of the stages to be completed.
    pipeline_stages = [
        download_data,
        load_to_bucket,
        clean_df,
        load_to_bucket,
        load_df_to_bigquery
    ]

    # Run through each stage
    for stage in pipeline_stages:
        data_bus = stage(data_bus)


if __name__ == "__main__":
    run_pipeline()
