"""
FOOTFALL PIPELINE

1. Loads raw footfall data from the DataMillNorth site
2. Copies the CSV files to a GCP bucket
3. Cleans the dataframes (see clean_all_dfs())
4. Copies the processed dataframe CSVs to a GCP bucket
5. Uploads the processed dataframes to BigQuery

Pipeline stages use a dictionary, data_bus, to communicate between functions. All relevant data is saved
to the data_bus and each stage returns the modified data bus for the next one to use.
"""
import requests
import os
import pandas as pd
import pandas_gbq #pip install pandas-gbq
from prefect import flow, task
from google.cloud import storage
from google.cloud import logging
import json
import re


@task(name='Download data',task_run_name='Download data', retries=3)
def download_data(data_bus):
    """
    Retrieves all footfall files from the given api url and downloads them to local storage. 
    
    Data bus requirements:
    - local_storage->String:    local folder to store downloaded files in
    - api_url->String:          Url for gathering a json list of files available
    - download_url->String:     Url for downloading individual files, including {{key}} and {{file_name}}
                                placeholders for replacements.
    """
    r = requests.get(data_bus['api_url'])
    api_response = r.json()

    file_list = []
    dfs = {}
    for key, file in api_response['resources'].items():
        if file['format'] == 'csv':
            file_name = file['url'].split('/')[-1]
            url = (data_bus['download_url']
                    .replace('{{key}}',key)
                    .replace('{{file_name}}',file_name))

            # Ignore any files listed in the exclude_files setting
            if file_name not in data_bus['exclude_files']:
                # Filenames have web address encoding, so replace %20 with underscore
                file_name = file_name.replace('%20','_')
                df = pd.read_csv(url)
                file_list.append(file_name)
                dfs[file_name] = df
        
    data_bus['file_list'] = file_list
    data_bus['dfs'] = dfs
    data_bus['dfs_raw'] = True
    return data_bus


@task(name='Load dataframes to bucket',task_run_name='Load dataframes to bucket', retries=3)
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
    - dfs->Dictionary:      a dictionary containing dataframes with the key as the filename and
                            the dataframe as the value.
    """
    # If we're operating pre-processing, load to the raw folder otherwise load to processed.
    if data_bus['dfs_raw'] == True:
        sink_folder = data_bus['gcp_bucket_folder_raw']
    else:
        sink_folder = data_bus['gcp_bucket_folder_processed']
    
    # Load each dataframe as a CSV to the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bus['gcp_bucket'])
    for key, value in data_bus['dfs'].items():
        filename = f'{sink_folder}/{key}'
        blob = bucket.blob(filename)
        blob.upload_from_string(value.to_csv(), 'text/csv')

    return data_bus


def create_preferred_column(df, prefered_columns, new_column):
    """
    DATA CLEANING
    
    Runs through a list of potential columns in preferential order and creating a new column based on 
    the first preferred column found.

    E.g. for FootfallLocation, the function will first look for an existing column called LocationName,
    and if it finds it, creates a new column called FootfallLocation which copies LocationName. If it 
    doesn't find LocationName, it searches for one called Location and will use that instead.

    Inputs:
    - df->DataFrame:        The dataframe to perform the creation on
    - preferred_columns->List: 
                            Source column names (strings) in preferential order
    - new_column->String:   The new column to be created.
    """
    for source in prefered_columns:
        if source in list(df.columns):
            df[new_column] = df[source]
            return df
    return False


def clean_hours(x):
    """
    DATA CLEANING
    Takes in a variety of hour formats and returns it in a consistent string of HH:MM:SS.

    Values searched for:
    - H
    - HH
    - HH:MM

    Inputs:
    - x->Unknown:       The hour value to be converted. Is immediately converted to string for
                        processing.
    """
    x = str(x)
    return re.search(r"^([0-9]{1,2})(?:\.|\:|$)", x).group(1).rjust(2,'0') + ':00:00'


def clean_df(data_bus, df_name):
    """
    DATA CLEANING
    Cleans a raw pandas dataframe which was loaded from the API download. See individual 
    comments describing the transformation steps.

    Data bus requirements:
    - dfs->Dictionary:      A dictionary containing values of dataframes and keys of the filenames.

    Inputs:
    - df_name->String:      The dataframe name from the dictionary to be processed.
    """
    df = data_bus['dfs'][df_name].copy()

    # Remove any unnamed columns
    df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
    
    # Get the location column
    df = create_preferred_column(df, data_bus['prefered_footfall_columns'], 'total_footfall')
    
    # Get the footfall column
    df = create_preferred_column(df, data_bus['prefered_location_columns'], 'footfall_location')
    
    # Drop any rows where hour is NA (indicating it's a daily total, not hourly)
    missing_hours = df['Hour'].isna().sum()
    if missing_hours > 0:
        df = df.dropna(subset=['Hour'])
    
    # Resolve the hour value
    df['Hour'] = df['Hour'].apply(clean_hours)
    
    # Convert the datetime formats
    df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)
    df['timestamp'] = df['Date'].dt.strftime('%d/%m/%Y') + ' ' + df['Hour']
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H:%M:%S')

    # Name the city
    df['city'] = 'Leeds'

    # Drop empty rows and keep only the columns we're interested in
    df = df.dropna()
    data_bus['dfs'][df_name] = df[['city','footfall_location','timestamp','total_footfall']]
    
    return data_bus


@task(name='Clean raw dataframes',task_run_name='Clean raw dataframes', retries=3)
def clean_all_dfs(data_bus):
    """
    Cleans all dataframes in the databus.
    
    Data bus requirements:
    - dfs->Dictionary:      A dictionary containing values of dataframes and keys of the filenames.
    - dfs_raw->Boolean:     Whether the dataframes stored are raw or have been processed (by this function)
    """
    for key, value in data_bus['dfs'].items():
        data_bus = clean_df(data_bus, key)

    # Dataframes have now been processed
    data_bus['dfs_raw'] = False
    return data_bus


@task(name='Load processed dataframes to BigQuery',task_run_name='Load processed dataframes to BigQuery', retries=3)
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

    # Join all the dataframes together
    df = pd.concat(data_bus['dfs'].values(), ignore_index=True)
    df = df.drop_duplicates()

    # Upload to BigQuery
    pandas_gbq.to_gbq(
        df,
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
    with open('footfall-leeds-settings.json') as f:
        data_bus = json.load(f)

    # List of the stages to be completed.
    pipeline_stages = [
        download_data,
        #load_to_bucket,
        clean_all_dfs,
        load_to_bucket,
        load_df_to_bigquery
    ]

    # Run through each stage
    for stage in pipeline_stages:
        data_bus = stage(data_bus)


if __name__ == "__main__":
    run_pipeline()