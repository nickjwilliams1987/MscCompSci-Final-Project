"""
HISTORIC

Runs a pipeline extracting historical weather data for a single day from openweathermap.org, transforming it and loading to a BigQuery table.

1. Retrieve the Openweathermap.org API key
2. Get the historic weather data for a single day, broken down by hour
3. Load a raw json file of the response to a GCP bucket
4. Transform the response data into our output dataframe
5. Copy the dataframe to a GCP bucket
6. Set the table name so it's sharded with a _yyyymmdd table suffix
7. Load the dataframe to BigQuery
"""
import requests
from prefect import flow, task
import datetime as dt
import pandas as pd
from datetime import datetime, timedelta, date
import time
import json
from api_functions import init, get_api_key, load_raw_to_bucket, load_df_to_bucket, load_df_to_bigquery
from google.cloud import bigquery
 

@task(name='Get historic weather data from API', task_run_name='Get historic weather data from API')
def get_data(data_bus: dict)->dict:
    """
    Retrieves the past weather data from the API for a single day, broken down by hour
    
    Data bus requirements:
    - api_key->str:         openweathermap.org API key previously retrieved from GCP Secrets Manager
    - date->date:           the date of which to retrieve the api data. 

    Data bus modifications:
    - api_response->str:    the raw json response received
    """
    api_key = data_bus['api_key']
    start_date = time.mktime(data_bus['date'].timetuple())
    data_bus['api_response'] = {}
    
    for city in data_bus['api_cities']:
        lat = city['lat']
        lon = city['lon']
        r = requests.get(f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start_date}&cnt=24&appid={api_key}')
        data_bus['api_response'][city['city']] = r.json()

    return data_bus


@task(name='Process raw data into a dataframe', task_run_name='Process raw data into a dataframe')
def process_to_df(data_bus: dict)->dict:
    """
    Creates a pandas dataframe of just the fields we're interested in
    
    Data bus requirements:
    - api_response->dict:       the raw response recieved from the API

    Data bus modifications:
    - df->dataframe:            the transformed dataframe generated from the API
    """
    rows_list = []

    for city in data_bus['api_response']:
        for hour in data_bus['api_response'][city]['list']:
            row = {
                'city': city,
                'date': dt.datetime.fromtimestamp(hour['dt']),
                'temp': round(float(hour['main']['temp']) - 273.15,1),
                'pressure': hour['main']['pressure'],
                'humidity': hour['main']['humidity'],
                'clouds': hour['clouds']['all'],
                'wind': hour['wind']['speed']
            }
            if 'rain' in hour:
                row['rain'] = hour['rain']['1h']
            else:
                row['rain'] = 0.00
                
            if 'snow' in hour:
                row['snow'] = hour['snow']['1h']
            else:
                row['snow'] = 0.00
                
            rows_list.append(row)

    data_bus['df'] = pd.DataFrame(rows_list)

    return data_bus


@task(name='Update output tablename to historic_yyyymmdd',task_run_name='Update output tablename to historic_yyyymmdd')
def update_tablename(data_bus: dict)->dict:
    """
    Updates the output tablename to historic_yyyymmdd for a sharded table in BigQuery
    
    This is done so that if we need to re-run a date, it can just replace the _yyyymmdd table
    for that date, and we don't need to worry about deleting existing records for that date.

    
    Data bus requirements:
    - date->date:           the date that API data was retrieved for
    - bq_tablename->str:    the output table name, minus the _yyyymmdd table suffix

    Data bus modifications:
    - bq_tablename->str:    the output table name, now including the _yyyymmdd table suffix

    """
    date = data_bus['date'].strftime('%Y%m%d')
    data_bus['bq_tablename'] = data_bus['bq_tablename'] + f'_{date}' 

    return data_bus


@task(name='Update historic data table', task_run_name="Update historic data table")
def update_historic_table(data_bus: dict)->dict:
    """
    Runs a query to update the historic table with the new data. Uses all of the data we
    just loaded and then appends all the data from the historic data that doesn't match
    the date and location we just loaded.

    Data bus requirements:
    - date->date:           the table suffix that we just loaded API data into

    Data bus modificiations:
    None
    """

    date = data_bus['date'].strftime('%Y%m%d')
    sql = f'''SELECT
                *
                FROM
                `nwilliams-msc-comp-sci.weather.historic_{date}`
                UNION ALL
                SELECT
                historic.*
                FROM
                `nwilliams-msc-comp-sci.weather.historic_complete` as historic
                LEFT JOIN
                `nwilliams-msc-comp-sci.weather.historic_{date}` as latest
                ON historic.city = latest.city
                AND historic.date = latest.date
                WHERE latest.date IS NULL'''
    
    client=bigquery.Client()
    table_id = data_bus['gcp_project'] + '.' + data_bus['bq_dataset'] + '.' + data_bus['bq_complete_history']

    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition='WRITE_TRUNCATE'
    )
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.


@flow(name='Historic weather',flow_run_name='Historic weather')
def run_pipeline(date=None):
    """Run the full pipeline"""
    data_bus = init('historic-settings')

    # If we don't get a date provided, run for yesterday's data
    if date == None:
        date = (datetime.now() - timedelta(1)).date()
    else:
        date = dt.datetime.strptime(date,'%Y-%m-%d').date()
    
    data_bus['date'] = date

    pipeline_stages = [
        get_api_key,
        get_data,
        load_raw_to_bucket,
        process_to_df,
        load_df_to_bucket, 
        update_tablename,
        load_df_to_bigquery,
        update_historic_table
    ]

    for stage in pipeline_stages:
        data_bus = stage(data_bus)

    print('Pipeline complete')


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

if __name__ == "__main__":
    start_date = date(2023, 7, 15)
    end_date = date(2023, 8, 1)
    for single_date in daterange(start_date, end_date):
        run_pipeline(single_date.strftime("%Y-%m-%d"))
        print(single_date.strftime("%Y-%m-%d"))
    #run_pipeline('2023-07-15')