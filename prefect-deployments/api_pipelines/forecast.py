"""
HISTORIC

Runs a pipeline extracting weather forecasts from openweathermap.org, transforming it and loading to a BigQuery table.

1. Retrieve the Openweathermap.org API key
2. Get the forecast weather data, broken down by hour
3. Load a raw json file of the response to a GCP bucket
4. Transform the response data into our output dataframe
5. Copy the dataframe to a GCP bucket
6. Load the dataframe to BigQuery
"""
import requests
from prefect import flow, task
import datetime as dt
import pandas as pd
from api_functions import init, get_api_key, load_raw_to_bucket, load_df_to_bucket, load_df_to_bigquery


@task(name='Get forecast from API', task_run_name='Get forecast from API')
def get_forecast(data_bus: dict)->dict:
    """
    Retrieves the forecast data from the API

    Data bus requirements:
    - api_key->str:         openweathermap.org API key previously retrieved from GCP Secrets Manager
    - date->date:           the date of which to retrieve the api data. 

    Data bus modifications:
    - api_response->str:    the raw json response received
    """
    api_key = data_bus['api_key']
    api_responses = {}

    for city in data_bus['api_cities']:
        lat = city['lat']
        lon = city['lon']
        r = requests.get(f'https://pro.openweathermap.org/data/2.5/forecast/climate?lat={lat}&lon={lon}&appid={api_key}')
        api_responses[city['city']] = r.json()

    data_bus['api_response'] = api_responses

    return data_bus


@task(name='Process raw data into a dataframe', task_run_name='Process raw data into a dataframe')
def process_to_df(data_bus: dict)->dict:
    """
    Creates a pandas dataframe of the fields we're interested in
    
    Data bus requirements:
    - api_response->dict:       the raw response recieved from the API

    Data bus modifications:
    - df->dataframe:            the transformed dataframe generated from the API
    """
    rows_list = []

    for city in data_bus['api_response']:
        for day in data_bus['api_response'][city]['list']:
            row = {
                'city': city,
                'date': dt.date.fromtimestamp(day['dt']),
                'min_temp': float(day['temp']['min']) - 273.15,
                'max_temp': float(day['temp']['max']) - 273.15,
                'pressure': day['pressure'],
                'humidity': day['humidity'],
                'clouds': day['clouds'],
                'wind': day['speed']
            }
            if 'rain' in day:
                row['rain'] = day['rain']
            else:
                row['rain'] = 0.00
                
            if 'snow' in day:
                row['snow'] = day['snow']
            else:
                row['snow'] = 0.00
                
            rows_list.append(row)

    data_bus['df'] = pd.DataFrame(rows_list)

    return data_bus


@flow(name='Weather forecast',flow_run_name='Weather forecast')
def run_pipeline():
    """Run the full pipeline"""
    data_bus = init('forecast-settings')

    pipeline_stages = [
        get_api_key,
        get_forecast,
        load_raw_to_bucket,
        process_to_df,
        load_df_to_bucket,
        load_df_to_bigquery
    ]

    for stage in pipeline_stages:
        data_bus = stage(data_bus)

    print('Pipeline complete')


if __name__ == "__main__":
    run_pipeline()