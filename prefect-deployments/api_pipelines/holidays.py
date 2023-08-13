"""
HOLIDAYS

Runs a pipeline extracting public holidays dates for England from nager.net

1. Retrieve public holidays data from the API for each year
2. Load the raw API response as json to a GCP bucket
3. Transform the data into a dataframe
4. Load the dataframe as a CSV to a GCP bucket
5. Load the dataframe to BigQuery
"""

import requests
import datetime as dt
import pandas as pd
from prefect import flow, task
import datetime as dt

from api_functions import init, load_raw_to_bucket, load_df_to_bucket, load_df_to_bigquery


@task(name='Get holidays from API', task_run_name='Get holidays from API')
def retrieve_date(data_bus: dict)->dict:
    """
    Retrieves public holiday data from the API for each year since the start year
    specified in the pipeline settings to the current year +1

    Data bus requirements:
    - start_year->str:      the year to start retrieving holidays from

    Data bus modifications:
    - api_responses->list:  each annual API response 
    """
    responses = []
    start_year = int(data_bus['start_year'])
    # Note: end year set to next year in case the forecast dips over new years eve
    end_year = int(dt.date.today().strftime("%Y"))+1

    for year in range(start_year,end_year+1):
        r = requests.get(f'https://date.nager.at/api/v3/PublicHolidays/{year}/GB')
        api_response = r.json()
        responses.append(api_response)
        
    data_bus['api_response'] = responses
    return data_bus


@task(name='Process raw data into a dataframe', task_run_name='Process raw data into a dataframe')
def process_to_df(data_bus: dict)->dict:
    """
    Transforms the raw api data to a Dandas dataframe
    
    Response has bank holidays for all UK countries including NI, Scotland which
    have bank holidays that are not celebrated in England, where Leeds is. The
    holidays therefore need to be filtered.

    Note that St Patricks day has still been included since it is informally
    celebrated in the UK.

    Data bus requirements:
    - api_responses->list:  each annual API response 

    Data bus modifications:
    - df->dataframe:        the processed dataframe
    """
    df_rows = []

    for response in data_bus['api_response']:
        for day in response:
            if (day['counties'] == None or 
                'GB-ENG' in day['counties'] or
                day['name'] == 'Saint Patrick\'s Day'):
                
                date_dict = {
                    'date': day['date'],
                    'name': day['localName']
                }
                df_rows.append(date_dict)

    data_bus['df'] = pd.DataFrame(df_rows)

    return data_bus


@flow(name='Public Holidays',flow_run_name='Public Holidays')
def run_pipeline():
    """Runs the full pipeline"""
    data_bus = init('holidays-settings')

    pipeline_stages = [
        retrieve_date,
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




