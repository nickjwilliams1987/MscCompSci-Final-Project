
# YORK FOOTFALL PIPELINE

This pipeline is for execution in Prefect and loads York pedestrian footfall data into BigQuery.

## Files
- footfall_york.py -> The main data pipeline script
- footfall_york_settings.json -> Contains global variables for use by the pipeline
- deployment.py -> Loads the pipeline as a deployment in the Prefect Cloud UI.


## Data Bus
Pipeline stages use a dictionary, data_bus, to communicate between functions. All relevant data is saved
to the data_bus and each stage returns the modified data bus for the next one to use.

The initial state of the data bus is loaded from the footfall_york_settings.json file.

## Pipeline Overview
1. run_pipeline(): Loads the settings file into the data bus and executes the remaining stages in turn
2. download_data(): Loads raw footfall data from the York open data site into the data bus
3. load_to_bucket(): Exports the raw, uncleaned data to a csv in a GCP bucket
4. clean_df(): Cleans the raw data through some minor transformations
5. load_to_bucket(): Exports the now cleaned data into a new csv in a GCP bucket
6. load_df_to_bigquery(): Loads the clean data into BigQuery

## Prefect Cloud UI example
![image](https://github.com/nickjwilliams1987/MscCompSci-Final-Project/assets/73435959/10a375d5-99a7-40de-8506-4e136b46f88f)
