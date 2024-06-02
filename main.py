import pandas as pd
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery

from flask import make_response
import json
import requests
import functions_framework


#configuration for local debugging
# spreadsheetId = os.environ['spreadsheetId']
# range_name = os.environ['range_name']
# project_id = os.environ['project_id']
# dataset_id = os.environ['dataset_id']
# table_id = os.environ['table_id']

### for local debugging
#Authentication with service account for bigquery
# credentials = service_account.Credentials.from_service_account_file(
#      'bq_service_account.json', scopes=[
#          "https://www.googleapis.com/auth/drive",
#          "https://www.googleapis.com/auth/cloud-platform"],
#  )
###
@functions_framework.http
def fire(request):
    ### for cloud functions deployment
    #Authentication with service account for bigquery
    scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/cloud-platform"]
    credentials, _ = google.auth.default(scopes = scopes)
    ###

    service = build('sheets', 'v4', credentials=credentials)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Extract data from Google Sheets
    values = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name).execute()['values']

    #Prepare data for loading into df
    for value in values:
        while len(value) < len(values[0]):
            value.append(0)

    # Find and replace empty strings with 0
    for sublist in values:
        for i in range(len(sublist)):
            if sublist[i] == '':
                sublist[i] = 0

    #Load data into a pandas DataFrame
    df = pd.DataFrame(values[1:], columns = values[0])

    #Change all values into int 
    for index in df:
        df[index] = df[index].astype(int) 

    #Transform data
    # List to store the new rows
    daily_data = []

    #Get current year, month and days per month 
    for index, row in df.iterrows():
        year = row['year']
        month = row['month']
        num_days = pd.Timestamp(year, month, 1).days_in_month

        #make values daily and add the date column
        for day in range(1, num_days + 1):
            new_row = row.copy()
            date = pd.Timestamp(year, month, day)
            new_row['date'] = date

            # Reorder columns to ensure date comes first
            daily_row = {'date': date}
            for column in df.columns:
                if column not in ['year', 'month']:
                    daily_row[column] = row[column] / num_days

            daily_data.append(daily_row)

    # Create a new DataFrame with daily values
    daily_df = pd.DataFrame(daily_data)
    #Transform date column into sql data type
    daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')


    #Load data to BigQuery
    #Set table id to the the initialised one
    table_id = f'{project_id}.{dataset_id}.{table_id}'

    # Load DataFrame into BigQuery
    job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
    job = client.load_table_from_dataframe(daily_df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    #print(f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')

    return (f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')