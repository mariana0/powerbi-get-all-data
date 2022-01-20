import requests
from datetime import datetime, timedelta
import time
import json
import pymysql.cursors
import utils
import pandas as pd

url_token = ''
client_id = ''
client_secret = ''
username = 'microsoft_user'
password = 'pass'

ACCESS_TOKEN = utils.get_access_token(url_token, client_id, username, password)
HEADERS = {"Authorization": "Bearer {0}".format(ACCESS_TOKEN)}
ENDPOINT = 'https://api.powerbi.com/v1.0/myorg/'

# COLLECTING DATA

# Workspaces

print ('Getting workspaces')
workspaces = utils.get_workspaces(ENDPOINT, HEADERS)
df_workspaces = pd.DataFrame.from_dict(workspaces)


# Dataflows

print ('Getting dataflows')
dataflows = utils.get_dataflows(workspaces, ENDPOINT, HEADERS)
df_dataflows = pd.DataFrame.from_dict(dataflows)
#print(df_dataflows.head())

print ('Getting running dataflows')
transactions = utils.get_running_transactions(dataflows, ENDPOINT, HEADERS)
df_df_running = pd.DataFrame.from_dict(transactions)
df_df_running = df_df_running.reindex(sorted(df_df_running.columns), axis=1)
df_df_running = df_df_running.dropna(how='all')
df_df_running.to_csv(r'PowerBI Running Log.csv', mode='a', header=False, index=False)

#Datasets

print ('Getting datasets')
datasets = utils.get_datasets(workspaces, ENDPOINT, HEADERS)
df_datasets = pd.DataFrame.from_dict(datasets)
#print(df_datasets.head())

print ('Getting running datasets')
refreshes = utils.get_running_refreshes(datasets,ENDPOINT,HEADERS)
df_ds_running = pd.DataFrame.from_dict(refreshes)
df_ds_running = df_ds_running.reindex(sorted(df_ds_running.columns), axis=1)
df_ds_running = df_ds_running.dropna(how='all')
df_ds_running.to_csv(r'PowerBI Running Log.csv', mode='a', header=False, index=False)

df_clean = pd.read_csv(r'PowerBI Running Log.csv',skipinitialspace=True)
df_clean=df_clean.sort_values(by=['callTime'], ascending=False)
df_clean.to_csv(r'PowerBI Running Log.csv',index=False)
