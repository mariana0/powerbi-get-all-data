import requests
from datetime import datetime, timedelta
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
GRAPH = 'https://graph.microsoft.com/v1.0/'
TODAY = datetime.now().strftime("%Y-%m-%d")

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# COLLECTING DATA

#Imports

print ('Getting imports')
imports = utils.get_imports(ENDPOINT, HEADERS)
df_imports = pd.DataFrame.from_dict(imports)
df_imports.to_csv(r'PowerBI Imports.csv',index=False)
print (len(df_imports))


# Workspaces

print ('Getting workspaces')
workspaces = utils.get_workspaces(ENDPOINT, HEADERS)
df_workspaces = pd.DataFrame.from_dict(workspaces)
df_workspaces.to_csv(r'PowerBI Workspaces.csv',index=False)
print (len(df_workspaces))

print ('Getting personal workspaces')
pworkspaces = utils.get_personal_workspaces(ENDPOINT, HEADERS)

print ('Getting workspaces user accesses')
users = utils.get_workspace_users(workspaces,ENDPOINT, HEADERS)
df_users = pd.DataFrame.from_dict(users)
df_users.to_csv(r'PowerBI Workspace Users.csv',index=False)
print (len(df_users))


# Dataflows

print ('Getting dataflows')
dataflows = utils.get_dataflows(workspaces, ENDPOINT, HEADERS)
df_dataflows = pd.DataFrame.from_dict(dataflows)
df_dataflows.to_csv(r'PowerBI Dataflows.csv',index=False)
print (len(df_dataflows))

print ('Getting dataflows datasources')
dataflows_sources = utils.get_df_datasources(dataflows, ENDPOINT, HEADERS)
df_df_sources = pd.DataFrame.from_dict(dataflows_sources)
df_df_sources.to_csv(r'PowerBI Dataflows Datasources.csv',index=False)
print (len(df_df_sources))

print ('Getting dataflows refresh history')
transactions = utils.get_transaction_history(dataflows, ENDPOINT, HEADERS)
df_transactions = pd.DataFrame.from_dict(transactions)
df_transactions = df_transactions.reindex(sorted(df_transactions.columns), axis=1)
df_transactions.to_csv(r'PowerBI Dataflows Refreshes.csv', mode='a', header=False, index=False)

df_clean = pd.read_csv(r'PowerBI Dataflows Refreshes.csv',skipinitialspace=True)
df_clean=df_clean.drop_duplicates(subset=['refreshId'], keep='last')
df_clean=df_clean.sort_values(by=['dataflow_id','start'], ascending=False)
df_clean.to_csv(r'PowerBI Dataflows Refreshes.csv',index=False)
print (len(df_clean))

print ('Getting dataflows metadata from json')
dataflows_tables, dataflows_fields = utils.get_dataflows_metadata(dataflows, ENDPOINT, HEADERS)
df_df_tables = pd.DataFrame.from_dict(dataflows_tables)
df_df_fields = pd.DataFrame.from_dict(dataflows_fields)
df_df_tables.to_csv(r'PowerBI Dataflows Tables Json.csv',index=False,sep = '|')
df_df_fields.to_csv(r'PowerBI Dataflows Fields Json.csv',index=False,sep = '|')
print(len(df_df_tables))
print(len(df_df_fields))


#Datasets

print ('Getting datasets')
datasets = utils.get_datasets(workspaces, ENDPOINT, HEADERS)
df_datasets = pd.DataFrame.from_dict(datasets)
df_datasets.to_csv(r'PowerBI Datasets.csv',index=False)
print(len(df_datasets))

print ('Getting datasets datasources')
datasets_sources = utils.get_ds_datasources(datasets, ENDPOINT, HEADERS)
df_ds_sources = pd.DataFrame.from_dict(datasets_sources)
df_ds_sources.to_csv(r'PowerBI Datasets Datasources.csv',index=False)
print(len(df_ds_sources))

print ('Getting datasets refresh schedules')
schedules = utils.get_schedules(datasets, ENDPOINT, HEADERS)
df_schedules = pd.DataFrame.from_dict(schedules)
df_enabled = df_schedules[df_schedules['enabled']==True]
df_enabled.to_csv(r'PowerBI Scheduled Datasets.csv',index=False)
print(len(df_enabled))

print ('Getting datasets refresh history')
refreshes = utils.get_refresh_history(datasets,ENDPOINT,HEADERS)
df_refreshes = pd.DataFrame.from_dict(refreshes)
df_refreshes = df_refreshes.reindex(sorted(df_refreshes.columns), axis=1)
df_refreshes.to_csv(r'PowerBI Datasets Refreshes.csv', mode='a', header=False, index=False)

df_clean = pd.read_csv(r'PowerBI Datasets Refreshes.csv',skipinitialspace=True)
df_clean=df_clean.drop_duplicates(subset=['refresh_id'], keep='last')
df_clean=df_clean.sort_values(by=['dataset_id','start'], ascending=False)
df_clean.to_csv(r'PowerBI Datasets Refreshes.csv',index=False)
print(len(df_clean))

print ('Getting DS-DF Relations')
relations = utils.get_df_ds_relation(workspaces, ENDPOINT, HEADERS)
df_relations = pd.DataFrame.from_dict(relations)
df_relations.to_csv(r'PowerBI Dataflow-Dataset Relations.csv',index=False)
print(len(df_clean))


# Reports

print ('Getting reports')
reports = utils.get_reports(workspaces, ENDPOINT, HEADERS)
df_reports = pd.DataFrame.from_dict(reports)
df_reports.to_csv(r'PowerBI Reports Information.csv',index=False)
print(len(df_reports))

print ('Getting personal reports')
preports = utils.get_reports(pworkspaces, ENDPOINT, HEADERS)
df_preports = pd.DataFrame.from_dict(preports)
df_preports.to_csv(r'PowerBI Personal Reports Information.csv',index=False)
print(len(df_preports))

print ('Getting reports user accesses')
rusers = utils.get_report_users(reports, ENDPOINT, HEADERS)
df_rusers = pd.DataFrame.from_dict(rusers)
df_rusers.to_csv(r'PowerBI Report Users.csv',index=False)
print(len(df_rusers))

print ('Getting reports pages')
pages = utils.get_report_pages(reports, ENDPOINT, HEADERS)
df_pages = pd.DataFrame.from_dict(pages)
df_pages.to_csv(r'PowerBI Report Pages.csv',index=False)
print(len(df_pages))


# Gateway

print ('Getting gateway sources')
gsources = utils.get_gateway_sources(ENDPOINT, HEADERS)
df_gsources = pd.DataFrame.from_dict(gsources)
df_gsources.to_csv(r'PowerBI Gateway Sources.csv',index=False)
print(len(df_gsources))
