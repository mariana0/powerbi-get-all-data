from datetime import datetime, timedelta
import utils
import pandas as pd

url_token = ''
client_id = ''
client_secret = ''

# USER THAT WILL AUTHENTICATE AND TAKE OVER EVERYTHING
username = 'microsoft_user'
password = 'pass'

ACCESS_TOKEN = utils.get_access_token(url_token, client_id, username, password)
HEADERS = {"Authorization": "Bearer {0}".format(ACCESS_TOKEN)}
ENDPOINT = 'https://api.powerbi.com/v1.0/myorg/'
GRAPH = 'https://graph.microsoft.com/v1.0/'
TODAY = datetime.now().strftime("%Y-%m-%d")

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# COLLECTING DATA

# Workspaces

print ('Getting workspaces')
workspaces = utils.get_workspaces(ENDPOINT, HEADERS)
df_workspaces = pd.DataFrame.from_dict(workspaces)
print (len(df_workspaces))


#Datasets

print ('Getting datasets')
datasets = utils.get_datasets(workspaces, ENDPOINT, HEADERS)
df_datasets = pd.DataFrame.from_dict(datasets)
print(len(df_datasets))

# Gateway

print ('Getting gateway sources')
gsources = utils.get_gateway_sources(ENDPOINT, HEADERS)
df_gsources = pd.DataFrame.from_dict(gsources)
df_gsources.to_csv(r'PowerBI Gateway Sources.csv',index=False)
print(len(df_gsources))

print ('Sharing all gateway sources')
utils.user_share_all_gateway_sources(username,gsources,ENDPOINT, HEADERS)

# CAUTION!!!!!
print ('Taking over datasets (very critical action and might cause several refresh failures when datasets configuration is lost)')
utils.take_over_datasets(datasets, ENDPOINT, HEADERS)
