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

# Gateway

print ('Getting gateway sources')
sources = utils.get_gateway_sources(ENDPOINT, HEADERS)
df_sources = pd.DataFrame.from_dict(sources)
print(len(df_sources))

print ('Getting gateway sources users')
users = utils.get_gateway_sources_users(sources, ENDPOINT, HEADERS)
df_users = pd.DataFrame.from_dict(users)
print(len(df_users))

# To add new team member change the file team.txt
print ('Sharing all gateway sources with DS Team')
users = utils.team_share_all_gateway_sources(sources,users,ENDPOINT, HEADERS)
