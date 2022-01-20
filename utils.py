import requests
import pymysql.cursors
import json
import pandas as pd
from datetime import datetime, timedelta



################################ AUTHENTICATION ################################

def get_access_token(url_token, client_id, username, password):
    """
        Function to get and return access_token for Power BI API

        Parameters:
        url_token: Microsoft URL to get access_token
        client_id: Client_id provided by Power BI API (APP)
        username: Power BI E-mail
        password: Power BI Password

        Return:
            None
    """

    data = {
                'grant_type' : 'password',
                'scope' : 'openid',
                'resource' : 'https://analysis.windows.net/powerbi/api',
                'client_id' : client_id,
                'username' : username,
                'password' : password
             }

    response = requests.post(url_token, data=data)
    return response.json()['access_token']



################################ INDEPENDENT ################################


def get_imports(ENDPOINT, HEADERS):
    """
        Function to get all imports.
    """
    
    imports = []
    im_port = {
        'import_id' : '',
        'created' : '',
        'updated': '',
        'dataset_id': '',
        'dataset_name': '',
        'report_url' : ''
    }

    result = requests.get(ENDPOINT+'admin/imports', headers=HEADERS)

    if result.status_code == 200:
        result = result.json()
            
        for row in result['value']:
            im_port['import_id'] = row['id']
            im_port['created'] = row['createdDateTime']
            im_port['updated'] = row['updatedDateTime']

            for row2 in row['datasets']:
                im_port['dataset_id'] = row2['id']
                im_port['dataset_name'] = row2['name']

            for row2 in row['reports']:
                im_port['report_url'] = row2['webUrl']
            
            imports.append(im_port)

            im_port = {
                    'import_id' : '',
                    'created' : '',
                    'updated': '',
                    'dataset_id': '',
                    'dataset_name': '',
                    'report_url' : ''
                }
            
    return imports

def get_events(ENDPOINT, HEADERS): # NOT USED
    """
        Function to get activity events 
    """
    
    events = []
    event = {
        'event_id' : '',
        'datetime' : '',
        'operation': '',
        'activity': '',
        'user': ''
    }

    result = requests.get(ENDPOINT+'admin/activityevents', headers=HEADERS)
    print(result)

    if result.status_code == 200:
        result = result.json()
            
        for row in result['activityEventEntities']:
            event['event_id'] = row['Id']
            event['datetime'] = row['CreationTime']
            event['operation'] = row['Operation']
            event['activity'] = row['Activity']
            event['user'] = row['UserId']
            
            events.append(event)

            event = {
                    'event_id' : '',
                    'datetime' : '',
                    'operation': '',
                    'activity': '',
                    'user': ''
                }
            
    return events



################################ BASE ################################


def get_workspaces(ENDPOINT, HEADERS):
    """
        Function to get all workspaces.
    """

    #result = requests.get(ENDPOINT+'groups', headers=HEADERS).json()
    result = requests.get(ENDPOINT+"admin/groups?$top=5000", headers=HEADERS).json()
    
    workspaces = []
    workspace = {
        'id' : '',
        'name' : '',
        'description':'',
        'status' : '',
        'type' : '',
        'isPremium':''
    }

    for row in result['value']:
        workspace['id'] = row['id']
        workspace['name'] = row['name']
        if 'description' in row:
            workspace['description'] = row['description']
        if 'isOnDedicatedCapacity' in row:
            workspace['isPremium'] = row['isOnDedicatedCapacity']
        workspace['status'] = row['state']
        workspace['type'] = row['type']
        
        workspaces.append(workspace)

        workspace = {
            'id' : '',
            'name' : '',
            'description':'',
            'status' : '',
            'type' : '',
            'isPremium':''
        }
    
    return list(filter(lambda w: w['status'] == 'Active' and (w['type'] == 'Workspace' or w['type'] == 'Group'), workspaces))


def get_personal_workspaces(ENDPOINT, HEADERS):
    """
        Function to get personal workspaces.
    """

    #result = requests.get(ENDPOINT+'groups', headers=HEADERS).json()
    result = requests.get(ENDPOINT+"admin/groups?$top=5000", headers=HEADERS).json()
    
    workspaces = []
    workspace = {
        'id' : '',
        'name' : '',
        'status' : '',
        'type' : ''
    }

    for row in result['value']:
        workspace['id'] = row['id']
        workspace['name'] = row['name']
        workspace['status'] = row['state']
        workspace['type'] = row['type']
        
        workspaces.append(workspace)

        workspace = {
            'id' : '',
            'name' : '',
            'status' : '',
            'type' : ''
        }
  
    return list(filter(lambda w: w['type'] == 'PersonalGroup', workspaces))



################################ WORKSPACES ################################


def get_datasets(workspaces, ENDPOINT, HEADERS):
    """
        Function to get all datasets.
    """
    
    datasets = []
    dataset = {
        'dataset_id' : '',
        'dataset_name' : '',
        'workspace_id' : '',
        'workspace_name' : '',
        'configuredBy': '',
        'rls': ''
    }

    for row in workspaces:
        workspace_id = row['id']
        workspace_name = row['name']

        #result = requests.get(ENDPOINT+'groups/{0}/datasets'.format(workspace_id), headers=HEADERS)
        result = requests.get(ENDPOINT+'admin/groups/{0}/datasets'.format(workspace_id), headers=HEADERS)

        if result.status_code == 200:
            result = result.json()
            
            for row2 in result['value']:
                dataset_id = row2['id']
                dataset_name = row2['name']

                dataset['dataset_id'] = dataset_id
                dataset['dataset_name'] = dataset_name
                dataset['workspace_id'] = workspace_id
                dataset['workspace_name'] = workspace_name
                
                if 'configuredBy' in row2:
                    dataset['configuredBy'] = row2['configuredBy']

                if 'isEffectiveIdentityRolesRequired' in row2:
                    dataset['rls'] = row2['isEffectiveIdentityRolesRequired']
                
                datasets.append(dataset)

                dataset = {
                    'dataset_id' : '',
                    'dataset_name' : '',
                    'workspace_id' : '',
                    'workspace_name' : '',
                    'configuredBy': '',
                    'rls' : ''
                }
    return datasets


def get_dataflows(workspaces, ENDPOINT, HEADERS):
    """
        Function to get all dataflows.
    """
    dataflows = []
    dataflow = {
        'dataflow_id' : '',
        'dataflow_name' : '',
        'workspace_id' : '',
        'workspace_name' : '',
        'dataflow_modified':'',
        'dataflow_description':''
    }

    for row in workspaces:
        workspace_id = row['id']
        workspace_name = row['name']

        #result = requests.get(ENDPOINT+'groups/{0}/dataflows'.format(workspace_id), headers=HEADERS)
        result = requests.get(ENDPOINT+'admin/groups/{0}/dataflows'.format(workspace_id), headers=HEADERS)

        if result.status_code == 200:
            result = result.json()
            for row2 in result['value']:

                dataflow_id = row2['objectId']
                dataflow_name = row2['name']

                jsn = requests.get(ENDPOINT+'admin/dataflows/{0}/export'.format(dataflow_id), headers=HEADERS)
        
                if jsn.status_code == 200:

                    jsn = jsn.json()
                
                    if 'modifiedTime' in jsn:
                        dataflow['dataflow_modified'] = jsn['modifiedTime']
                    

                dataflow['dataflow_id'] = dataflow_id
                dataflow['dataflow_name'] = dataflow_name
                dataflow['workspace_id'] = workspace_id
                dataflow['workspace_name'] = workspace_name

                if 'description' in row2:
                    dataflow['dataflow_description'] = row2['description']


                
                dataflows.append(dataflow)

                dataflow = {
                    'dataflow_id' : '',
                    'dataflow_name' : '',
                    'workspace_id' : '',
                    'workspace_name' : '',
                    'dataflow_modified':'',
                    'dataflow_description': ''
                }
                
    return dataflows


def get_reports(workspaces, ENDPOINT, HEADERS):
    """
        Function to get all reports.
    """
    reports = []
    report = {
            'workspace_id' : '',
            'report_id' : '',
            'workspace_name' : '',
            'report_name' : ''
        }

    for row in workspaces:
        workspace_id = row['id']
        workspace_name = row['name']

        result = requests.get(ENDPOINT+'admin/groups/{0}/reports'.format(workspace_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                report['workspace_id'] = workspace_id
                report['workspace_name'] = workspace_name
                report['report_id'] = row2['id']
                report['report_name'] = row2['name']

                reports.append(report)

                report = {
                'workspace_id' : '',
                'report_id' : '',
                'workspace_name' : '',
                'report_name' : ''
            }
                
    return reports


def get_workspace_users(workspaces, ENDPOINT, HEADERS):
    users = []
    user = {
            'workspace_id' : '',
            'workspace_name' : '',
            'user' : '',
            'acessRight' : '',
            'principalType': '',
            'identifier':'',
            'displayName':'',
            'graphId': ''
        }

    for row in workspaces:
        workspace_id = row['id']
        workspace_name = row['name']

        #result = requests.get(ENDPOINT+'groups/{0}/users'.format(workspace_id), headers=HEADERS)
        result = requests.get(ENDPOINT+'admin/groups/{0}/users'.format(workspace_id), headers=HEADERS)
        
        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                user['workspace_id'] = workspace_id
                user['workspace_name'] = workspace_name
                user['acessRight'] = row2['groupUserAccessRight']
                user['displayName'] =row2['displayName']

                if 'identifier' in row2:
                    user['identifier'] = row2['identifier']

                if 'emailAddress' in row2:
                    user['user'] = row2['emailAddress']

                if 'principalType' in row2:
                    user['principalType'] = row2['principalType']

                if 'graphId' in row2:
                    user['graphId'] = row2['graphId']


                users.append(user)

                user = {
                    'workspace_id' : '',
                    'workspace_name' : '',
                    'user' : '',
                    'acessRight' : '',
                    'principalType': '',
                    'identifier':'',
                    'displayName':'',
                    'graphId': ''
                }

    return users
    

def get_df_ds_relation(workspaces, ENDPOINT, HEADERS):
    """
        Function to get all Dataflow-Dataset relations.
    """
    relations = []
    relation = {
            'workspace_id' : '',
            'dataset_id':'',
            'dataflow_id' : '',
        }

    for row in workspaces:
        workspace_id = row['id']
        workspace_name = row['name']

        result = requests.get(ENDPOINT+'admin/groups/{0}/datasets/upstreamDataflows'.format(workspace_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()
    
            for row2 in result['value']:
                relation['workspace_id'] = workspace_id
                relation['dataset_id'] = row2['datasetObjectId']
                relation['dataflow_id'] = row2['dataflowObjectId']

                relations.append(relation)

                relation = {
                'workspace_id' : '',
                'dataset_id':'',
                'dataflow_id' : '',
                }
                
    return relations



################################ DATASETS ################################


def take_over_datasets (datasets,ENDPOINT, HEADERS):
    """
        Function to set authenticated DS team member as owner in all datasets
    """
    
    for row in datasets:
        dataset_name = row['dataset_name']
        result = requests.post(ENDPOINT+'groups/{0}'.format(row['workspace_id'])+'/datasets/{0}'.format(row['dataset_id'])+'/Default.TakeOver', headers=HEADERS)
        print(dataset_name)
        print(result.status_code)
        

def get_schedules(datasets, ENDPOINT, HEADERS):
    """
        Function to get all datasets refresh schedules.
    """
    schedules = []
    schedule = {
            'workspace_id' : '',
            'dataset_id' : '',
            'workspace_name' : '',
            'dataset_name' : '',
            'enabled' : '',
            'days' : '',
            'times' : '',
            'localTimeZoneId': ''
        }

    for row in datasets:
        workspace_id = row['workspace_id']
        dataset_id = row['dataset_id']
        workspace_name = row['workspace_name']
        dataset_name = row['dataset_name']

        result = requests.get(ENDPOINT+'groups/{0}/datasets'.format(workspace_id)+'/{0}/refreshSchedule'.format(dataset_id), headers=HEADERS)

        if result.status_code == 200:
            
            result = result.json()
            enabled = result['enabled']
            localTimeZoneId = result['localTimeZoneId']
            days = result['days']
            times = result['times']

            schedule['workspace_id'] = workspace_id
            schedule['dataset_id'] = dataset_id
            schedule['workspace_name'] = workspace_name
            schedule['dataset_name'] = dataset_name
            schedule['enabled'] = enabled
            schedule['localTimeZoneId'] = localTimeZoneId
            schedule['days'] = days
            schedule['times'] = times

            schedules.append(schedule)

            schedule = {
                'workspace_id' : '',
                'dataset_id' : '',
                'workspace_name' : '',
                'dataset_name' : '',
                'enabled' : '',
                'days' : '',
                'times' : '',
                'localTimeZoneId': ''
            }
            
    return schedules


def get_ds_datasources(datasets, ENDPOINT, HEADERS):
    """
        Function to get all datasets datasources.
    """
    datasources = []
    datasource = {
            'workspace_id' : '',
            'dataset_id' : '',
            'workspace_name' : '',
            'dataset_name' : '',
            'datasourceType' : '',
            'connectionDetails' : '',
            'datasourceId' : '',
            'gatewayId' : ''
        }

    for row in datasets:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataset_id = row['dataset_id']
        dataset_name = row['dataset_name']


        result = requests.get(ENDPOINT+'admin/datasets/{0}/datasources'.format(dataset_id), headers=HEADERS)
        #result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'datasets/{0}/datasources'.format(dataset_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                datasource['workspace_id'] = workspace_id
                datasource['dataset_id'] = dataset_id
                datasource['workspace_name'] = workspace_name
                datasource['dataset_name'] = dataset_name
                datasource['datasourceType'] = row2['datasourceType']

                if 'gatewayId' in row2:
                    datasource['gatewayId'] = row2['gatewayId']
                    
                if 'connectionDetails' in row2:
                    datasource['connectionDetails'] = row2['connectionDetails']

                if 'datasourceId' in row2:
                    datasource['datasourceId'] = row2['datasourceId']

                datasources.append(datasource)

                datasource = {
                'workspace_id' : '',
                'dataset_id' : '',
                'workspace_name' : '',
                'dataset_name' : '',
                'datasourceType' : '',
                'connectionDetails' : '',
                'datasourceId' : '',
                'gatewayId' : ''
            }
    return datasources


def get_latest_refresh(datasets, ENDPOINT, HEADERS):
    """
        Function to get datasets refresh history.
    """
    refreshes = []
    refresh = {
            'workspace_id' : '',
            'dataset_id' : '',
            'workspace_name' : '',
            'dataset_name' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'start' : '',
            'end' : ''
        }

    for row in datasets:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataset_id = row['dataset_id']
        dataset_name = row['dataset_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'datasets/{0}/refreshes?$top=1'.format(dataset_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['dataset_id'] = dataset_id
                refresh['workspace_name'] = workspace_name
                refresh['dataset_name'] = dataset_name
                refresh['refreshId'] = row2['requestId']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                refreshes.append(refresh)

                refresh = {
                    'workspace_id' : '',
                    'dataset_id' : '',
                    'workspace_name' : '',
                    'dataset_name' : '',
                    'refreshId' : '',
                    'refreshType' : '',
                    'refreshStatus' : '',
                    'start' : '',
                    'end' : ''
                }
                
    return refreshes


def get_running_refreshes(datasets, ENDPOINT, HEADERS):
    """
        Function to get datasets refresh history.
    """
    
    refreshes = []
    refresh = {
            'callTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'workspace_id' : '',
            'workspace_name' : '',
            'entityType':'Dataset',
            'entityID' : '',
            'entityName' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'start' : '',
            'end' : ''
        }

    for row in datasets:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataset_id = row['dataset_id']
        dataset_name = row['dataset_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'datasets/{0}/refreshes?$top=1'.format(dataset_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['workspace_name'] = workspace_name
                refresh['entityID'] = dataset_id
                refresh['entityName'] = dataset_name
                refresh['refreshId'] = row2['requestId']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                refreshes.append(refresh)

                refresh = {
                        'callTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'workspace_id' : '',
                        'workspace_name' : '',
                        'entityType':'Dataset',
                        'entityID' : '',
                        'entityName' : '',
                        'refreshId' : '',
                        'refreshType' : '',
                        'refreshStatus' : '',
                        'start' : '',
                        'end' : ''
                    }
                              
    return list(filter(lambda w: w['refreshStatus'] == 'Unknown', refreshes))


def get_refresh_history(datasets, ENDPOINT, HEADERS):
    """
        Function to get datasets refresh history.
    """
    refreshes = []
    refresh = {
            'workspace_id' : '',
            'dataset_id' : '',
            'workspace_name' : '',
            'dataset_name' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'error':'',
            'errorDescription':'',
            'start' : '',
            'end' : ''
        }

    for row in datasets:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataset_id = row['dataset_id']
        dataset_name = row['dataset_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'datasets/{0}/refreshes'.format(dataset_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['dataset_id'] = dataset_id
                refresh['workspace_name'] = workspace_name
                refresh['dataset_name'] = dataset_name
                refresh['refreshId'] = row2['requestId']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                if 'serviceExceptionJson' in row2:
                    serviceExceptionJson = json.loads(row2['serviceExceptionJson'])
                    
                    if 'errorDescription' in serviceExceptionJson:
                        refresh['errorDescription'] = serviceExceptionJson['errorDescription']
                    else:
                        refresh['errorDescription'] = ''
                        
                    refresh['error'] = serviceExceptionJson['errorCode']
                    
                else:
                    refresh['error'] = ''

                refreshes.append(refresh)

                refresh = {
                    'workspace_id' : '',
                    'dataset_id' : '',
                    'workspace_name' : '',
                    'dataset_name' : '',
                    'refreshId' : '',
                    'refreshType' : '',
                    'refreshStatus' : '',
                    'error':'',
                    'errorDescription':'',
                    'start' : '',
                    'end' : ''
                }
                
    return refreshes



################################ REPORTS ################################


def get_report_users (reports, ENDPOINT, HEADERS):
    """
        Function to get all reports user list.
    """
    users = []
    user = {
                'workspace_id' : '',
                'report_id' : '',
                'workspace_name' : '',
                'report_name' : '',
                'user' : '',
                'accessRight' : '',
                'displayName' : '',
                'principalType' : ''
            }

    for row in reports:
        workspace_id = row['workspace_id']
        report_id = row['report_id']
        workspace_name = row['workspace_name']
        report_name = row['report_name']

        result = requests.get(ENDPOINT+'admin/reports/{0}/users'.format(report_id), headers=HEADERS)
        
        if result.status_code == 200:           
            result = result.json()
            
            for row2 in result['value']:
                identifier = row2['identifier']
                reportUserAccessRight = row2['reportUserAccessRight']

                user['workspace_id'] = workspace_id
                user['report_id'] = report_id
                user['workspace_name'] = workspace_name
                user['report_name'] = report_name
                user['accessRight'] = reportUserAccessRight
                user['user'] = row2['identifier']
                user['displayName'] =row2['displayName']                    

                if 'principalType' in row2:
                    user['principalType'] = row2['principalType']

                users.append(user)

                user = {
                        'workspace_id' : '',
                        'report_id' : '',
                        'workspace_name' : '',
                        'report_name' : '',
                        'user' : '',
                        'accessRight' : '',
                        'displayName' : '',
                        'principalType' : ''
                    }
            
    return users


def get_report_pages (reports, ENDPOINT, HEADERS):
    """
        Function to get all reports pages.
    """
    pages = []
    page = {
            'workspace_id' : '',
            'report_id' : '',
            'workspace_name' : '',
            'report_name' : '',
            'page_dname' : '',
            'page_name' : '',
            'page_order': ''
        }

    for row in reports:
        workspace_id = row['workspace_id']
        report_id = row['report_id']
        workspace_name = row['workspace_name']
        report_name = row['report_name']

        result = requests.get(ENDPOINT+'groups/{0}'.format(workspace_id)+'/reports/{0}/pages'.format(report_id), headers=HEADERS)
        
        if result.status_code == 200:           
            result = result.json()
            
            for row2 in result['value']:
                page_dname = row2['displayName']
                page_order = row2['order']
                
                page['workspace_id'] = workspace_id
                page['report_id'] = report_id
                page['workspace_name'] = workspace_name
                page['report_name'] = report_name
                page['page_dname'] = page_dname
                page['page_order'] = page_order

                if 'name' in row2:
                    page['page_name'] = row2['name']
                else:
                    page['page_name'] = ''

                pages.append(page)

                page = {
                        'workspace_id' : '',
                        'report_id' : '',
                        'workspace_name' : '',
                        'report_name' : '',
                        'page_dname' : '',
                        'page_name' : '',
                        'page_order': ''
                    }
            
    return pages



################################ DATAFLOWS ################################


def get_df_datasources(dataflows, ENDPOINT, HEADERS):
    """
        Function to get all dataflows datasources.
    """
    datasources = []
    datasource = {
            'workspace_id' : '',
            'dataflow_id' : '',
            'workspace_name' : '',
            'dataflow_name' : '',
            'datasourceType' : '',
            'connectionDetails' : '',
            'datasourceId' : '',
            'gatewayId' : ''
        }

    for row in dataflows:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataflow_id = row['dataflow_id']
        dataflow_name = row['dataflow_name']

        #result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'dataflows/{0}/datasources'.format(dataflow_id), headers=HEADERS)
        result = requests.get(ENDPOINT+'admin/dataflows/{0}/datasources'.format(dataflow_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                datasource['workspace_id'] = workspace_id
                datasource['dataflow_id'] = dataflow_id
                datasource['workspace_name'] = workspace_name
                datasource['dataflow_name'] = dataflow_name
                datasource['datasourceType'] = row2['datasourceType']

                if 'gatewayId' in row2:
                    datasource['gatewayId'] = row2['gatewayId']
                else:
                    datasource['gatewayId'] = ''
                    
                if 'connectionDetails' in row2:
                    datasource['connectionDetails'] = row2['connectionDetails']
                else:
                    datasource['connectionDetails'] = ''

                if 'datasourceId' in row2:
                    datasource['datasourceId'] = row2['datasourceId']
                else:
                    datasource['datasourceId'] = ''


                datasources.append(datasource)

                datasource = {
                'workspace_id' : '',
                'dataflow_id' : '',
                'workspace_name' : '',
                'dataflow_name' : '',
                'datasourceType' : '',
                'connectionDetails' : '',
                'datasourceId' : '',
                'gatewayId' : ''
            }
    return datasources


def get_latest_transaction(dataflows, ENDPOINT, HEADERS):
    """
        Function to get dataflows refresh history.
    """
    refreshes = []
    refresh = {
            'workspace_id' : '',
            'dataflow_id' : '',
            'workspace_name' : '',
            'dataflow_name' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'start' : '',
            'end' : ''
        }

    for row in dataflows:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataflow_id = row['dataflow_id']
        dataflow_name = row['dataflow_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'dataflows/{0}/transactions'.format(dataflow_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['dataflow_id'] = dataflow_id
                refresh['workspace_name'] = workspace_name
                refresh['dataflow_name'] = dataflow_name
                refresh['refreshId'] = row2['id']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                refreshes.append(refresh)

                refresh = {
                    'workspace_id' : '',
                    'dataflow_id' : '',
                    'workspace_name' : '',
                    'dataflow_name' : '',
                    'refreshId' : '',
                    'refreshType' : '',
                    'refreshStatus' : '',
                    'start' : '',
                    'end' : ''
                }
                
    return refreshes


def get_running_transactions(dataflows, ENDPOINT, HEADERS):
    """
        Function to get dataflows running refreshes
    """
    refreshes = []
    refresh = {
            'callTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'workspace_id' : '',
            'workspace_name' : '',
            'entityType':'Dataflow',
            'entityID' : '',
            'entityName' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'start' : '',
            'end' : ''
        }

    for row in dataflows:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataflow_id = row['dataflow_id']
        dataflow_name = row['dataflow_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'dataflows/{0}/transactions'.format(dataflow_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['workspace_name'] = workspace_name
                refresh['entityID'] = dataflow_id
                refresh['entityName'] = dataflow_name
                refresh['refreshId'] = row2['id']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                refreshes.append(refresh)

                refresh = {
                        'callTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'workspace_id' : '',
                        'workspace_name' : '',
                        'entityType':'Dataflow',
                        'entityID' : '',
                        'entityName' : '',
                        'refreshId' : '',
                        'refreshType' : '',
                        'refreshStatus' : '',
                        'start' : '',
                        'end' : ''
                    }
                
    return list(filter(lambda w: w['refreshStatus'] == 'InProgress', refreshes))


def get_transaction_history(dataflows, ENDPOINT, HEADERS):
    """
        Function to get dataflows refresh history.
    """
    refreshes = []
    refresh = {
            'workspace_id' : '',
            'dataflow_id' : '',
            'workspace_name' : '',
            'dataflow_name' : '',
            'refreshId' : '',
            'refreshType' : '',
            'refreshStatus' : '',
            'start' : '',
            'end' : ''
        }

    for row in dataflows:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataflow_id = row['dataflow_id']
        dataflow_name = row['dataflow_name']

        result = requests.get(ENDPOINT+'groups/{0}/'.format(workspace_id)+'dataflows/{0}/transactions'.format(dataflow_id), headers=HEADERS)

        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                refresh['workspace_id'] = workspace_id
                refresh['dataflow_id'] = dataflow_id
                refresh['workspace_name'] = workspace_name
                refresh['dataflow_name'] = dataflow_name
                refresh['refreshId'] = row2['id']
                refresh['refreshType'] = row2['refreshType']
                refresh['refreshStatus'] = row2['status']
                refresh['start'] = row2['startTime']

                if 'endTime' in row2:
                    refresh['end'] = row2['endTime']
                else:
                    refresh['end'] = ''

                refreshes.append(refresh)

                refresh = {
                    'workspace_id' : '',
                    'dataflow_id' : '',
                    'workspace_name' : '',
                    'dataflow_name' : '',
                    'refreshId' : '',
                    'refreshType' : '',
                    'refreshStatus' : '',
                    'start' : '',
                    'end' : ''
                }
                
    return refreshes


def get_dataflows_metadata(dataflows, ENDPOINT, HEADERS):
    """
        Function to get dataflows tables and fields from json.
    """
    tables = []
    table = {
            'workspace_id' : '',
            'dataflow_id' : '',
            'table_id':'',
            'workspace_name' : '',
            'dataflow_name' : '',
            'table_name' : '',
            'table_refreshPolicy' : '',
            'table_model':'',
            'table_enabled' : '',
            'table_description':'',
            'table_mashup_code':''
            }

    fields = []
    field = {
            'workspace_id' : '',
            'dataflow_id' : '',
            'table_id':'',
            'workspace_name' : '',
            'dataflow_name' : '',
            'table_name':'',
            'field_name':'',
            'field_type' : ''
            }

    for row in dataflows:
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        dataflow_id = row['dataflow_id']
        dataflow_name = row['dataflow_name']

        result = requests.get(ENDPOINT+'admin/dataflows/{0}/export'.format(dataflow_id), headers=HEADERS)
        
        if result.status_code == 200:

            result = result.json()

            try: 
                # Parte gambiarrada, se der problema pode saber que é aqui     
                document = result['pbi:mashup']['document']
                document = document.replace('section Section1;\r\nshared','').replace('\r\n','') #apagando partes desnecessárias
                split_document = document.split('shared') # separando entre tabelas

                mashups = pd.DataFrame(columns=['table','code'])

                for d in split_document:

                    table_code = d.split("=",1) #separando nome da tabela e código
                    table_code[0]=table_code[0][1:-1] #tirando espaço em branco
                    if table_code[0][0] == "#": # algumas entidades vem #"assim" e outras assim
                        table_code[0] = table_code[0][2:-1] # tirando o hashtag e aspas               
                    if len(table_code) == 1 : # pra evitar erro de index
                        table_code.append('') #se não vier o nome ou o código da tabela, vai adicionar um elemento vazio
                    mashup = {'table':table_code[0],'code':table_code[1].replace('\n','')}
                    mashups = mashups.append(mashup,ignore_index=True)
            except:
                print('Failed processing "document" attribute into mashup codes')
                
            for qry in result['pbi:mashup']['queriesMetadata']:

                    table['workspace_id'] = workspace_id
                    table['dataflow_id'] = dataflow_id
                    table['table_id'] = result['pbi:mashup']['queriesMetadata'][qry]['queryId']
                    table['workspace_name'] = workspace_name
                    table['dataflow_name'] = dataflow_name
                    table['table_name'] = qry
                    
                    if table['table_name'][0:2].lower() == 'f_':
                        table['table_model'] = 'Fact'
                    elif table['table_name'][0:2].lower() == 'd_':
                        table['table_model'] = 'Dimension'
                    else:
                        table['table_model'] = 'Unknown'                       
                                        

                    if 'loadEnabled' in result['pbi:mashup']['queriesMetadata'][qry]:
                        table['table_enabled'] = 'True'
                    else:
                        table['table_enabled'] = 'False'
                        
                    for e in result['entities']:
                        if e['name'] == table['table_name'] :
                            if 'pbi:refreshPolicy' in e:
                                table['table_refreshPolicy'] = e['pbi:refreshPolicy']['$type']
                            if 'description' in e:
                                table['table_description'] = e['description']
                            if 'attributes' in e:
                                for at in e['attributes']:
                                    field['workspace_id'] = workspace_id
                                    field['dataflow_id'] = dataflow_id
                                    field['table_id'] = result['pbi:mashup']['queriesMetadata'][qry]['queryId']
                                    field['workspace_name'] = workspace_name
                                    field['dataflow_name'] = dataflow_name
                                    field['table_name'] = qry
                                    field['field_name'] = at['name']
                                    field['field_type'] = at['dataType']
                                    fields.append(field)
                                    field = {
                                        'workspace_id' : '',
                                        'dataflow_id' : '',
                                        'table_id':'',
                                        'workspace_name' : '',
                                        'dataflow_name' : '',
                                        'table_name':'',
                                        'field_name':'',
                                        'field_type' : ''
                                        }
                            
                    for index, row2 in mashups.iterrows():
                        if row2['table'] == table['table_name']:
                            table['table_mashup_code'] = row2['code']                          
                                
                    tables.append(table)

                    table = {
                            'workspace_id' : '',
                            'dataflow_id' : '',
                            'table_id':'',
                            'workspace_name' : '',
                            'dataflow_name' : '',
                            'table_name' : '',
                            'table_refreshPolicy' : '',
                            'table_model':'',
                            'table_enabled' : '',
                            'table_description':'',
                            'table_mashup_code':''
                            }
                
    return (tables,fields)



################################ GATEWAY ################################


def get_gateway_sources (ENDPOINT, HEADERS):
    """
        Function to get gateway sources
    """
    
    gateways = []
    gateway = {
        'gateway_id' : '',
        'gateway_name' : '',
    }

    result = requests.get(ENDPOINT+'admin/gateways', headers=HEADERS)
    
    if result.status_code == 200:
        result = result.json()
            
        for row in result['value']:
            gateway['gateway_id'] = row['id']
            gateway['gateway_name'] = row['name']
            
            gateways.append(gateway)

            gateway = {
                'gateway_id' : '',
                'gateway_name' : '',
            }

    sources = []
        
    source = {
        'gateway_id':'',
        'source_id' : '',
        'source_name': '',
        'source_type': '',
        'source_connection' : '',
        'CredentialType': '',
        'sourceStatus':''
    }

    for row in gateways:
        
        gateway_id = row['gateway_id']
        
        result = requests.get(ENDPOINT+'gateways/{0}'.format(gateway_id)+'/datasources', headers=HEADERS)
        
        if result.status_code == 200:
            
            result = result.json()
                
            for row in result['value']:
                source['gateway_id'] = gateway_id
                source['source_id'] = row['id']
                source['source_name'] = row['datasourceName']
                source['source_type'] = row['datasourceType']
                source['source_connection'] = row['connectionDetails']
                source['CredentialType'] = row['credentialType']

                result = requests.get(ENDPOINT+'gateways/{0}'.format(gateway_id)+'/datasources/{0}'.format(source['source_id'])+'/status', headers=HEADERS)
                
                if result.status_code == 400:
                    result = result.json()
                    source['source_status'] = result['error']['code']
                else:
                    'Ok'
                    
                sources.append(source)

                source = {
                    'gateway_id':'',
                    'source_id' : '',
                    'source_name': '',
                    'source_type': '',
                    'source_connection' : '',
                    'CredentialType': '',
                    'sourceStatus':''
                }
            
    return sources


def get_gateway_sources_users(gateway_sources, ENDPOINT, HEADERS):

    users = []
    user = {
            'gateway_id':'',
            'source_id' : '',
            'source_name': '',
            'user' : '',
            'acessRight' : '',
            'principalType': '',
            'displayName':''
        }

    for row in gateway_sources:
        gateway_id = row['gateway_id']
        source_id = row['source_id']
        source_name = row['source_name']

        result = requests.get(ENDPOINT+'gateways/{0}/datasources/'.format(gateway_id)+'{0}/users'.format(source_id), headers=HEADERS)
        
        if result.status_code == 200:

            result = result.json()

            for row2 in result['value']:
                user['gateway_id'] = gateway_id
                user['source_id'] = source_id
                user['source_name'] = source_name
                user['user'] = row2['identifier']
                user['displayName'] =row2['displayName']

                if 'principalType' in row2:
                    user['principalType'] = row2['principalType']

                users.append(user)

                user = {
                        'gateway_id':'',
                        'source_id' : '',
                        'source_name': '',
                        'user' : '',
                        'acessRight' : '',
                        'principalType': '',
                        'displayName':''
                    }

    return users


def user_share_all_gateway_sources (user_email,gateway_sources,ENDPOINT, HEADERS):
    """
        Function to share all gateway sources to DS team member
    """
    
    for row in gateway_sources:
        
        gateway_id = row['gateway_id']
        source_id = row['source_id']
        source_name = row['source_name']
        
        result = requests.post(ENDPOINT+'gateways/{0}'.format(row['gateway_id'])+'/datasources/{0}'.format(row['source_id'])+'/users',
                                   json={
                                            "emailAddress":user_email,
                                            "datasourceAccessRight":"Read"
                                       }, headers=HEADERS)
        print(source_name)
        print(result.status_code)
        

def team_share_all_gateway_sources (gateway_sources, sources_users, ENDPOINT, HEADERS):
    """
        Function to share all gateway sources to entire DS team
    """
    
    team = [line.strip() for line in open('C:/Users/Administrator/Launch Pad Tecnologia E Servicos/PowerBI-DS - Documents/Team - Access to gateway sources/team.txt', 'r')]
    team = [x for x in team if x!='']
    
    for row in gateway_sources:
        
        for member in team:
            gateway_id = row['gateway_id']
            source_id = row['source_id']
            source_name = row['source_name']
            already_has = False
            
            for row2 in sources_users:
                if row2['source_id'] == row['source_id'] and row2['user'] == member:
                    already_has = True
                else:
                    result = requests.post(ENDPOINT+'gateways/{0}'.format(row['gateway_id'])+'/datasources/{0}'.format(row['source_id'])+'/users',
                                           json={
                                                    "emailAddress":member,
                                                    "datasourceAccessRight":"Read"
                                               }, headers=HEADERS)
                    print("Shared "+source_name+" with "+member+" and got result "+str(result.status_code))


