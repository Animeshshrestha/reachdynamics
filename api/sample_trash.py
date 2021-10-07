import itertools
import json
from multiprocessing import Manager, Process, Queue

import gspread
import pandas as pd
import requests
from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials

creds = ServiceAccountCredentials.from_json_keyfile_name("/home/animesh/authority_workspace/reachdynamics/api/key.json", SCOPES)
client = gspread.authorize(creds)
sheet = client.open("Testing_Sheet")

# def get_client_details():

#     api_url = 'http://api.reachdynamics.com/api/v1.0/agency/clients'
#     cred_headers = {
#         'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
#     }
#     response = requests.request(url=api_url, method='GET', headers=cred_headers)
#     return response.json()

# client_details = get_client_details()

# def get_client_id_by_name(client_name):

#     filtered_value = list(filter(lambda x: x.get('clientName') == client_name, client_details))
#     if filtered_value:
#         return filtered_value[0]['accountId']
#     return filtered_value

# def get_account_details(account_id):
#     final_data = []
#     api_url = 'http://api.reachdynamics.com/api/v1.0/attribution/senddetails'
#     cred_headers = {
#         'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
#     }
#     params = {'accountId':account_id}
#     response = requests.request(url=api_url, method='GET', headers=cred_headers, params=params)
#     for data in response.json():
#         final_data.append({
#             'fname':data.get('firstName',None),
#             'lname':data.get('lastName',None),
#             'email':data.get('audienceMemberAttributes').get('Email'),
#             'total_visits':len(data.get('audienceMemberAttributes').get('PageVisits')),
#         })
#     print(len(final_data))
#     return pd.DataFrame(final_data)

# def create_worksheet():
#     get_existing_worksheet = [sheet.title for sheet in sheet.worksheets()]
#     for detail in client_details:
#         if not detail.get('clientName') in get_existing_worksheet:
#             sheet.add_worksheet(title=detail.get('clientName'), rows="1700", cols="6")

# def write_data_to_googlesheet():

#     data = get_account_details(18345)
#     sheet_details = sheet.worksheet('Fayetteville Ford')
#     #to append use append_rows
#     sheet_details.update([data.columns.values.tolist()] + data.values.tolist())


# print(write_data_to_googlesheet())
# q= Queue()
# def get_email_sent():

#     api_url = 'http://api.reachdynamics.com/api/v1.0/reports/emailsummary'
#     cred_headers = {
#         'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
#     }
#     params = {
#         "Columns":["Company","AccountId"]
#     }
#     payload = [{
#     "ColumnName": "AccountId",
#     "FilterValue":"AccountId"
#     }]
        
#     response = requests.request(url=api_url, method='POST', headers=cred_headers, params=params, json=payload)
#     q.put(response.json())
#     # return response.json()


# def get_direct_mail_sent():

#     api_url = 'http://api.reachdynamics.com/api/v1.0/reports/directmailsummary'
#     cred_headers = {
#         'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
#     }
#     params = {
#         "Columns":["CompanyName","AccountId"]
#     }
#     payload = [{
#     "ColumnName": "AccountId"
#     }]
        
#     response = requests.request(url=api_url, method='POST', headers=cred_headers, params=params, json=payload)
#     q.put(response.json())


# def get_social_summary():

#     api_url = 'http://api.reachdynamics.com/api/v1.0/reports/displayandsocialsummary'
#     cred_headers = {
#         'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
#     }
#     params = {
#         "Columns":["CompanyName","AccountId"]
#     }
#     payload = [{
#     "ColumnName": "AccountId"
#     }]
        
#     response = requests.request(url=api_url, method='POST', headers=cred_headers, params=params, json=payload)
#     q.put(response.json()) 


# def runInParallel(*fns):

#     proc = []
#     rets = []
#     for fn in fns:
#         p = Process(target=fn)
#         proc.append(p)
#         p.start()
#     for p in proc:
#         ret = q.get()
#         rets.append(ret)
#     for p in proc:
#         p.join()
#     return rets

# data = runInParallel(get_email_sent, get_direct_mail_sent, get_social_summary)

# final_data = list(itertools.chain(*data))
# df = pd.DataFrame(final_data)
# df.fillna(0, inplace=True)

# def fix_my_stuff(x):

#     data = list(set(x.tolist()))
#     for d in data:
#         if isinstance(d, str):
#            return d

# aggregation_functions = {'companyName':lambda x: fix_my_stuff(x),'accountId': 'first','quantitySent': 'sum','cost': 'sum','delivered': 'sum','impressions': 'sum','opens': 'sum','clicks': 'sum','bounced': 'sum','unsubs': 'sum'}
# df_new = df.groupby(['accountId'], as_index=False).aggregate(aggregation_functions)
# del df_new['accountId']
