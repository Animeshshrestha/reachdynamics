import itertools
from multiprocessing import Process, Queue

import pandas as pd
import requests


class ApiReachDynamicsMixin:

    queue = Queue()

    def __init__(self):
        self.cred_headers = {
                'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
                            }
        self.url = 'http://api.reachdynamics.com/api/v1.0/'
    
    @property
    def get_client_details(self):

        api_url = self.url+'agency/clients'
        response = requests.request(url=api_url, method='GET', headers=self.cred_headers)
        return response.json()

    def get_account_details(self):

        final_data = []
        api_url = self.url+'attribution/senddetails'
        for client in self.get_client_details:
            params = {'accountId':client.get('accountId')}
            response = requests.request(url=api_url, method='GET', headers=self.cred_headers, 
                                        params=params)
            for data in response.json():
                final_data.append({
                    'fname':data.get('firstName',None),
                    'lname':data.get('lastName',None),
                    'email':data.get('audienceMemberAttributes').get('Email'),
                    'total_visits':len(data.get('audienceMemberAttributes').get('PageVisits')),
                    'clientName':client.get('clientName')
                })
        return pd.DataFrame(final_data)
    
    @property
    def get_email_sent(self):

        api_url = self.url+'reports/emailsummary'
        params = {
            "Columns":["Company","AccountId","ActionDate"]
        }
        payload = [
            {
                "ColumnName": "StartDate"
                # "FilterValue":"2021-09-01T00:00:00"
            },
            {
                "ColumnName":"EndDate"
                # "FilterValue":"2021-10-03T00:00:00"
            }
        ]
        status_code = 400
        while status_code != 200:   
            response = requests.request(url=api_url, method='POST', headers=self.cred_headers, 
                                    params=params, json=payload)
            print("1",response.status_code)
            response_data = response.json()
            status_code = response.status_code
        for data in response_data:
            data['Date'] = data.pop('actionDate')
        self.queue.put(response_data)

    @property
    def get_direct_mail_sent(self):

        api_url = self.url+'reports/directmailsummary'
        params = {
            "Columns":["CompanyName","AccountId","QueuedOnDate"]
        }
        payload = [
            {
                "ColumnName": "StartDate"
                # "FilterValue":"2021-09-01T00:00:00"
            },
            {
                "ColumnName":"EndDate"
                # "FilterValue":"2021-10-03T00:00:00"
            }
        ]
        status_code = 400
        while status_code != 200:  
            response = requests.request(url=api_url, method='POST', headers=self.cred_headers,
                                        params=params, json=payload)
            response_data = response.json()
            print("2",response.status_code)
            status_code = response.status_code
        for data in response_data:
            data['Date'] = data.pop('queuedOnDate')
        self.queue.put(response_data)

    @property
    def get_social_summary(self):

        api_url = self.url+'reports/displayandsocialsummary'
        params = {
            "Columns":["CompanyName","AccountId","StatDate"]
        }
        payload = [
            {
                "ColumnName": "StartDate"
                # "FilterValue":"2021-09-01T00:00:00"
            },
            {
                "ColumnName":"EndDate"
                # "FilterValue":"2021-10-03T00:00:00"
            }
        ]
        status_code = 400
        while status_code != 200:   
            response = requests.request(url=api_url, method='POST', headers=self.cred_headers, 
                                        params=params, json=payload)
            response_data = response.json()
            print("3",response.status_code)
            status_code = response.status_code
        for data in response_data:
            data['Date'] = data.pop('statDate')
        self.queue.put(response_data)
    
    @staticmethod
    def runInParallel(*fns):

        proc = []
        rets = []
        for fn in fns:
            p = Process(target=fn)
            proc.append(p)
            p.start()
        for p in proc:
            ret = ApiReachDynamicsMixin.queue.get()
            rets.append(ret)
        for p in proc:
            p.join()
        return rets
    
    @property
    def fetch_multiple_data(self):

        data = self.runInParallel(self.get_email_sent, 
                                  self.get_direct_mail_sent, 
                                  self.get_social_summary)
        return data
    
    @staticmethod
    def fix_my_stuff(x):
        data = list(set(x.tolist()))
        for d in data:
            if isinstance(d, str):
                return d

    @property
    def get_final_summary_data(self):
        response = self.fetch_multiple_data
        final_data = list(itertools.chain(*response))
        df = pd.DataFrame(final_data)
        df.fillna(0, inplace=True)
        aggregation_functions = {'Date':'first','companyName':lambda x: self.fix_my_stuff(x),'accountId': 'first','quantitySent': 'sum','cost': 'sum','delivered': 'sum','impressions': 'sum','opens': 'sum','clicks': 'sum','bounced': 'sum','unsubs': 'sum'}
        df_new = df.groupby(['Date'], as_index=False).aggregate(aggregation_functions)
        df_new['Date'] = pd.to_datetime(df_new['Date'])
        df_new['Total Leads'] = df_new['quantitySent']
        df_new['Date']  = df_new['Date'].dt.strftime('%Y-%m-%d').replace("'", "")
        df_new.rename(columns={
            'quantitySent':'Direct Mail Sent',
            'cost':'Direct Mail Cost',
            'delivered':'Emails delivered',
            'impressions':'Social impressions'
        }, inplace = True)
        return df_new


api = ApiReachDynamicsMixin()






