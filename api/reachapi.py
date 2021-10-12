import itertools
from datetime import datetime, time, timedelta
from multiprocessing import Process, Queue

import pandas as pd
import pytz
import requests


class ApiReachDynamicsMixin:

    queue = Queue()

    def __init__(self):
        self.cred_headers = {
                'api_key':'OkvrEZcAsXTr/PrCUkQ6o5ZUk1xT5ayGdWpkW+gI61ktLbpDWVo0vB0aZonkOneVV6M='
                            }
        self.url = 'http://api.reachdynamics.com/api/v1.0/'
        self.company_details = self.get_client_details
    
    @staticmethod
    def get_honolulu_date():
        date = pytz.timezone('Pacific/Honolulu')
        honolulu_date = datetime.now(date)

        previous_day_date = honolulu_date-timedelta(days=1)
        return previous_day_date.date().strftime("%Y-%m-%d")+"T00:00:00"


    
    @property
    def get_client_details(self):

        api_url = self.url+'agency/clients'
        response = requests.request(url=api_url, method='GET', headers=self.cred_headers)
        return response.json()
    
    def get_page_visit_by_date(self):

        final_data = []
        api_url = self.url+'attribution/senddetails'
        for client in self.get_client_details:
            params = {'accountId':client.get('accountId')}
            response = requests.request(url=api_url, method='GET', headers=self.cred_headers, 
                                        params=params)
            for data in response.json():
                final_data.append({
                    'accountId':data.get('accountId'),
                    'clientName':client.get('clientName'),
                    'email':data.get('audienceMemberAttributes').get('Email'),
                    'fname':data.get('firstName',None),
                    'lname':data.get('lastName',None),
                    'pagevisits':data.get('audienceMemberAttributes').get('PageVisits'),
                    'phoneNumber':data.get('audienceMemberAttributes').get('PhoneNumber', None)
                })
        new_data = pd.json_normalize(
                final_data,
                record_path=['pagevisits'],
                meta=["accountId","clientName","email","fname","lname","phoneNumber"]
                )
        new_data['PageViewDateTime'] =  pd.to_datetime(new_data['PageViewDateTime']).dt.strftime('%Y-%m-%d')
        del new_data['IP']
        del new_data['PixelUrl']
        return new_data
        

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
                    'clientName':client.get('clientName'),
                    'initialDate':data.get('audienceMemberAttributes').get('InitialPageViewedOn'),
                    'phoneNumber':data.get('audienceMemberAttributes').get('PhoneNumber', None)
                })
        new_data = pd.DataFrame(final_data)
        new_data['initialDate'] =  pd.to_datetime(new_data['initialDate']).dt.strftime('%Y-%m-%d')
        return new_data
        
    
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
            
        response = requests.request(url=api_url, method='POST', headers=self.cred_headers, 
                                   params=params, json=payload)
        response_data = response.json()
        for data in response_data:
            data['Date'] = data.pop('actionDate')
        self.queue.put(response_data)

    @property
    def get_total_leads(self):

        api_url = self.url+'reports/dataSummary'
        params = {
            "Columns":["CompanyName","AccountId","QueuedOnDate"]
        }
        payload = [
            {
                "ColumnName": "StartDate"
            },
            {
                "ColumnName":"EndDate"
            }
        ]
        response = requests.request(url=api_url, method='POST', headers=self.cred_headers,
                                    params=params, json=payload)
        response_data = response.json()
        for data in response_data:
            data['Date'] = data.pop('queuedOnDate')
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
            
        response = requests.request(url=api_url, method='POST', headers=self.cred_headers,
                                    params=params, json=payload)
        response_data = response.json()
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
            
        response = requests.request(url=api_url, method='POST', headers=self.cred_headers, 
                                    params=params, json=payload)
        response_data = response.json()
        for data in response_data:
            data['Date'] = data.pop('statDate')
            data['dispclick'] = data.pop('clicks')
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
                                  self.get_social_summary,
                                  self.get_total_leads)
        return data

    @property
    def get_final_summary_data(self):
        response = self.fetch_multiple_data
        final_data = list(itertools.chain(*response))
        df = pd.DataFrame(final_data)
        df.fillna(0, inplace=True)
        aggregation_functions = {'Date':'first',
                                'quantitySent': 'sum',
                                'cost': 'sum','delivered': 'sum',
                                'impressions': 'sum','opens': 'sum',
                                'clicks': 'sum','bounced': 'sum',
                                'unsubs': 'sum','ctr':'sum','attempted':'sum',
                                'openRate':'sum','dispclick':'sum','quantityDelivered':'sum'}

        df_new = df.groupby(['Date','accountId'], as_index=False).aggregate(aggregation_functions)
        account_id_values = df_new['accountId'].items()
        account_id_list = []
        for _, acc_id in account_id_values:
            account_id_list.append(acc_id)

        company_details = self.company_details
        companyName = []
        for aId in account_id_list:
            data = next(
            (d for d in company_details \
                if d.get('accountId') == aId), None)
            companyName.append(data.get('clientName'))
        df_new['companyName'] = companyName       
        df_new['Date'] = pd.to_datetime(df_new['Date'])
        df_new['Date']  = df_new['Date'].dt.strftime('%Y-%m-%d').replace("'", "")

        df_new.rename(columns={
            'quantitySent':'Direct Mail Sent',
            'cost':'Direct Mail Cost',
            'delivered':'Emails delivered',
            'impressions':'Social impressions',
            'opens':'Email Opened',
            'clicks':'Email Clicked',
            'ctr':'Email Ctr',
            'attempted':'Email Attempted',
            'openRate':'Email Open Rate',
            'dispclick':'Display Clicks',
            'quantityDelivered':'Total Leads'
        }, inplace = True)
        return df_new


api = ApiReachDynamicsMixin()






