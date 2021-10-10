import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from reachapi import api


class GoogleWorkSheetMixin:

    SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
             ]

    def __init__(self):

        self.creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", self.SCOPES)
        self.key = '1YVHs5P_auwfruhspFFNoRQAyu_Do6NRa44CJ-AGufK8'
    
    @property
    def authorize_cred(self):
        return gspread.authorize(self.creds)
    
    @property
    def open_sheet(self):
        if self.key is None:
            return self.authorize_cred.open_by_url("https://docs.google.com/spreadsheets/d/1YVHs5P_auwfruhspFFNoRQAyu_Do6NRa44CJ-AGufK8")
        return self.authorize_cred.open_by_key(self.key)
    
    def create_worksheet(self):
        """
        Methods that Create worksheet
        """
        get_existing_worksheet = [sheet.title for sheet in self.open_sheet.worksheets()]
        for detail in api.get_client_details:
            if not detail.get('clientName') in get_existing_worksheet:
                self.open_sheet.add_worksheet(title=detail.get('clientName'), rows="1700", cols="6")

    def write_client_data_to_googlesheet(self):

        data = api.get_account_details()
        
        sheet_details = self.open_sheet.worksheet('Client Data')
        #top append use append_rows
        sheet_details.update([data.columns.values.tolist()] + data.values.tolist())
    
    def write_client_summary_to_googlesheet(self):

        data = api.get_final_summary_data

        sheet_details = self.open_sheet.worksheet('Client Details')
        sheet_details.update([data.columns.values.tolist()] + data.values.tolist())
    
    def write_all_client_details(self):

        data = api.get_client_details
        final_data = pd.DataFrame(data)

        sheet_details = self.open_sheet.worksheet('All Clients Account name')
        sheet_details.update([final_data.columns.values.tolist()] + final_data.values.tolist())
    
    def write_datewise_page_visit(self):
        
        data = api.get_page_visit_by_date()

        sheet_details = self.open_sheet.worksheet('Datewise page visits')
        sheet_details.update([data.columns.values.tolist()] + data.values.tolist())



spreadsheet_instance = GoogleWorkSheetMixin()







