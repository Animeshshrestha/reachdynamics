import json

from flask import Flask, abort, request
from flask.json import jsonify

from spreadsheet import spreadsheet_instance

app = Flask(__name__)

@app.route('/google-sheet', methods=['POST'])
def webhook():
    if request.method == 'POST':
        try:
            #to insert client details and account id on googlesheet
            spreadsheet_instance.write_all_client_details()
            # #to insert data on client details sheet
            spreadsheet_instance.write_client_summary_to_googlesheet()

            #to insert data on client data sheet
            spreadsheet_instance.write_client_data_to_googlesheet()
            #to insert data on datewise page visit sheets
            spreadsheet_instance.write_datewise_page_visit()
            return {
                "message":"SpreadSheet Updated Sucessfully"
            }
        except Exception as e:
            return {
                "message":"SpreadSheet Not Updated Sucessfully",
                "errors":jsonify(e)
            }
    else:
        abort(400)

