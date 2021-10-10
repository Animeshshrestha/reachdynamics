from spreadsheet import spreadsheet_instance

#to insert client details and account id on googlesheet
spreadsheet_instance.write_all_client_details()
# #to insert data on client details sheet
spreadsheet_instance.write_client_summary_to_googlesheet()

#to insert data on client data sheet
spreadsheet_instance.write_client_data_to_googlesheet()

#to insert data on datewise page visit sheets
spreadsheet_instance.write_datewise_page_visit()

# from apscheduler.schedulers.blocking import BlockingScheduler

# sched = BlockingScheduler()
# from spreadsheet import spreadsheet_instance

# def scheduled_job():
#     #to insert client details and account id on googlesheet
#     spreadsheet_instance.write_all_client_details()
#     # #to insert data on client details sheet
#     spreadsheet_instance.write_client_summary_to_googlesheet()

#     #to insert data on client data sheet
#     spreadsheet_instance.write_client_data_to_googlesheet()

# sched.add_job(scheduled_job, 'interval', hours=24)
# sched.start()
