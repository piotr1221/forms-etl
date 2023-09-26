import os
import logging
from dotenv import load_dotenv

import functions_framework
from cloudevents.http.event import CloudEvent
from googleapiclient.discovery import build
from google.oauth2 import service_account

from etl.google_sheets.google_sheets_extractor import GoogleSheetsExtractor
from etl.google_sheets.google_sheets import GoogleSheetsFileBatch
from datetime import datetime as dt
from datasource.mysql import MySQLDataSource

load_dotenv()

# @functions_framework.cloud_event
def main() -> None:
    start = dt.now()

    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s - %(asctime)s - %(message)s')

    logging.info(f'main - Execution started at {start}')

    CREDENTIALS_FILE = 'data-campus-393820-49e0dec8ba65.json'
    # CREDENTIALS_FILE = 'forms-etl-test.json'

    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE)
    
    # with build('compute', 'v1', credentials=credentials) as compute:
    #     res = compute.instances().start(project='data-campus-393820', zone='us-central1-a', instance='forms-etl-test').execute()
    #     print(res)
    # gsheets_service = build('sheets', 'v4', credentials=credentials)
    
    with build('sheets', 'v4', credentials=credentials) as gsheets_service:
        page_titles = [(page['properties']['sheetId'], page['properties']['title'])
                        for page
                        in gsheets_service.spreadsheets()
                        .get(spreadsheetId='1BMAOUHTQfzaOl9xJfzpa0mn13Yaq4w2Zsp1oXEOh-4k')
                        .execute()['sheets']]
        
        print(page_titles)

        # mysql_datasource = MySQLDataSource(os.environ['DB_HOST'],
        #                                 os.environ['DB_USER'],
        #                                 os.environ['DB_PASSWORD'],
        #                                 os.environ['DB_DATABASE'])

        # extractor = GoogleSheetsExtractor(mysql_datasource, gsheets_service)

        # google_sheets_file_batch = GoogleSheetsFileBatch(mysql_datasource, gsheets_service)    

        # for google_sheets_file in google_sheets_file_batch.google_sheets_files:
        #     extractor.execute(google_sheets_file)

    # gsheets_service.close()

    end = dt.now()
    logging.info(f'main - Execution finished at {end}.')
    logging.info(f'main - Execution duration of {end - start}.')
    # logging.info(f'main - {extractor.total_new_rows} total new rows.')

main()