import os
import logging
from dotenv import load_dotenv
import json

from googleapiclient.discovery import build
from google.oauth2 import service_account
import google.cloud.logging

from etl.google_sheets.google_sheets_extractor import GoogleSheetsExtractor
from etl.google_sheets.google_sheets import GoogleSheetsFileBatch
from datetime import datetime as dt
from datasource.mysql import MySQLDataSource

load_dotenv()

def main() -> None:
    start = dt.now()

    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s - %(asctime)s - %(message)s')
    client = google.cloud.logging.Client()
    client.setup_logging()

    logging.info(f'main - Execution started at {start}')

    try:

        CREDENTIALS_FILE = 'data-campus-393820-49e0dec8ba65.json'
        # CREDENTIALS_FILE = 'forms-etl-test.json'

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE)
        
        # with build('compute', 'v1', credentials=credentials) as compute:
        #     res = compute.instances().start(project='data-campus-393820', zone='us-central1-a', instance='forms-etl-test').execute()
        #     print(res)
        # gsheets_service = build('sheets', 'v4', credentials=credentials)

        GOOGLE_SHEETS_CONFIG = json.loads(os.environ['GOOGLE_SHEETS_CONFIG'])
        sheet_id = GOOGLE_SHEETS_CONFIG['google_sheets_files_metadata'][0]['id']
        
        with build('sheets', 'v4', credentials=credentials) as gsheets_service:
            page_titles = [(page['properties']['sheetId'], page['properties']['title'])
                            for page
                            in gsheets_service.spreadsheets()
                            .get(spreadsheetId=sheet_id)
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
    except Exception as e:
        logging.error(e)

    end = dt.now()
    logging.info(f'main - Execution finished at {end}.')
    logging.info(f'main - Execution duration of {end - start}.')
    # logging.info(f'main - {extractor.total_new_rows} total new rows.')

main()