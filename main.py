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

        CREDENTIALS_FILE = 'educared-datos-forms-etl-sa.json'

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE)
        
        with build('sheets', 'v4', credentials=credentials) as gsheets_service:

            mysql_datasource = MySQLDataSource(os.environ['DB_HOST'],
                                            os.environ['DB_USER'],
                                            os.environ['DB_PASSWORD'],
                                            os.environ['DB_DATABASE'])

            google_sheets_file_batch = GoogleSheetsFileBatch(mysql_datasource, gsheets_service)    

            extractor = GoogleSheetsExtractor(mysql_datasource, gsheets_service)
            extractor.execute(
                google_sheets_files_list=google_sheets_file_batch.google_sheets_files
            )

    except Exception as e:
        logging.exception(e)

    end = dt.now()
    logging.info(f'main - Execution finished at {end}.')
    logging.info(f'main - Execution duration of {end - start}.')
    logging.info(f'main - {extractor.total_new_rows} total new rows.')

main()