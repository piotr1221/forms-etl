import os
import json
from dotenv import load_dotenv

from googleapiclient.discovery import Resource

from datasource.mysql import MySQLDataSource
from constant.enum import Mode
from util.filter_strategy import FilterByLastRecordedValueStrategy
from util.filter import FilterValue


load_dotenv()

class GoogleSheetsPage():

    def __init__(self, title: str, range: str, mode: Mode, file_id: str, sheet_id: str,
                 filter_value: FilterValue) -> None:
        self.title = title
        self.range = range
        self.mode = mode
        self.link = f'https://docs.google.com/spreadsheets/d/{file_id}/edit?resourcekey#gid={sheet_id}'
        self.filter_value = filter_value

    def get_full_range(self) -> str:
        return f'{self.title}!{self.range}'
    
    def get_title_and_link(self) -> list[str]:
        return [self.title, self.link]
    

class _GoogleSheetsPageListBuilder():

    def __init__(self, service: Resource, google_sheets_file_id: str, pages_in_db: list[str],
                 filter_values: list[FilterValue]):
        self.google_sheets_pages = []
        self.service = service
        self.google_sheets_file_id = google_sheets_file_id
        self.pages_in_db = pages_in_db
        self.filter_values = filter_values

    def range(self, range: str):
        self._range = range
        return self

    def build(self):
        page_titles = [(page['properties']['sheetId'], page['properties']['title'])
                        for page
                        in self.service.spreadsheets()
                        .get(spreadsheetId=self.google_sheets_file_id)
                        .execute()['sheets']]
        
        for page_id, page_title in page_titles:
            mode = Mode.INCREMENTAL if page_title in self.pages_in_db else Mode.HISTORICAL
            filter_value = None

            if mode == Mode.INCREMENTAL:
                filter_value = FilterByLastRecordedValueStrategy.get_filter_value_by_page(
                    self.filter_values, page_title
                )

            self.google_sheets_pages.append(
                GoogleSheetsPage(page_title, self._range,
                                 mode, self.google_sheets_file_id, page_id, filter_value)
            )
        return self.google_sheets_pages


class GoogleSheetsFileBuilder():

    def __init__(self):
        self.google_sheets_file = GoogleSheetsFile()

    def id(self, id: str):
        self.google_sheets_file.id = id
        return self
    
    def questions(self, questions: int):
        self.google_sheets_file.questions = questions
        return self
    
    def shared_existing_columns(self, shared_existing_columns: list[str]):
        self.google_sheets_file.shared_existing_columns = shared_existing_columns
        return self
    
    def shared_new_columns(self, shared_new_columns: list[str]):
        self.google_sheets_file.shared_new_columns = shared_new_columns
        return self
    
    def db_table(self, db_table: str):
        self.google_sheets_file.db_table = db_table
        return self
    
    def google_sheets_pages(self, service: Resource, range: str, pages_in_db: list[str],
                            filter_values: list[FilterValue]):
        self.google_sheets_file.google_sheets_pages = \
            (_GoogleSheetsPageListBuilder(
                    service,
                    self.google_sheets_file.id,
                    pages_in_db,
                    filter_values
                ).range(range)
                .build())
        return self
    
    def build(self):
        return self.google_sheets_file
        

class GoogleSheetsFile():
    
    def __init__(self, id: str=None,
                 questions: int=None,
                 google_sheets_pages: list[GoogleSheetsPage]=None,
                 db_table: str=None,
                 shared_existing_columns: list[str]=None,
                 shared_new_columns: list[str]=None) -> None:
        self.id = id
        self.questions = questions
        self.google_sheets_pages = google_sheets_pages
        self.db_table = db_table
        self.shared_existing_columns = shared_existing_columns
        self.shared_new_columns = shared_new_columns

    @classmethod
    def builder(cls) -> GoogleSheetsFileBuilder:
        return GoogleSheetsFileBuilder()


class GoogleSheetsFileBatch():

    GOOGLE_SHEETS_CONFIG = json.loads(os.environ['GOOGLE_SHEETS_CONFIG'])

    def __init__(self, datasource: MySQLDataSource, service: Resource):
        self.datasource = datasource
        self.google_sheets_files = []
        
        shared_metadata = self.GOOGLE_SHEETS_CONFIG['google_sheets_shared_metadata']

        with self.datasource as conn:
            for google_sheets_file_metadata in self.GOOGLE_SHEETS_CONFIG['google_sheets_files_metadata']:
                pages_in_db = [row[0]
                               for row
                               in conn.execute(self.query(google_sheets_file_metadata['db_table']))
                                    .fetchall()]
                
                filter_values = FilterByLastRecordedValueStrategy.get_filter_value(
                    conn, google_sheets_file_metadata['db_table']
                )

                self.google_sheets_files.append(
                    GoogleSheetsFile.builder()
                        .id(google_sheets_file_metadata['id'])
                        .questions(google_sheets_file_metadata['questions'])
                        .shared_existing_columns(shared_metadata['shared_existing_columns'])
                        .shared_new_columns(shared_metadata['shared_new_columns'])
                        .google_sheets_pages(
                            service,
                            google_sheets_file_metadata['range'],
                            pages_in_db,
                            filter_values)
                        .db_table(google_sheets_file_metadata['db_table'])
                        .build()
                )
                
    def query(self, db_table: str):
        return f"SELECT DISTINCT pagina FROM {db_table}"