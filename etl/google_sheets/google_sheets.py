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
                 filter_values: list[FilterValue], excluded_pages: list[str]):
        self.google_sheets_pages = []
        self.service = service
        self.google_sheets_file_id = google_sheets_file_id
        self.pages_in_db = pages_in_db
        self.filter_values = filter_values
        self.excluded_pages = excluded_pages

    def range(self, range: str):
        self._range = range
        return self

    def build(self):
        page_titles = [(page['properties']['sheetId'], page['properties']['title'])
                        for page
                        in self.service.spreadsheets()
                        .get(spreadsheetId=self.google_sheets_file_id)
                        .execute()['sheets']
                        if page['properties']['title'] not in self.excluded_pages]
        
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
    
    def columns(self, columns: list[str]):
        self.google_sheets_file.columns = columns
        return self
    
    def db_table(self, db_table: str):
        self.google_sheets_file.db_table = db_table
        return self
    
    def wordpress_link(self, wordpress_link: str):
        self.google_sheets_file.wordpress_link = wordpress_link
        return self
    
    def seminar_title(self, seminar_title: str):
        self.google_sheets_file.seminar_title = seminar_title
        return self
    
    def excluded_columns(self, excluded_columns: str):
        self.google_sheets_file.excluded_columns = excluded_columns
        return self
    
    def google_sheets_pages(self, service: Resource, range: str, pages_in_db: list[str],
                            filter_values: list[FilterValue], excluded_pages: list[str]):
        self.google_sheets_file.google_sheets_pages = \
            (_GoogleSheetsPageListBuilder(
                    service,
                    self.google_sheets_file.id,
                    pages_in_db,
                    filter_values,
                    excluded_pages
                ).range(range)
                .build())
        return self
    
    def build(self):
        return self.google_sheets_file

        

class GoogleSheetsFile():
    
    def __init__(self, id: str=None,
                 google_sheets_pages: list[GoogleSheetsPage]=None,
                 db_table: str=None,
                 columns: list[str]=None,
                 wordpress_link: str=None,
                 seminar_title: str=None,
                 excluded_columns: list[str]=None) -> None:
        self.id = id
        self.google_sheets_pages = google_sheets_pages
        self.db_table = db_table
        self.columns = columns
        self.wordpress_link = wordpress_link
        self.seminar_title = seminar_title
        self.excluded_columns = excluded_columns

    def get_google_sheets_columns(self) -> list[str]:
        """Returns columns found in original google sheets.
        Excludes database Primary Key, tab name, google sheets link, wordpress link,
        seminar name and status"""
        return self.columns[1:-5]
    
    def get_custom_columns(self) -> list[str]:
        """Returns only custom columns not found in original google sheets
        nor database Primary Key. These columns are tab name, google sheets link,
        wordpress link, seminar name and status"""
        return self.columns[-5:]

    @classmethod
    def builder(cls) -> GoogleSheetsFileBuilder:
        return GoogleSheetsFileBuilder()


class GoogleSheetsFileBatch():

    GOOGLE_SHEETS_CONFIG = json.loads(os.environ['GOOGLE_SHEETS_CONFIG'])

    def __init__(self, datasource: MySQLDataSource, service: Resource):
        self.datasource = datasource
        self.google_sheets_files: list[GoogleSheetsFile] = []
        
        with self.datasource as conn:
            for google_sheets_file_metadata in self.GOOGLE_SHEETS_CONFIG['google_sheets_files_metadata']:
                pages_in_db = \
                    [row[0]
                    for row
                    in conn.execute(
                        self.pages_in_db_query(google_sheets_file_metadata['db_table']))
                        .fetchall()]
                
                columns = \
                    [row[0]
                    for row
                    in conn.execute(
                        self.columns_from_table_query(
                            self.datasource._DB, google_sheets_file_metadata['db_table']))
                        .fetchall()]    
                
                filter_values = FilterByLastRecordedValueStrategy.get_filter_value(
                    conn, google_sheets_file_metadata['db_table']
                )

                excluded_columns = (google_sheets_file_metadata.get('excluded_columns')
                                    if google_sheets_file_metadata.get('excluded_columns') is not None
                                    else self.GOOGLE_SHEETS_CONFIG['common_metadata']['excluded_columns'])

                self.google_sheets_files.append(
                    GoogleSheetsFile.builder()
                        .id(google_sheets_file_metadata['id'])
                        .google_sheets_pages(
                            service,
                            google_sheets_file_metadata['range'],
                            pages_in_db,
                            filter_values,
                            google_sheets_file_metadata.get('excluded', []))
                        .db_table(google_sheets_file_metadata['db_table'])
                        .columns(columns)
                        .wordpress_link(google_sheets_file_metadata['wp_link'])
                        .seminar_title(google_sheets_file_metadata['seminar'])
                        .excluded_columns(excluded_columns)
                        .build()
                )
                
    def pages_in_db_query(self, db_table: str):
        return f"SELECT DISTINCT pestania FROM {db_table}"
    
    def columns_from_table_query(self, database:str, db_table: str):
        return f"""SELECT `COLUMN_NAME` 
                FROM `INFORMATION_SCHEMA`.`COLUMNS` 
                WHERE `TABLE_SCHEMA`='{database}' 
                AND `TABLE_NAME`='{db_table}';
                """