import logging
from datetime import datetime as dt

import polars as pl
from googleapiclient.discovery import Resource

from etl.google_sheets.google_sheets import GoogleSheetsPage, GoogleSheetsFile
from util.filter import DateTimeFilterByLastRecordedValue
from datasource.mysql import MySQLDataSource


class GoogleSheetsExtractor():

    SOURCE_DATETIME_STRING_FORMAT = '%d/%m/%Y %H:%M:%S'
    datetime_filter = DateTimeFilterByLastRecordedValue()

    def __init__(self, mysql_datasource: MySQLDataSource, service: Resource) -> None:
        self.datasource = mysql_datasource
        self.service = service
        self.total_new_rows = 0

    def execute(self, *, google_sheets_file: GoogleSheetsFile=None,
                google_sheets_files_list: list[GoogleSheetsFile]=None) -> None:
        if google_sheets_file is not None and google_sheets_files_list is not None:
            raise Exception('Only one argument must be non null')
        elif google_sheets_file is None and google_sheets_files_list is None:
            raise Exception('Only one argument must be null')
        elif google_sheets_file is not None:
            self._execute_single_file(google_sheets_file)
        elif google_sheets_files_list is not None:
            self._execute_file_list(google_sheets_files_list)

    def _execute_single_file(self, google_sheets_file: GoogleSheetsFile, counter: dict=None):
        logging.info(f"{type(self).__name__} - Extraction started for file '{google_sheets_file.id}'")

        new_rows_per_file = 0

        file_df: pl.DataFrame = None

        for i, google_sheets_page in enumerate(google_sheets_file.google_sheets_pages):
            if counter is None:
                logging.info((f"{type(self).__name__} - Extraction started for page '{google_sheets_page.title}'."
                            f" Page {i + 1}/{len(google_sheets_file.google_sheets_pages)}"))
            else:
                logging.info((f"{type(self).__name__} - Extraction started for page '{google_sheets_page.title}'."
                            f" File {counter['current_file_idx']}/{counter['total_files']}."
                            f" Page {i + 1}/{len(google_sheets_file.google_sheets_pages)}"))

            values = self._get_excel_values(google_sheets_file.id, google_sheets_page)
            df = self._generate_dataframe(values, google_sheets_file, google_sheets_page)
            df = self.datetime_filter.filter(df, google_sheets_page.filter_value, google_sheets_page.mode)
            
            if not df.is_empty():
                if file_df is None:
                    file_df = df
                else:
                    file_df = pl.concat([file_df, df])

            new_rows_per_file += df.height

            logging.info(f"{type(self).__name__} - Extraction finished for page '{google_sheets_page.title}'. {df.height} new rows")

        self.total_new_rows += new_rows_per_file

        if file_df is not None:
            file_df.write_database(
                google_sheets_file.db_table, self.datasource.url, if_exists='append'
            )

        logging.info(f"{type(self).__name__} - Extraction finished for file '{google_sheets_file.id}'. {new_rows_per_file} new rows")

    def _execute_file_list(self, google_sheets_files: list[GoogleSheetsFile]):
        for i, file in enumerate(google_sheets_files):
            self._execute_single_file(
                file,
                {'current_file_idx': i + 1, 'total_files': len(google_sheets_files)}
            )

    def _get_excel_values(self, google_sheets_file_id: str,
                          google_sheets_page: GoogleSheetsPage) -> list[list[str]]:
        request = (self.service.spreadsheets()
                    .values()
                    .get(spreadsheetId=google_sheets_file_id,
                        range=google_sheets_page.get_full_range(),
                        majorDimension='COLUMNS',
                        fields="values"))
        response = request.execute()
        return response.get('values', [])
    
    def _generate_dataframe(self, values: list[list[str]],
                            google_sheets_file: GoogleSheetsFile,
                            google_sheets_page: GoogleSheetsPage) -> pl.DataFrame:
        columnar_values = [columnar_value[1:] for columnar_value in values]

        data = {column: columnar_values[idx]
                for (idx, column) in enumerate(google_sheets_file.get_google_sheets_columns())}
        
        schema =  {c: pl.Utf8 for c in google_sheets_file.get_google_sheets_columns()}

        page_title_page_link_and_wordpress_link = [
            *google_sheets_page.get_title_and_link(),
            google_sheets_file.wordpress_link
        ]
        new_cols = [pl.lit(value).alias(column)
                    for column, value
                    in zip(google_sheets_file.get_custom_columns(),
                            page_title_page_link_and_wordpress_link, strict=True)]

        df = (pl.DataFrame(data, schema=schema)
                .with_columns([
                    pl.col(f'{google_sheets_file.get_google_sheets_columns()[0]}').str
                        .to_datetime(self.SOURCE_DATETIME_STRING_FORMAT, strict=False),
                    pl.col(f'{google_sheets_file.get_google_sheets_columns()[1]}').str
                        .split('/')
                        .list
                        .get(0)
                        .str
                        .strip_chars()
                        .cast(pl.UInt8),
                    pl.col(f'{google_sheets_file.get_google_sheets_columns()[5]}').str.replace_all(' ', ''),
                    pl.col(f'{google_sheets_file.get_google_sheets_columns()[6]}').str.replace_all(' ', ''),
                    *new_cols
                ]))
        return df
    