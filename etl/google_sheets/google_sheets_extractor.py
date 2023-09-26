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

    def execute(self, google_sheets_file: GoogleSheetsFile) -> None:
        logging.info(f"{type(self).__name__} - Extraction started for file '{google_sheets_file.id}'")

        new_rows_per_file = 0

        for google_sheets_page in google_sheets_file.google_sheets_pages:
            logging.info(f"{type(self).__name__} - Extraction started for page '{google_sheets_page.title}'")

            values = self._get_excel_values(google_sheets_file.id, google_sheets_page)
            df = self._generate_dataframe(values, google_sheets_file, google_sheets_page)
            df = self.datetime_filter.filter(df, google_sheets_page.filter_value, google_sheets_page.mode)
            
            if not df.is_empty():
                df.write_database(
                    google_sheets_file.db_table, self.datasource.url, if_exists='append'
                )

            new_rows_per_file += df.height

            logging.info(f"{type(self).__name__} - Extraction finished for page '{google_sheets_page.title}'. {df.height} new rows")

        self.total_new_rows += new_rows_per_file

        logging.info(f"{type(self).__name__} - Extraction finished for file '{google_sheets_file.id}'. {new_rows_per_file} new rows")

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
        header = (google_sheets_file.shared_existing_columns
            + [f'pregunta_{i}' for i in range(1, google_sheets_file.questions + 1)])

        columnar_values = [columnar_value[1:] for columnar_value in values]

        data = {column: columnar_values[idx]
                for (idx, column) in enumerate(header)}

        schema =  {h: pl.Utf8 for h in header}

        new_cols = [pl.lit(value).alias(column)
                    for column, value
                    in zip(google_sheets_file.shared_new_columns,
                            google_sheets_page.get_title_and_link())]

        df = (pl.DataFrame(data, schema=schema)
                .with_columns([
                    pl.col(f'{header[0]}').str
                        .to_datetime(self.SOURCE_DATETIME_STRING_FORMAT),
                    pl.col(f'{header[1]}').str
                        .split('/')
                        .list
                        .get(0)
                        .str
                        .strip_chars()
                        .cast(pl.UInt8),
                    pl.col(f'{header[5]}').str.replace_all(' ', ''),
                    pl.col(f'{header[6]}').str.replace_all(' ', ''),
                    *new_cols
                ]))
        return df
    