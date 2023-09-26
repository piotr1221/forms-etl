import os
import json
import abc
from dotenv import load_dotenv

from datetime import datetime as dt, timedelta
import polars as pl

from constant.enum import Mode

load_dotenv()

class FilterValue():
    
    def __init__(self, datetime_to_filter_by: dt):
        self.datetime_to_filter_by = datetime_to_filter_by


class LastRecordedValue(FilterValue):

    def __init__(self, datetime_to_filter_by: dt, page: str, emails: list[str]):
        super().__init__(datetime_to_filter_by)
        self.page = page
        self.emails = emails


class DateTimeFilter(metaclass=abc.ABCMeta):
    EXTRACTION_INTERVAL = json.loads(os.environ['EXTRACTION_INTERVAL'])
    FIXED_MODE = os.getenv('FIXED_MODE', '')

    def filter(self, df: pl.DataFrame, filter_value: FilterValue=None, mode: Mode=None) -> pl.DataFrame:
        if self.FIXED_MODE:
            mode = Mode[self.FIXED_MODE]

        if mode not in Mode:
            raise Exception('mode must be of enum type Mode')

        if mode == Mode.INCREMENTAL:
            if filter_value is None:
                raise Exception('filter_value can not be None in incremental mode')
            return self._custom_filter(df, filter_value)
        
        if mode == Mode.HISTORICAL:
            return df
        
    @abc.abstractmethod
    def _custom_filter(self, df: pl.DataFrame, filter_value: FilterValue) -> pl.DataFrame:
        raise NotImplementedError
    

class DateTimeFilterByPreviousPeriod(DateTimeFilter):

    INTERVAL_TYPES = ['microseconds', 'seconds', 'minutes', 'hours', 'days']

    def _custom_filter(self, df: pl.DataFrame, filter_value: FilterValue) -> pl.DataFrame:
        previous_hour = \
            (filter_value.datetime_to_filter_by - timedelta(**self.EXTRACTION_INTERVAL)).replace(
                **self._replace_datetime_components()
            )

        return df.filter(
            pl.col('marca_temporal').ge(previous_hour)
        )
    
    def _replace_datetime_components(self) -> dict[str, int]:
        replace = {}
        interval_type = list(self.EXTRACTION_INTERVAL.keys())[0]
        interval_type_idx = self.INTERVAL_TYPES.index(interval_type)

        if interval_type_idx == 0:
            return replace
        
        if interval_type_idx > 0:
            replace['microsecond'] = 0
        
        if interval_type_idx > 1:
            replace['second'] = 0

        if interval_type_idx > 2:
            replace['minute'] = 0

        if interval_type_idx > 3:
            replace['hour'] = 0

        if interval_type_idx > 4:
            replace['day'] = 0

        return replace
    

class DateTimeFilterByLastRecordedValue(DateTimeFilter):
    
    def _custom_filter(self, df: pl.DataFrame, filter_value: LastRecordedValue):
        return df.filter(
            pl.col('marca_temporal').ge(filter_value.datetime_to_filter_by)
            .and_(pl.col('pagina').eq(filter_value.page))
            .and_(pl.col('correo').is_in(filter_value.emails).not_())
        )
    