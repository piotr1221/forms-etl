from util.filter import LastRecordedValue
from datasource.connection_wrapper import SQLAlchemyConnectionWrapper

class FilterStrategy():
    pass
    
class FilterByLastRecordedValueStrategy(FilterStrategy):

    @classmethod
    def get_filter_value(cls, conn: SQLAlchemyConnectionWrapper, table: str) -> list[LastRecordedValue]:
        filter_values = conn.execute(cls._query(table)).fetchall()
        page_emails = {}
        page_datetime = {}

        for filter_value in filter_values:
            if filter_value[1] in page_emails.keys():
                page_emails[filter_value[1]].append( filter_value[2] )
            else:
                page_emails.setdefault(filter_value[1], [filter_value[2]])

            if filter_value[1] not in page_datetime.keys():
                page_datetime[filter_value[1]] = filter_value[0]

        return [LastRecordedValue(page_datetime[key], key, page_emails[key])
                for key in page_emails.keys()]

    @classmethod
    def get_filter_value_by_page(cls, filter_values: list[LastRecordedValue], page: str):
        return next((filter_value for filter_value in filter_values if filter_value.page == page))

    @classmethod
    def _query(cls, db_table: str):
        return \
    f"""SELECT
        t1.ultima_fecha,
        t1.pagina,
        t2.correo
    FROM (
        SELECT
            MAX(marca_temporal) AS ultima_fecha,
            pagina
        FROM
            {db_table}
        GROUP BY
            pagina
    ) AS t1
    INNER JOIN
        {db_table} AS t2
        ON t2.pagina = t1.pagina
        AND t2.marca_temporal = t1.ultima_fecha;
    """