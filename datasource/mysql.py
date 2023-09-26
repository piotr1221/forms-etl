import logging

from sqlalchemy import create_engine, URL

from datasource.connection_wrapper import SQLAlchemyConnectionWrapper

class MySQLDataSource():

    def __init__(self, host: str, user: str, password: str, db: str) -> None:
        self._HOST = host
        self._USER = user
        self._PASSWORD = password
        self._DB = db
        self.url = URL.create(
            'mysql+mysqldb',
            username=self._USER,
            password=self._PASSWORD,
            host=self._HOST,
            database=self._DB
        )

    def __enter__(self) -> SQLAlchemyConnectionWrapper:
        engine = create_engine(self.url, echo=False)
        self._CONNECTION = SQLAlchemyConnectionWrapper(engine.connect())
        logging.info(f'MySQL connection to database {self._DB} stablished')
        return self._CONNECTION

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._CONNECTION.close()
        logging.info(f'MySQL connection to database {self._DB} terminated')

    # def connect(self):
    #     engine = create_engine(self._url, echo=False)
    #     self._CONNECTION = SQLAlchemyConnectionWrapper(engine.connect())
    #     logging.info(f'MySQL connection to database {self._DB} stablished')
    #     return self._CONNECTION
    
    # def close(self):
    #     self._CONNECTION.close()
    #     logging.info(f'MySQL connection to database {self._DB} terminated')