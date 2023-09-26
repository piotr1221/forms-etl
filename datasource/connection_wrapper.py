import abc

from sqlalchemy.sql import text

class ConnectionWrapper(metaclass=abc.ABCMeta):
    
    @abc.abstractmethod
    def execute(self, query: str, data: any):
        raise NotImplementedError
    
    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
    
    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'execute') and 
                callable(subclass.execute) and 
                hasattr(subclass, 'close') and 
                callable(subclass.close) and
                hasattr(subclass, 'commit') and 
                callable(subclass.commit) or 
                NotImplemented)

class SQLAlchemyConnectionWrapper(ConnectionWrapper):

    def __init__(self, sql_alchemy_conn):
        self.conn = sql_alchemy_conn

    def execute(self, query: str, data: list[dict]=None):
        if data is not None:
            return self.conn.execute(text(query), data)
        return self.conn.execute(text(query))
    
    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()