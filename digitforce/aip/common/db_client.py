import pymysql
from config.db_config import *
from dbutils.pooled_db import PooledDB
import pandas as pd

class DBClient:
    def __init__(self, host, port, user, password, database, charset):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self._conn = None
        self.conn_create_time = -1
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=100,
            mincached=2,
            maxcached=5,
            maxshared=1,
            blocking=True,
            maxusage=None,
            setsession=[],
            ping=1,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset
        )

    def get_connection(self, shareable=False):
        conn = self.pool.connection(shareable)
        conn.ping(1)
        return conn

    def get_connect_url(self):
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}" \
               f"/{self.database}?charset={self.charset}"

    def close(self):
        pass

    def query_to_df(self, sql):
        df = pd.read_sql(sql, self.get_connection())
        return df


if __name__ == '__main__':
    pass
