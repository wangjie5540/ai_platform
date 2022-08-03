import pandas as pd
from pyhive import hive

from digitforce.aip.common.config.bigdata_config import HIVE_HOST, HIVE_PORT


def _get_conn(func):
    def wrapper(self, *args, **kwargs):
        self.get_conn()
        res = func(self, *args, **kwargs)
        return res

    return wrapper


class HiveClient:
    def __init__(self, host=None, port=None, username=None, database='default', auth=None,
                 configuration=None, kerberos_service_name=None, password=None, ):
        self.conn = None
        self.cursor = None
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.auth = auth
        self.configuration = configuration
        self.kerberos_service_name = kerberos_service_name
        self.password = password

        self.conn = None

    def get_conn(self):
        if self.conn is None:
            conn = hive.Connection(host=self.host, port=self.port)
            self.conn = conn
        return self.conn

    @_get_conn
    def query_to_df(self, sql):
        df = pd.read_sql(sql, self.conn)
        return df

    @_get_conn
    def query_to_table(self, sql, table_name, db=None, delete_tb=False):
        cursor = self.conn.cursor()
        if delete_tb:
            self.delete_table(table_name)
        table_name = f"{db}.{table_name}" if db else table_name
        _sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS " \
               f"{sql}"
        cursor.execute(_sql)

    @_get_conn
    def delete_table(self, table_name):
        cursor = self.conn.cursor()
        _sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(_sql)

    def close(self):
        if self.conn:
            self.conn.close()
        self.conn = None


df_hive_helper = HiveClient(host=HIVE_HOST, port=HIVE_PORT)

if __name__ == '__main__':
    df = df_hive_helper.query_to_df("select * from algorithm.sku_profile limit 100")
    print(df)
    from pyhiveConnection import hiveConnector

    cursor = hiveConnector.connection("10.170.1.35:2181,10.170.1.40:2181", "/hiveserver2", "serverUri", "admin", None,
                                      "tmp")