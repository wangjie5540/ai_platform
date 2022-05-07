import pandas as pd
from pyhive import hive


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

    def close(self):
        if self.conn:
            self.conn.close()
        self.conn = None


ss_dev_hive_helper = HiveClient(host="172.21.32.74", port=10000)

if __name__ == '__main__':
    df = ss_dev_hive_helper.query_to_df("select * from algorithm.sku_profile limit 100")
    print(df)
