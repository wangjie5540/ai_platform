# coding: utf-8
import pandas as pd
from pyhive import hive

import digitforce.aip.common.utils.config_helper as config_helper


class HiveClient:
    def __init__(self, host=None, port=None, username='root'):
        if host is None or port is None:
            hive_config = config_helper.get_module_config("hive")
            host = hive_config['server2']['host']
            port = hive_config['server2']['port']
        self.conn = hive.Connection(host=host, port=port, username=username)

    def __del__(self):
        self.conn.close()

    def get_table_size(self, table_name):
        cur = self.conn.cursor()
        sql = f'desc formatted {table_name}'
        cur.execute(sql)
        all_data = cur.fetchall()
        cur.close()
        for item in all_data:
            if item[1] is not None and item[1].startswith('totalSize'):
                return int(item[2].strip())

    def query_to_df(self, sql):
        df = pd.read_sql(sql, self.conn)
        return df

    def query_to_table(self, sql, table_name, db=None, delete_tb=False):
        cursor = self.conn.cursor()
        if delete_tb:
            self.delete_table(table_name)
        table_name = f"{db}.{table_name}" if db else table_name
        _sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS " \
               f"{sql}"
        cursor.execute(_sql)

    def delete_table(self, table_name):
        cursor = self.conn.cursor()
        _sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(_sql)

    def close(self):
        if self.conn:
            self.conn.close()
        self.conn = None


hive_client = HiveClient()
# hive_client = HiveClient(host="172.22.20.57", port=7001)
