# coding: utf-8
from pyhive import hive
import digitforce.aip.common.utils.config_helper as config_helper

hive_config = config_helper.get_module_config("hive")


class HiveClient:
    def __init__(self, host=None, port=None, username='root'):
        if host is None or port is None:
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
