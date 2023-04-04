import digitforce.aip.common.utils.spark_helper as spark_helper
import pandas as pd
import digitforce.aip.common.utils.id_helper as id_helper
import requests
from io import StringIO


def read_to_table(url: str, columns: str, x_tenant: str = None):
    column_list = columns.strip().split(',')
    headers = {}
    skiprows = 0
    if x_tenant is not None:
        headers['X_TENANT'] = x_tenant
        skiprows = 1
    response = requests.get(url, headers=headers)
    data = StringIO(response.text)
    df = pd.read_csv(data, skiprows=skiprows, names=column_list)
    df = spark_helper.SparkClient.get().get_session().createDataFrame(df)
    table_name = f'aip.cos_{id_helper.gen_uniq_id()}'
    df.write.format("hive").mode("overwrite").saveAsTable(table_name)
    print(f'write to table {table_name} success')
    return table_name, column_list
