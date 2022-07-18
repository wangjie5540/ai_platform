import os

from digitforce.aip.common.utils.spark_helper import SparkClient

class SparkEnv:
    def __new__(self, envname):
        self.spark = SparkClient().get_session()
        return self.spark

def spark_read(spark, table_name, tempViewName):
    df = spark.read.format('starrocks').option('starrocks.table.identifier', f'{table_name}').option(
        'starrocks.fenodes', 'bigdata-server-07:8030').option('user', 'root').option('password', '').load()
    df.createOrReplaceTempView(f'{tempViewName}')
    print(f'success creating temp view: {tempViewName}')
