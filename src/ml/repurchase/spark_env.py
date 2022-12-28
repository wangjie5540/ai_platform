from digitforce.aip.common.utils.spark_helper import SparkClient


class SparkEnv:
    def __init__(self, name):
        self.spark = SparkClient(name).get_session()
        self.spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
        self.spark.sql("set hive.exec.dynamic.partition=true")
        self.spark.sql("set spark.sql.hive.mergeFiles=true")


def spark_read(spark, table_name, tempViewName, partitionField, minTime, maxTime):
    df = spark.read.format('starrocks').option('starrocks.table.identifier', f'{table_name}').option(
        'starrocks.fenodes', 'bigdata-server-07:8030').option('user', 'root').option('password', '').load()
    df.filter((df[partitionField] >= minTime) & (df[partitionField] <= maxTime)).write.saveAsTable(tempViewName, mode="overwrite")
    print(f'success creating temp hive table: {tempViewName}')


if __name__ == '__main__':
    spark = SparkEnv('test').spark
    spark.sql('show databases').show()
    spark.stop()
