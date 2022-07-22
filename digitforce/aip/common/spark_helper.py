import findspark #使用spark-submit 的cluster时要注释掉
findspark.init()
import os
from pyspark.sql import SparkSession
from digitforce.aip.common.data_helper import tuple_self
import pyspark.sql.functions as psf


def build_spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name).master("yarn")
             .enableHiveSupport()
             .getOrCreate())
    return spark


def forecast_spark_session(app_name):
    """
    初始化特征
    :return:
    """

    os.environ["PYSPARK_DRIVER_PYTHON"]="/data/ibs/anaconda3/bin/python"
    os.environ['PYSPARK_PYTHON']="/data/ibs/anaconda3/bin/python"
    spark=SparkSession.builder \
        .appName(app_name).master('yarn') \
        .config("spark.yarn.queue", "shusBI") \
        .config("spark.executor.instances", "50") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "6g") \
        .config("spark.default.parallelism", "600") \
        .config("spark.network.timeout", "240s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.join.enabled", "true") \
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamici.partition", True) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "10000") \
        .enableHiveSupport().getOrCreate()
    spark.sql("set hive.exec.dynamic.partitions=true")
    spark.sql("set hive.exec.max.dynamic.partitions=2048")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("use ai_dm_dev")
    sc = spark.sparkContext
    zip_path1 = './forecast.zip'
    zip_path2 = './digitforce.zip'
    sc.addPyFile(zip_path1)
    sc.addPyFile(zip_path2)
    return spark


class SparkHelper:

    def __init__(self, spark):
        self.spark = spark

    def get_spark(self):
        return self.spark

    def is_exist_table(self, check_table):
        """
        判断表是否存在
        :param spark:
        :param check_table: 要检查的表
        :return:
        """
        result = False
        try:
            if self.spark.table("{0}".format(check_table)):
                result = True
        except:
            pass
        return result

    def show_columns(self, check_table):
        columns = self.spark.sql("show columns in {0}".format(check_table)).toPandas()['col_name'].tolist()
        return columns

    def read_table(self, table_name, sdt='Y', dt="dt", partition_name='shop_id', partition_list=[]):
        """dt:分区字段
           sdt:时间戳字段
        """
        filter_str = ""
        if len(partition_list) > 0:
            filter_str = " where {0} in {1}".format(partition_name, tuple_self(partition_list))
        sparkdf = self.spark.sql("""select * from {0} {1} """.format(table_name, filter_str))
        if sdt == 'N':
            sparkdf = sparkdf.withColumn("sdt", psf.unix_timestamp(psf.to_timestamp(psf.col(dt), 'yyyyMMdd'),
                                                                   "format='yyyy-MM-dd"))
        return sparkdf

    def save_table(self, sparkdf, table_name, save_mode='overwrite', partition=["shop_id", "dt"]):
        if self.is_exist_table(table_name):
            columns = self.show_columns(table_name)
            print(columns, table_name)
            sparkdf.repartition(1).select(columns).write.mode("overwrite").insertInto(table_name, True)
        else:
            print("save table name", table_name)
            sparkdf.write.mode(save_mode).partitionBy(partition).saveAsTable(table_name)

    def read_origin_table(self, table_name, query_sql, col_name='', col_value=[]):
        if len(col_value) > 0:
            filter_str = " where {0} in {1}".format(col_name, tuple_self(col_value))
        else:
            filter_str = " where 1=1 "
        sparkdf = self.spark.sql(query_sql.format(table_name, filter_str))
        return sparkdf

    # def write_to_csv(location, repartition, encoding, header, file_path, data, mode_type):
    #     """
    #     结果写入csv，分为hdfs和local
    #     :param location: 位置
    #     :param repartition: repartition
    #     :param encoding: encoding
    #     :param header: header
    #     :param file_path: 保存地址
    #     :param data: 数据
    #     :param mode_type:overwrite or append
    #     :return:
    #     """
    #     if location == "hdfs":
    #         data.write.mode(mode_type).format('csv').repartition(repartition).option("encoding", encoding).option(
    #             "header", header).save(file_path)
    #     else:
    #         data_tmp = data.toPandas()  # 拉到本地
    #         data_tmp.to_csv(file_path, index=False, encoding=encoding)


forecast_spark_helper = SparkHelper(forecast_spark_session("forecast_awg"))