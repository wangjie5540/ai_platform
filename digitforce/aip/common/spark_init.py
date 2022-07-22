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

