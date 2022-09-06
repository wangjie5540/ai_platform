# !/usr/bin/env python3
# encoding: utf-8
'''
@file: spark_env.py
@time: 2022/3/23 14:50
@desc: 连接spark类
'''
import os
import logging
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.config_helper as config_helper
starrocks_config = config_helper.get_module_config("starrocks")


class SparkEnv:
    def __init__(self, name):
        self.sparkClient = SparkClient(name)
        self.spark = self.sparkClient.get_session()
        # self.spark = (SparkSession.builder.appName(name)
        #               .master("yarn")
        #               .config("spark.yarn.dist.archives", "/data/anaconda3/envs/pjy-pyspark3.6.zip#pjy-pyspark3.6")
        #               .config("spark.yarn.queue", "bdp")
        #               .config("spark.executor.instances", "10")
        #               .config("spark.executor.memory", "8g")
        #               .config("spark.executor.cores", "2")
        #               .config("spark.driver.memory", "8g")
        #               .config("spark.driver.maxResultSize", "2g")
        #               .config("spark.sql.shuffle.partitions", "600")
        #               .config("spark.default.parallelism", "2000")
        #               .config("spark.network.timeout", "60s")
        #               .config("spark.sql.adaptive.enabled", "true")
        #               .config("spark.sql.adaptive.join.enabled", "true")
        #               .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000")
        #               .config("spark.sql.hive.convertMetastoreParquet", "false")
        #               .config("spark.dynamicAllocation.enabled", "true")
        #               .config("spark.dynamicAllocation.minExecutors", "1")
        #               .config("spark.dynamicAllocation.maxExecutors", "25")
        #               .config("spark.shuffle.service.enabled", "true")
        #               .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
        #               .config("spark.jars", "hdfs:///user/algorithm/starrocks-spark2_2.11-1.0.0.jar")
        #               .enableHiveSupport()
        #               .getOrCreate())
        self.spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
        self.spark.sql("set hive.exec.dynamic.partition=true")
        self.spark.sql("set spark.sql.hive.mergeFiles=true")


def spark_read(sparkClient, table_name, tempViewName, partitionField, minTime, maxTime):
    df = sparkClient.get_starrocks_table_df(table_name)
    df.filter((df[partitionField] >= minTime) & (df[partitionField] <= maxTime)).write.saveAsTable(tempViewName,
                                                                                                   mode="overwrite")
    print(f'success creating temp hive table: {tempViewName}')


if __name__ == '__main__':
    pass
