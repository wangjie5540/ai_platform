import findspark
findspark.init()
import os
from pyspark.sql import SparkSession
import pandas
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from graphframes import *
import pandas as pd
import numpy as np
import time

os.environ['PYSPARK_PYTHON'] = "./pjy-pyspark3.6/pjy-pyspark3.6/bin/python"

class SparkEnv:
    def __new__(self, envname):


        self.spark = (SparkSession.builder.appName("fugou-predict")
                 .master("yarn")
                 .config("spark.yarn.dist.archives", "/data/anaconda3/envs/pjy-pyspark3.6.zip#pjy-pyspark3.6")
                 .config("spark.yarn.queue", "bdp")
                 .config("spark.executor.instances", "3")
                .config("spark.executor.memory","4g")
                .config("spark.executor.cores","2")
                .config("spark.driver.memory","4g")
                .config("spark.driver.maxResultSize", "2g")
            #     .config("spark.sql.shuffle.partitions","200")
                .config("spark.default.parallelism","600")
                .config("spark.network.timeout","60s") 
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.join.enabled", "true")   
                .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.dynamicAllocation.minExecutors", "1")
                .config("spark.dynamicAllocation.maxExecutors", "25")
                .config("spark.shuffle.service.enabled", "true")
                .config("spark.jars", "/usr/local/service/spark/jars/graphframes-0.8.2-spark3.0-s_2.12.jar")
                .enableHiveSupport()
                 .getOrCreate())
        self.spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
        self.spark.sql("set hive.exec.dynamic.partition=true")
        self.spark.sql("set spark.sql.hive.mergeFiles=true")
        return self.spark
    
    def stop_spark_env(self):
        self.spark.stop()