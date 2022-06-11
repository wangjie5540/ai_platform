# import findspark
# findspark.init()
# import os
# from pyspark.sql import SparkSession
# import pandas
# from pyspark.sql.types import *
# from pyspark.sql.functions import *
# # from graphframes import *
# import pandas as pd
# import numpy as np
# import time


# coding: utf-8
import subprocess
import findspark
import os
os.environ['SPARK_HOME'] = '/bd-components/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark'
os.environ['JAVA_HOME'] = '/usr/java/jdk1.8.0_181-cloudera'
os.environ['PYSPARK_PYTHON'] = "./pjy-pyspark3.6/pjy-pyspark3.6/bin/python"
findspark.init()

from pyspark.sql import SparkSession

class SparkEnv:
    def __new__(self, envname):
        self.spark = SparkSession.builder.master('yarn')\
            .appName(envname)\
            .config("spark.yarn.dist.archives", "/data/anaconda3/envs/pjy-pyspark3.6.zip#pjy-pyspark3.6")\
            .enableHiveSupport().getOrCreate()
        return self.spark
