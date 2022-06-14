import os

os.environ['SPARK_HOME'] = '/bd-components/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark'
os.environ['JAVA_HOME'] = '/usr/java/jdk1.8.0_181-cloudera'
os.environ['PYSPARK_PYTHON'] = "./pjy-pyspark3.6.2.zip/pjy-pyspark3.6/bin/python"
import findspark

findspark.init()
from pyspark.sql import SparkSession

class SparkEnv:
    def __new__(self, envname):
        self.spark = SparkSession.builder.master('yarn') \
                    .appName(envname) \
                    .config("spark.yarn.dist.archives", "hdfs:///tmp/wtg/pjy-pyspark3.6.2.zip") \
                    .config("spark.jars", "hdfs:///user/algorithm/starrocks-spark2_2.11-1.0.0.jar") \
                    .enableHiveSupport().getOrCreate()
        return self.spark

def spark_read(spark, table_name, tempViewName):
    df = spark.read.format('starrocks').option('starrocks.table.identifier', f'{table_name}').option(
        'starrocks.fenodes', 'bigdata-server-07:8030').option('user', 'root').option('password', '').load()
    df.createOrReplaceTempView(f'{tempViewName}')
    print(f'success creating temp view: {tempViewName}')
