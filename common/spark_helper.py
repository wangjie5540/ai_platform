from pyspark.sql import SparkSession


def build_spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name).master("yarn")
             .enableHiveSupport()
             .getOrCreate())
    return spark
