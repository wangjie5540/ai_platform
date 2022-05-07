from pyspark.sql import SparkSession


def build_spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name).master("yarn")
             .enableHiveSupport()
             .config("spark.yarn.queue", "bdp")
             .config("spark.executor.instances", "3")
             .config("spark.executor.memory", "4g")
             .config("spark.executor.cores", "2")
             .config("spark.driver.memory", "4g")
             .config("spark.driver.maxResultSize", "2g")
             #     .config("spark.sql.shuffle.partitions","200")
             .config("spark.default.parallelism", "600")
             .config("spark.network.timeout", "60s")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.join.enabled", "true")
             .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000")
             .config("spark.sql.hive.convertMetastoreParquet", "false")
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.dynamicAllocation.minExecutors", "1")
             .config("spark.dynamicAllocation.maxExecutors", "25")
             .config("spark.shuffle.service.enabled", "true")
             .config("spark.jars",
                     "/bd-components/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark/jars/graphframes-0.8.2-spark2.4-s_2.11.jar")
             .getOrCreate())
    return spark
