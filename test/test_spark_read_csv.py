import digitforce.aip.common.utils.spark_helper as spark_helper
import findspark
findspark.init()

spark = spark_helper.SparkClient.get().get_session()
df = spark.read.csv("hdfs://HDFS8001206/tmp/wtg_test_replace.csv", header=True, inferSchema=True, sep='\001')
df.show()
