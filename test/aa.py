import digitforce.aip.common.utils.spark_helper as spark_helper
import time

client = spark_helper.SparkClient.get()
df = client.get_starrocks_table_df("algorithm.sample_jcbq_zxr")
start = time.time()
df = df.select('age')
df.show()
df.write.mode("overwrite").saveAsTable("algorithm.test_sr9")
# end = time.time()
# print(str(end-start))
# client.get_session().sql("select * from algorithm.test_sr9").show()