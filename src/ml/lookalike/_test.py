# coding: utf-8
import digitforce.aip.common.utils.spark_helper as spark_helper

spark_client = spark_helper.SparkClient()
spark_client.get_starrocks_table_df("aip.item").show(10)
# spark_client.get_session().sql("show databases").show(5)