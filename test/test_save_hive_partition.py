#!/usr/bin/env python3
# encoding: utf-8
'''
@file: test_save_hive.py
@time: 2023/4/4 10:11
@desc:
'''


# from digitforce.aip.common.utils.spark_helper import SparkClient
# from decimal import Decimal
# spark_client = SparkClient.get()
# spark = spark_client.get_session()
# test = spark.sparkContext.parallelize(
# [('23332997', 63, 20230404, Decimal('71390.75')),
#  ('61028288', 52, 20230404, Decimal('71390.75')),
#  ('34105628', 40, 20230404, Decimal('71390.75')),
#  ('23332923', 4, 20230404, Decimal('71390.75')),
#  ('61028284', 2, 20230404, Decimal('71390.75')),
#  ('34105625', 0, 20230404, Decimal('71390.75'))
#  ]
# )
# test_df = test.toDF(['user_id','a','dt','b'])
# test_df.write.format("orc").mode("overwrite").saveAsTable('algorithm.tmp_test_save_table')
# # test_df.write.format("orc").mode("overwrite").partitionBy('dt').saveAsTable('algorithm.tmp_test_save_table')