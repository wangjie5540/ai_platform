#!/usr/bin/env python3
# encoding: utf-8
'''
@file: test_save_hive.py
@time: 2023/4/4 10:11
@desc:
'''

from digitforce.aip.common.utils.spark_helper import SparkClient
from decimal import Decimal


def main():
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    test = spark.sparkContext.parallelize(
        [('23332911', 63, 20230405, Decimal('71390.75')),
         ('61028212388', 52, 20230405, Decimal('714550.75')),
         ('34105628', 40, 20230405, Decimal('71213190.75')),
         ('233213132923', 4, 20230405, Decimal('7213390.75')),
         ('6102328284', 2, 20230405, Decimal('721390.75')),
         ('34105625', 1, 20230405, Decimal('71390.75'))
         ]
    )
    test_df = test.toDF(['user_id', 'a', 'dt', 'b'])
    # test_df.write.format("orc").mode("overwrite").saveAsTable('algorithm.tmp_test_save_table')
    test_df.write.format("orc").mode("overwrite").insertInto('algorithm.tmp_test_save_table')
    # test_df.write.format("orc").mode("overwrite").partitionBy('dt').saveAsTable('algorithm.tmp_test_save_table')
    # test_df.write.format("hive").mode("overwrite").saveAsTable('algorithm.tmp_test_save_table')
    # test_df.write.format("hive").mode("overwrite").partitionBy('dt').saveAsTable('algorithm.tmp_test_save_table')
    # test_df.write.mode("overwrite").saveAsTable('algorithm.tmp_test_save_table')
    # test_df.write.mode("overwrite").partitionBy('dt').saveAsTable('algorithm.tmp_test_save_table')
    # test_df.write.mode("overwrite").insertInto('algorithm.tmp_test_save_table')


if __name__ == '__main__':
    main()
