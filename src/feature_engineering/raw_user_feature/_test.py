#!/usr/bin/env python3
# encoding: utf-8
from calculate_raw_user_feature import calculate_raw_user_feature

# #
# # # standard_order_table = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade")
# # # test = standard_order_table.select(['DT']).take(1)
# # # print("*********")
#
raw_user_feature_table_name = calculate_raw_user_feature("algorithm.tmp_test_raw_user_feature")
print(raw_user_feature_table_name)
# table_name = "algorithm.tmp_test_raw_user_feature"
# from digitforce.aip.common.utils.spark_helper import SparkClient
#
# spark_client = SparkClient.get()
# # spark_client.get_session().sql(f"DROP TABLE IF EXISTS {table_name}")
# from digitforce.aip.common.utils.time_helper import *
#
# user_table_name = "zq_standard.dm_cust_label_base_attributes_df"
# spark_client.get_starrocks_table_df('zq_standard.dm_cust_label_base_attributes_df').createOrReplaceTempView("dm_cust_label_base_attributes_df")
# approximate_dt_sql = f'''
#                     select
#                           dt,
#                           abs(DATEDIFF(dt,'{get_today_str()}')) as datediffs
#                     from dm_cust_label_base_attributes_df
#                     group by dt
#                     order by datediffs asc
#                     limit 1
#                 '''
# approximate_dt_df = spark_client.get_session().sql(approximate_dt_sql).toPandas()
# approximate_dt = approximate_dt_df['dt'][0]
# print(approximate_dt)
