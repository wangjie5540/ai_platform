#!/usr/bin/env python3
# encoding: utf-8
from calculate_raw_user_feature import calculate_raw_user_feature
#
# # standard_order_table = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade")
# # test = standard_order_table.select(['DT']).take(1)
# # print("*********")

raw_user_feature_table_name = calculate_raw_user_feature("algorithm.tmp_test_raw_user_feature")
print(raw_user_feature_table_name)

