#!/usr/bin/env python3
# encoding: utf-8

# from digitforce.aip.common.utils.spark_helper import spark_client
from digitforce.aip.common.utils import cos_helper
#
# value_test = [('10001', '[1,1,0,1,0,1,1,0]'),
#               ('10002', '[1,1,1,0,1,1,0,1]'),
#               ('10003', '[1,1,0,1,0,0,0,1]'),
#               ('10004', '[1,2,0,0,5,6,7,1]'),
#               ('10005', '[1,2,3,4,0,0,7,1]'),
#               ('10006', '[1,0,3,1,5,1,7,8]'),
#               ('10007', '[1,2,1,4,5,0,7,1]'),
#               ('10008', '[1,2,3,1,5,6,1,8]'),
#               ('10009', '[1,2,3,1,1,0,7,8]'),
#               ('10010', '[1,2,3,4,5,6,1,1]')]
# # print(value_test)
# user_vec = spark_client.get_session().createDataFrame(value_test, ['user_id', 'user_vec'])
# # user_vec.write.format("hive").mode("overwrite").saveAsTable("algorithm.aip_zq_lookalike_user_vec")
#
# value_test = [('10001',),
#               ('10002',),
#               ('10003',),
#               ('10004',)]
# seeds_crowd = spark_client.get_session().createDataFrame(value_test, ['user_id'])
# # seeds_crowd.write.format("hive").mode("overwrite").saveAsTable("algorithm.aip_zq_lookalike_seeds_crowd")
#
# value_test = [('10005',),
#               ('10006',),
#               ('10007',),
#               ('10008',),
#               ('10009',),
#               ('10010',)]
# predict_crowd = spark_client.get_session().createDataFrame(value_test, ['user_id'])
# predict_crowd.write.format("hive").mode("overwrite").saveAsTable("algorithm.aip_zq_lookalike_predict_crowd")
# columns = ["custom_id", "trade_date", "trade_type", "fund_code"]
# buy_code = "fund_buy"
# dt = '2022-04-11'
# user_id = columns[0]
# trade_date = columns[1]
# trade_type = columns[2]
# item_id = columns[3]
# data = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade")
# data = data.select(columns) \
#     .filter((data[trade_date] == dt)) \
#     .filter(data[trade_type] == buy_code)
# print(data.count())
# df = spark_client.get_starrocks_table_df("algorithm.user_info")
#
# print(df.count())

url_seed = cos_helper.upload_file("seeds.csv", "aip_test_lookalike_seeds.csv")
print(url_seed)
# https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_seeds.csv
url_predict = cos_helper.upload_file("predict.csv", "aip_test_lookalike_predict.csv")
print(url_predict)
# https://algorithm-1308011215.cos.ap-beijing.myqcloud.com/aip_test_lookalike_predict.csv