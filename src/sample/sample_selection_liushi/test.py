# #!/usr/bin/env python3
# # encoding: utf-8
# from digitforce.aip.common.utils.spark_helper import spark_client
#
# user_feature = spark_client.get_session().sparkContext.parallelize(
# [('23332997', "20230105", "20230112"),
#  ('61028288', "20230105", "20230112"),
#  ('34105628', "20230105", "20230112"),
#  ('55027282', "20230105", "20230112"),
#  ('18509159', "20230105", "20230112")]
# )
#
#
# ##############写表
# def write_hive(inp_df, table_name, partition_col):
#     check_table = spark_client.get_session()._jsparkSession.catalog().tableExists(table_name)
#
#     if check_table:  # 如果存在该表
#         print("table:{} exist......".format(table_name))
#         inp_df.write.format("orc").mode("overwrite").insertInto(table_name)
#
#     else:  # 如果不存在
#         print("table:{} not exist......".format(table_name))
#         inp_df.write.format("orc").mode("overwrite").partitionBy(partition_col).saveAsTable(table_name)
# user_feature = user_feature.toDF(['user_id','date','dt'])
# write_hive(user_feature, "algorithm.aip_zq_liushi_custom_test", "dt")
from digitforce.aip.common.utils.spark_helper import spark_client
sql = """(select * from algorithm.user_info limit 10) t"""
print(sql)
table = "algorithm.user_info"
df = spark_client.get_starrocks_table_df(sql)
print(df.count())