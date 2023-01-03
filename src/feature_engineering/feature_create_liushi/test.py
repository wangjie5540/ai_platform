#!/usr/bin/env python3
# encoding: utf-8
from digitforce.aip.common.utils.spark_helper import spark_client
from decimal import Decimal


def write_hive(inp_df, table_name, partition_col):
    check_table = spark_client.get_session()._jsparkSession.catalog().tableExists(table_name)

    if check_table:  # 如果存在该表
        print("table:{} exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").insertInto(table_name)

    else:  # 如果不存在
        print("table:{} not exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").partitionBy(partition_col).saveAsTable(table_name)

today = "20230103"
user_feature = spark_client.get_session().sparkContext.parallelize(
[('23332997', (63, 0, Decimal('71390.75'))),
 ('61028288', (52, 1, Decimal('71390.75'))),
 ('34105628', (40, 0, Decimal('71390.75'))),
 ('55027282', (36, 1, Decimal('71390.75'))),
 ('18509159', (41, 0, Decimal('71390.75')))]
)
columns = ['age', 'label', 'count', 'dt']
user_feature_df = user_feature.map(lambda x: list(x[1])+[today]).toDF(columns)
write_hive(user_feature_df, "algorithm.aip_zq_liushi_custom_feature_train2", 'dt')