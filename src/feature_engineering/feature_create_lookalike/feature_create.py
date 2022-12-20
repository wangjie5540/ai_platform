#!/usr/bin/env python3
# encoding: utf-8
import digitforce.aip.common.utils.spark_helper as spark_helper
import digitforce.aip.common.utils.time_helper as time_helper
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

DATE_FORMAT = "%Y-%m-%d"


def feature_create(event_code, sample_table_name, sample_columns):
    spark_client = spark_helper.SparkClient()
    user_table_columns = ['cust_id', 'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST']
    item_table_columns = ['ts_code', 'fund_type', 'management', 'custodian', 'invest_type']
    order_table_columns = ["custom_id", "trade_date", "trade_type", "fund_code", "trade_money", "fund_shares",
                           "fund_nav"]

    # 构建列名
    user_id = user_table_columns[0]
    item_id = item_table_columns[0]
    order_user_id = order_table_columns[0]
    order_item_id = order_table_columns[3]
    sample_user_id = sample_columns[0]
    sample_item_id = sample_columns[1]

    user_data = spark_client.get_starrocks_table_df('algorithm.user_info')
    user_data = user_data.select(user_table_columns)

    item_data = spark_client.get_starrocks_table_df('algorithm.zq_fund_basic')
    # TODO：后续优化物品表没有分区，去重为临时方案
    item_data = item_data.select(item_table_columns).distinct()

    sample_data = spark_client.get_session().sql(f"""select {",".join(sample_columns)} from {sample_table_name}""")

    order_table = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade")
    order_data = order_table.select(order_table_columns)

    # 3. 构造用户、物品特征
    user_order_feature_list, item_order_feature_list = get_order_feature(order_data, sample_data, event_code, order_table_columns,
                                                                         sample_columns)


    # 4. 拼接特征，存入hive表
    user_data = user_data.withColumnRenamed(user_id, sample_user_id)
    user_order_feature_list = user_order_feature_list.withColumnRenamed(order_user_id, sample_user_id)
    user_feature_table = user_data.join(user_order_feature_list, sample_user_id, "left")

    item_data = item_data.withColumnRenamed(item_id, sample_item_id)
    item_order_feature_list = item_order_feature_list.withColumnRenamed(order_item_id, sample_item_id)
    item_feature_table = item_data.join(item_order_feature_list, sample_item_id, "left")

    user_feature_table_columns = user_feature_table.columns
    item_feature_table_columns = item_feature_table.columns
    # TODO：动态hive表名
    user_feature_table_name = "algorithm.tmp_aip_user_feature"
    user_feature_table.write.format("hive").mode("overwrite").saveAsTable(user_feature_table_name)

    # TODO：动态hive表名
    item_feature_table_name = "algorithm.tmp_aip_item_feature"
    item_feature_table.write.format("hive").mode("overwrite").saveAsTable(item_feature_table_name)

    return user_feature_table_name, user_feature_table_columns,\
           item_feature_table_name, item_feature_table_columns


def get_order_feature(data, sample, event_code, col_data, col_sample):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today().date()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = (datetime.datetime.today() + datetime.timedelta(days=-360)).strftime(DATE_FORMAT)
    # TODO：后续统一规范event_code
    buy_code = event_code.get("buy")
    # 构建列名
    user_id = col_data[0]
    item_id = col_data[3]
    trade_type = col_data[2]
    trade_date = col_data[1]
    trade_money = col_data[4]
    item_id_sample = col_sample[1]

    user_list = data.select(user_id).distinct()

    user_buy_df = data.select(col_data).filter(data[trade_type] == buy_code)

    user_buy_counts_30d = user_buy_df.filter(user_buy_df[trade_date] >= thirty_days_ago_str) \
        .groupby(user_id) \
        .agg(F.count(trade_money).alias('u_buy_counts_30d'), \
             F.sum(trade_money).alias('u_amount_sum_30d'), \
             F.avg(trade_money).alias('u_amount_avg_30d'), \
             F.min(trade_money).alias('u_amount_min_30d'), \
             F.max(trade_money).alias('u_amount_max_30d'))

    user_buy_days_30d = user_buy_df.filter(user_buy_df[trade_date] >= thirty_days_ago_str) \
        .select([user_id, trade_date]) \
        .groupby([user_id]) \
        .agg(countDistinct(trade_date), \
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), ((today - x[3]).days))) \
        .toDF([user_id, "u_buy_days_30d", "u_buy_avg_days_30d", "u_last_buy_days_30d"])

    user_buy_list = user_buy_df.select([user_id, item_id, trade_date]) \
        .rdd \
        .sortBy(keyfunc=(lambda x: x[2]), ascending=False) \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: (x[0], "|".join(x[1][:5]))) \
        .toDF([user_id, "u_buy_list"])

    # 5. 拼接用户特征
    user_feature_list = user_list.join(user_buy_counts_30d, user_id)
    user_feature_list = user_feature_list.join(user_buy_days_30d, user_id)
    user_feature_list = user_feature_list.join(user_buy_list, user_id)

    item_list = sample.select(item_id_sample).distinct()
    item_df = data.join(item_list, data[item_id] == item_list[item_id_sample], "right"). \
        drop(item_id_sample)
    item_buy_df = item_df.select(col_data).filter(item_df[trade_type] == buy_code)

    item_buy_counts_30d = item_buy_df.filter(item_buy_df[trade_date] >= thirty_days_ago_str) \
        .groupby(item_id) \
        .agg(F.count(trade_money).alias('i_buy_counts_30d'), \
             F.sum(trade_money).alias('i_amount_sum_30d'), \
             F.avg(trade_money).alias('i_amount_avg_30d'), \
             F.min(trade_money).alias('i_amount_min_30d'), \
             F.max(trade_money).alias('i_amount_max_30d'))
    item_feature_list = item_list.join(item_buy_counts_30d, item_list[item_id_sample] == item_buy_counts_30d[item_id],
                                       "left").drop(item_id_sample)

    return user_feature_list, item_feature_list


