#!/usr/bin/env python3
# encoding: utf-8
'''
@file: feature_create.py
@time: 2022/12/7 18:54
@desc:
'''
import digitforce.aip.common.utils.spark_helper as spark_helper
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

DATE_FORMAT = "%Y-%m-%d"


def feature_create(data_table_name, event_code_list, category_a, sample_table_name):
    spark_client = spark_helper.SparkClient()
    data = spark_client.get_session().sql(f"select * from {data_table_name}")
    sample = spark_client.get_session().sql(f"select * from {sample_table_name}")
    # 构建列名
    col_data = data.columns
    user_id = col_data[0]

    col_sample = sample.columns
    user_id_sample = col_sample[0]


    # 1. 构建sample用户id
    user_list = sample.select(user_id_sample).distinct()

    # 2. 构造用户特征
    user_order_feature_list, item_order_feature_list = get_order_feature(data, sample, event_code_list, category_a, col_data, col_sample)
    user_label_feature = get_user_feature(data, col_data)

    # 3. 拼接特征，存入hive表
    user_feature_list = user_list.join(user_order_feature_list, user_id)
    user_feature_list = user_feature_list.join(user_label_feature, user_id)
    user_feature_list = user_feature_list.withColumnRenamed(user_id, user_id_sample)

    # TODO：动态hive表名
    user_feature_table_name = "algorithm.tmp_aip_user_feature_gaoqian"
    user_feature_list.write.format("hive").mode("overwrite").saveAsTable(user_feature_table_name)

    return user_feature_table_name


def get_order_feature(data, sample, event_code_list, category_a, col_data, col_sample):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today().date()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = (datetime.datetime.today() + datetime.timedelta(days=-360)).strftime(DATE_FORMAT)
    # TODO：后续统一规范event_code

    # 构建列名
    col_trade = [c for c in col_data if not c.startswith("u_") and not c.startswith("i_")]
    col_trade.append('i_fund_type')
    user_id = col_data[0]
    trade_type = col_data[2]
    trade_date = col_data[1]
    trade_money = col_data[4]
    fund_type = col_data[15]
    user_id_sample = col_sample[0]

    user_list = sample.select(user_id_sample).distinct()

    if len(event_code_list) == 1:
        user_event_df = data.select(col_trade).filter(data[trade_type] == event_code_list[0])
    else:
        user_event_df = data.select(col_trade).filter((data[trade_type] == event_code_list[0]) & (data[trade_type] == event_code_list[1]))

    user_event1_counts_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
        .groupby(user_id) \
        .agg(F.count(trade_money).alias('u_event1_counts_30d'), \
             F.sum(trade_money).alias('u_event1_amount_sum_30d'), \
             F.avg(trade_money).alias('u_event1_amount_avg_30d'), \
             F.min(trade_money).alias('u_event1_amount_min_30d'), \
             F.max(trade_money).alias('u_event1_amount_max_30d'))

    user_event1_days_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
        .select([user_id, trade_date]) \
        .groupby([user_id]) \
        .agg(countDistinct(trade_date), \
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), ((today - x[3]).days))) \
        .toDF([user_id, "u_event1_days_30d", "u_event1_avg_days_30d", "u_last_event1_days_30d"])

    # todo: event的行为序列，用于关联规则挖掘
    if len(event_code_list) == 2:
        user_event2_counts_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
            .groupby(user_id) \
            .agg(F.count(trade_money).alias('u_event2_counts_30d'), \
                 F.sum(trade_money).alias('u_event2_amount_sum_30d'), \
                 F.avg(trade_money).alias('u_event2_amount_avg_30d'), \
                 F.min(trade_money).alias('u_event2_amount_min_30d'), \
                 F.max(trade_money).alias('u_event2_amount_max_30d'))

        user_event2_days_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
            .select([user_id, trade_date]) \
            .groupby([user_id]) \
            .agg(countDistinct(trade_date), \
                 F.min(trade_date),
                 F.max(trade_date)) \
            .rdd \
            .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), ((today - x[3]).days))) \
            .toDF([user_id, "u_event2_days_30d", "u_event2_avg_days_30d", "u_last_event2_days_30d"])

    # todo: 分别统计两个品类相关特征

    # 拼接用户特征
    user_feature_list = user_list.join(user_event1_counts_30d, user_id)
    user_feature_list = user_feature_list.join(user_event1_days_30d, user_id)
    if len(event_code_list) == 2:
        user_feature_list = user_feature_list.join(user_event2_counts_30d, user_id)
        user_feature_list = user_feature_list.join(user_event2_days_30d, user_id)

    return user_feature_list


def get_user_feature(data, col_data):
    user_id = col_data[0]
    user_feature_col = [c for c in col_data if c.startswith("u_")]
    user_label_feature = data.select([user_id] + user_feature_col).distinct()
    return user_label_feature

