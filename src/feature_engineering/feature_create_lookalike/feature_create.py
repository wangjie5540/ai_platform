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

def feature_create(data_table_name, columns, event_code, sample_table_name):
    spark_client = spark_helper.SparkClient()
    data = spark_client.get_session().sql(f"select * from {data_table_name}")
    sample = spark_client.get_session().sql(f"select * from {sample_table_name}")
    # 构建列名
    col_data = data.columns
    user_id = col_data[0]
    item_id = col_data[3]

    col_sample = sample.columns
    user_id_sample = col_sample[0]
    item_id_sample = col_sample[1]

    # 1. 构建用户子集
    # 2. 构建商品子集
    user_list = sample.select(user_id_sample).distinct()
    item_list = sample.select(item_id_sample).distinct()

    # 3. 构造用户、物品特征
    user_order_feature_list, item_order_feature_list = get_order_feature(data, sample, event_code, col_data, col_sample)
    user_label_feature = get_user_feature(data, sample, col_data, col_sample)
    item_label_feature = get_item_feature(data, sample, col_data, col_sample)

    # 4. 拼接特征，存入hive表
    user_feature_list = user_list.join(user_order_feature_list, user_list[user_id_sample] == user_order_feature_list[user_id], "left").drop(
        user_id)
    user_feature_list = user_feature_list.join(user_label_feature, user_feature_list[user_id_sample] == user_label_feature[user_id], "left").drop(
        user_id)

    item_feature_list = item_list.join(item_order_feature_list, item_list[item_id_sample] == item_order_feature_list[item_id],"left").drop(
        item_id)
    item_feature_list = item_feature_list.join(item_label_feature, item_feature_list[item_id_sample] == item_label_feature[item_id], "left").drop(
        item_id)

    user_feature_table_name = "algorithm.tmp_aip_user_feature"
    user_feature_list.write.format("hive").mode("overwrite").saveAsTable(user_feature_table_name)

    item_feature_table_name = "algorithm.tmp_aip_item_feature"
    item_feature_list.write.format("hive").mode("overwrite").saveAsTable(item_feature_table_name)

    return user_feature_table_name, item_feature_table_name


def get_order_feature(data, sample, event_code, col_data, col_sample):
    # todo: 60d、90d
    today = datetime.datetime.today().date()
    # str_30d_ago = n_days_ago_str(30)
    # str_60d_ago = n_days_ago_str(60)
    str_30d_ago = (datetime.datetime.today() + datetime.timedelta(days=-360)).strftime(DATE_FORMAT)
    buy_code = event_code.get("buy")
    # 构建列名
    col_trade = [c for c in col_data if not c.startswith("u_") and not c.startswith("i_")]
    user_id = col_data[0]
    item_id = col_data[3]
    trade_type = col_data[2]
    trade_date = col_data[1]
    trade_money = col_data[4]
    user_id_sample = col_sample[0]
    item_id_sample = col_sample[1]

    user_list = sample.select(user_id_sample).distinct()

    user_df = data.join(user_list, data[user_id] == user_list[user_id_sample], "right"). \
        drop(user_id_sample)

    user_buy_df = user_df.select(col_trade).filter(user_df[trade_type] == buy_code)

    user_buy_counts_7d = user_buy_df.filter(user_buy_df[trade_date] >= str_30d_ago) \
        .groupby(user_id) \
        .agg(F.count(trade_money).alias('u_buy_counts_7d'), \
             F.sum(trade_money).alias('u_amount_sum_7d'), \
             F.avg(trade_money).alias('u_amount_avg_7d'), \
             F.min(trade_money).alias('u_amount_min_7d'), \
             F.max(trade_money).alias('u_amount_max_7d'))

    user_buy_days_7d = user_buy_df.filter(user_buy_df[trade_date] >= str_30d_ago) \
        .select([user_id, trade_date]) \
        .groupby([user_id]) \
        .agg(countDistinct(trade_date), \
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), ((today - x[3]).days))) \
        .toDF([user_id, "u_buy_days_7d", "u_buy_avg_days_7d", "u_last_buy_days_7d"])

    user_buy_list = user_buy_df.select([user_id, item_id, trade_date]) \
        .rdd \
        .sortBy(keyfunc=(lambda x: x[2]), ascending=False) \
        .map(lambda x: (x[0], [x[1]])) \
        .reduceByKey(lambda a, b: a + b if len(a) <= 5 else a) \
        .toDF([user_id, "u_buy_list"])

    # 5. 拼接用户特征
    user_feature_list = user_list.join(user_buy_counts_7d, user_list[user_id_sample] == user_buy_counts_7d[user_id],
                               "left").drop(user_id)
    user_feature_list = user_feature_list.join(user_buy_days_7d, user_feature_list[user_id_sample] == user_buy_days_7d[user_id], "left").drop(
        user_id)
    user_feature_list = user_feature_list.join(user_buy_list, user_feature_list[user_id_sample] == user_buy_list[user_id], "left").drop(
        user_id_sample)

    item_list = sample.select(item_id_sample).distinct()
    item_df = data.join(item_list, data[item_id] == item_list[item_id_sample], "right"). \
        drop(item_id_sample)
    item_buy_df = item_df.select(col_trade).filter(item_df[trade_type] == buy_code)

    item_buy_counts_7d = item_buy_df.filter(item_buy_df[trade_date] >= str_30d_ago) \
        .groupby(item_id) \
        .agg(F.count(trade_money).alias('i_buy_counts_7d'), \
             F.sum(trade_money).alias('i_amount_sum_7d'), \
             F.avg(trade_money).alias('i_amount_avg_7d'), \
             F.min(trade_money).alias('i_amount_min_7d'), \
             F.max(trade_money).alias('i_amount_max_7d'))
    item_feature_list = item_list.join(item_buy_counts_7d, item_list[item_id_sample] == item_buy_counts_7d[item_id],
                                       "left").drop(item_id_sample)

    return user_feature_list, item_feature_list

def get_traffic_feature():
    pass

def get_user_feature(data, sample, col_data, col_sample):
    user_id = col_data[0]
    user_id_sample = col_sample[0]
    user_list = sample.select(user_id_sample).distinct()
    user_df = data.join(user_list, data[user_id] == user_list[user_id_sample], "right"). \
        drop(user_id_sample)
    user_feature_col = [c for c in col_data if c.startswith("u_")]
    user_label_feature = user_df.select([user_id] + user_feature_col).distinct()
    return user_label_feature

def get_item_feature(data, sample, col_data, col_sample):
    item_id = col_data[3]
    item_id_sample = col_sample[1]
    item_list = sample.select(item_id_sample).distinct()
    item_df = data.join(item_list, data[item_id] == item_list[item_id_sample], "right"). \
        drop(item_id_sample)
    item_feature_col = [c for c in col_data if c.startswith("i_")]
    item_label_feature = item_df.select([item_id] + item_feature_col).distinct()
    return item_label_feature


if __name__ == '__main__':
    pass