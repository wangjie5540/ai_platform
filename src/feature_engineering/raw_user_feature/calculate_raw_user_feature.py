#!/usr/bin/env python3
# encoding: utf-8
import datetime

from digitforce.aip.common.utils.spark_helper import spark_client
from pyspark.sql import functions as F
from pyspark.sql.functions import *

from digitforce.aip.common.utils.time_helper import *


def calculate_raw_user_feature(raw_user_feature_table_name):
    user_table_columns = ['cust_id', 'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST']
    order_table_columns = ["custom_id", "trade_date", "trade_type", "fund_code", "trade_money", "fund_shares",
                           "fund_nav"]

    # todo starrocks支持列存储  可以将select移至starrocks中 降低网络io
    standard_user_data = spark_client.get_starrocks_table_df('algorithm.user_info')
    standard_user_data = standard_user_data.select(user_table_columns)
    standard_user_data = standard_user_data.withColumnRenamed("cust_id", "custom_id")

    standard_order_table = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade")
    standard_order_data = standard_order_table.select(order_table_columns)

    # 3. 构造用户原始特征
    user_order_feature_dataframe = \
        calculate_raw_user_feature_from_order_feature(standard_order_data)

    # 4. 拼接特征，存入hive表
    raw_user_feature_dataframe = standard_user_data.join(user_order_feature_dataframe, "custom_id", "left")
    raw_user_feature_dataframe = raw_user_feature_dataframe.withColumnRenamed("custom_id", "user_id")

    raw_user_feature_dataframe.write.format("hive").mode("overwrite").saveAsTable(raw_user_feature_table_name)

    return raw_user_feature_table_name


def calculate_raw_user_feature_from_order_feature(standard_order_dataframe):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today().date()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = n_days_ago_str(120)
    # TODO：后续统一规范event_code
    buy_code = "fund_buy"
    # 构建列名

    trade_type = "trade_type"
    trade_date = "trade_date"
    trade_money = "trade_money"

    user_id_dataframe = standard_order_dataframe.select("custom_id").distinct()

    user_buy_df = standard_order_dataframe.filter(standard_order_dataframe[trade_type] == buy_code)

    user_buy_counts_30d = user_buy_df.filter(user_buy_df[trade_date] >= thirty_days_ago_str) \
        .groupby("custom_id") \
        .agg(F.count(trade_money).alias('u_buy_counts_30d'),
             F.sum(trade_money).alias('u_amount_sum_30d'),
             F.avg(trade_money).alias('u_amount_avg_30d'),
             F.min(trade_money).alias('u_amount_min_30d'),
             F.max(trade_money).alias('u_amount_max_30d'))

    user_buy_days_30d = user_buy_df.filter(user_buy_df[trade_date] >= thirty_days_ago_str) \
        .select(["custom_id", trade_date]) \
        .groupby(["custom_id"]) \
        .agg(countDistinct(trade_date),
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), (today - x[3]).days)) \
        .toDF(["custom_id", "u_buy_days_30d", "u_buy_avg_days_30d", "u_last_buy_days_30d"])

    user_order_fund_list_dataframe = user_buy_df.select(["custom_id", "fund_code", trade_date]) \
        .rdd \
        .sortBy(keyfunc=(lambda x: x[2]), ascending=False) \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: (x[0], "|".join(x[1][:5]))) \
        .toDF(["custom_id", "u_buy_list"])

    # 5. 拼接用户特征
    raw_user_feature_dataframe = user_id_dataframe.join(user_buy_counts_30d, "custom_id")
    raw_user_feature_dataframe = raw_user_feature_dataframe.join(user_buy_days_30d, "custom_id")
    raw_user_feature_dataframe = raw_user_feature_dataframe.join(user_order_fund_list_dataframe, "custom_id")

    return raw_user_feature_dataframe
