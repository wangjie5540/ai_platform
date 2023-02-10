#!/usr/bin/env python3
# encoding: utf-8
from digitforce.aip.common.utils.spark_helper import spark_client
from pyspark.sql import functions as F

from digitforce.aip.common.utils.time_helper import *


def calculate_raw_item_feature(output_table):
    item_table_columns = ['ts_code', 'fund_type', 'management', 'custodian', 'invest_type']
    order_table_columns = ["custom_id", "trade_date", "trade_type", "fund_code", "trade_money", "fund_shares",
                           "fund_nav"]

    standard_item_dataframe = spark_client.get_starrocks_table_df('algorithm.zq_fund_basic')
    # TODO：后续优化物品表没有分区，去重为临时方案
    standard_item_dataframe = standard_item_dataframe.select(item_table_columns).distinct()
    standard_item_dataframe = standard_item_dataframe.withColumnRenamed("ts_code", "fund_code")

    standard_fund_trade_dataframe = spark_client.get_starrocks_table_df("algorithm.zq_fund_trade_lite")
    standard_fund_trade_dataframe = standard_fund_trade_dataframe.select(order_table_columns)

    # 3. 构造用户、物品特征
    raw_feature_from_trade_dataframe = \
        calculate_raw_item_feature_from_order_table(standard_fund_trade_dataframe)

    # 4. 拼接特征，存入hive表
    item_feature_table = standard_item_dataframe.join(raw_feature_from_trade_dataframe, "fund_code", "left")

    # TODO：动态hive表名
    item_feature_table = item_feature_table.withColumnRenamed("fund_code", "item_id")
    # output_table = f'raw_item_feature_{id_helper.gen_uniq_id()}'
    item_feature_table.write.format("hive").mode("overwrite").saveAsTable(output_table)
    return output_table


def calculate_raw_item_feature_from_order_table(standard_fund_trade_dataframe):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today().date()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = n_days_ago_str(365)
    # TODO：后续统一规范event_code

    item_id = "fund_code"
    trade_type = "trade_type"
    trade_date = "trade_date"
    trade_money = "trade_money"

    item_id_dataframe = standard_fund_trade_dataframe.select("fund_code").distinct()

    item_buy_counts_30d = standard_fund_trade_dataframe.filter(
        standard_fund_trade_dataframe[trade_date] >= thirty_days_ago_str) \
        .groupby(item_id) \
        .agg(F.count(trade_money).alias('i_buy_counts_30d'), \
             F.sum(trade_money).alias('i_amount_sum_30d'), \
             F.avg(trade_money).alias('i_amount_avg_30d'), \
             F.min(trade_money).alias('i_amount_min_30d'), \
             F.max(trade_money).alias('i_amount_max_30d'))
    raw_feature_from_trade_dataframe = item_id_dataframe.join(item_buy_counts_30d, "fund_code",
                                                              "left")

    return raw_feature_from_trade_dataframe
