#!/usr/bin/env python3
# encoding: utf-8
from digitforce.aip.common.utils.spark_helper import SparkClient
import findspark
findspark.init()
from pyspark.sql import functions as F
from digitforce.aip.common.utils.time_helper import *


def calculate_raw_item_feature(output_table):
    spark_client = SparkClient.get()
    item_table_columns = ['product_id', 'product_type_pri']
    order_table_columns = ["cust_code", "event_time", "event_code", "product_id", "product_amt"]

    standard_item_dataframe = spark_client.get_starrocks_table_df('zq_standard.dm_cust_subs_redm_event_df')
    # TODO：后续优化物品表没有分区，去重为临时方案
    standard_item_dataframe = standard_item_dataframe.select(item_table_columns).distinct()

    standard_fund_trade_dataframe = spark_client.get_starrocks_table_df("zq_standard.dm_cust_subs_redm_event_df")
    standard_fund_trade_dataframe = standard_fund_trade_dataframe.select(order_table_columns)

    # 3. 构造用户、物品特征
    raw_feature_from_trade_dataframe = \
        calculate_raw_item_feature_from_order_table(standard_fund_trade_dataframe)

    # 4. 拼接特征，存入hive表
    item_feature_table = standard_item_dataframe.join(raw_feature_from_trade_dataframe, "product_id", "left")

    # TODO：动态hive表名
    item_feature_table = item_feature_table.withColumnRenamed("product_id", "item_id")
    # output_table = f'raw_item_feature_{id_helper.gen_uniq_id()}'
    item_feature_table.write.format("hive").mode("overwrite").saveAsTable(output_table)
    return output_table


def calculate_raw_item_feature_from_order_table(standard_fund_trade_dataframe):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today().date()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = n_days_ago_str(365)
    # TODO：后续统一规范event_code

    item_id = "product_id"
    trade_type = "event_code"
    trade_date = "event_time"
    trade_money = "product_amt"
    buy_code = "申购"

    item_id_dataframe = standard_fund_trade_dataframe.select(item_id).distinct()

    item_buy_counts_30d = standard_fund_trade_dataframe.filter(
        standard_fund_trade_dataframe[trade_date] >= thirty_days_ago_str) \
        .filter(standard_fund_trade_dataframe[trade_type] == buy_code) \
        .groupby(item_id) \
        .agg(F.count(trade_money).alias('i_buy_counts_30d'), \
             F.sum(trade_money).alias('i_amount_sum_30d'), \
             F.avg(trade_money).alias('i_amount_avg_30d'), \
             F.min(trade_money).alias('i_amount_min_30d'), \
             F.max(trade_money).alias('i_amount_max_30d'))
    raw_feature_from_trade_dataframe = item_id_dataframe.join(item_buy_counts_30d, item_id,
                                                              "left")

    return raw_feature_from_trade_dataframe
