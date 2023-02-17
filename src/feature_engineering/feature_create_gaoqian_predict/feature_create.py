#!/usr/bin/env python3
# encoding: utf-8
'''
@file: feature_create.py
@time: 2022/12/7 18:54
@desc:
'''
import digitforce.aip.common.utils.spark_helper as spark_helper
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime
import random

DATE_FORMAT = "%Y-%m-%d"
spark_client = spark_helper.SparkClient()
today = datetime.datetime.today().strftime("%Y%m%d")

def feature_create(event_table_name, event_columns, item_table_name, item_columns, user_table_name, user_columns, event_code_list, category_a, sample_table_name):

    sample = spark_client.get_session().sql(f"select custom_id as user_id, 0 as label from {sample_table_name}")
    # 构建列名
    user_id = event_columns[0]

    col_sample = sample.columns
    user_id_sample = col_sample[0]

    user_id_user = user_columns[0]

    # 1. 构建sample用户id
    user_list = sample

    # 2. 构造用户特征
    user_order_feature_list = get_order_feature(event_table_name, event_columns, item_table_name, item_columns, sample, event_code_list, category_a)
    user_label_feature = get_user_feature(user_table_name, user_columns)

    # 3. 拼接特征，存入hive表
    user_feature_list = user_list.join(user_order_feature_list, user_list[user_id_sample] == user_order_feature_list[user_id], 'left').drop(user_id_sample)
    user_feature_list = user_feature_list.join(user_label_feature, user_feature_list[user_id] == user_label_feature[user_id_user], 'left').drop(user_id_user)
    user_feature_list = user_feature_list.withColumnRenamed(user_id, user_id_sample)
    print(user_feature_list.show(5))


    # TODO：动态hive表名
    user_feature_table_name = "algorithm.tmp_aip_user_feature_gaoqian_predict"
    user_feature_list.write.format("hive").mode("overwrite").saveAsTable(user_feature_table_name)

    return user_feature_table_name


def get_order_feature(event_table_name, event_columns, item_table_name, item_columns, sample, event_code, category_a):
    today = datetime.datetime.today().date()
    one_year_ago = (datetime.datetime.today() - datetime.timedelta(365)).strftime(DATE_FORMAT)
    thirty_days_ago_str = (datetime.datetime.today() + datetime.timedelta(days=-360)).strftime(DATE_FORMAT)

    user_id = event_columns[0]
    trade_type = event_columns[1]
    item_id = event_columns[2]
    trade_money = event_columns[3]
    trade_date = event_columns[-1]
    item_id_item = item_columns[0]
    fund_type = item_columns[1]

    event_data = spark_client.get_starrocks_table_df(event_table_name)
    event_data = event_data.select(event_columns) \
        .filter((event_data[trade_date] >= one_year_ago) & (event_data[trade_date] < today))

    item_data = spark_client.get_starrocks_table_df(item_table_name)
    item_data = item_data.select(item_columns).distinct()

    join_data = event_data.join(item_data.select([item_id_item, fund_type]),
                                event_data[item_id] == item_data[item_id_item])

    columns = [user_id, trade_type, trade_date, trade_money, fund_type]
    user_event_df = join_data.select(columns)

    col_sample = sample.columns
    user_id_sample = col_sample[0]

    user_list = sample.select(user_id_sample).distinct()

    user_event1_counts_30d = user_event_df.filter(user_event_df[trade_type] == event_code)\
        .filter(user_event_df[trade_date] >= thirty_days_ago_str) \
        .groupby(user_id) \
        .agg(F.count(trade_money).alias('u_buy_counts_30d'), \
             F.sum(trade_money).alias('u_amount_sum_30d'), \
             F.avg(trade_money).alias('u_amount_avg_30d'), \
             F.min(trade_money).alias('u_amount_min_30d'), \
             F.max(trade_money).alias('u_amount_max_30d'))

    user_event1_days_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
        .select([user_id, trade_date]) \
        .groupby([user_id]) \
        .agg(countDistinct(trade_date), \
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((datetime.datetime.strptime(x[3], DATE_FORMAT) - datetime.datetime.strptime(x[2], DATE_FORMAT)).days / x[1]), ((datetime.datetime.today() - datetime.datetime.strptime(x[3], DATE_FORMAT)).days))) \
        .toDF([user_id, "u_buy_days_30d", "u_buy_avg_days_30d", "u_last_buy_days_30d"])

    # todo: event的行为序列，用于关联规则挖掘
    # if len(event_code_list) == 2:
    #     user_event2_counts_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
    #         .groupby(user_id) \
    #         .agg(F.count(trade_money).alias('u_event2_counts_30d'), \
    #              F.sum(trade_money).alias('u_event2_amount_sum_30d'), \
    #              F.avg(trade_money).alias('u_event2_amount_avg_30d'), \
    #              F.min(trade_money).alias('u_event2_amount_min_30d'), \
    #              F.max(trade_money).alias('u_event2_amount_max_30d'))
    #
    #     user_event2_days_30d = user_event_df.filter(user_event_df[trade_date] >= thirty_days_ago_str) \
    #         .select([user_id, trade_date]) \
    #         .groupby([user_id]) \
    #         .agg(countDistinct(trade_date), \
    #              F.min(trade_date),
    #              F.max(trade_date)) \
    #         .rdd \
    #         .map(lambda x: (x[0], x[1], ((x[3] - x[2]).days / x[1]), ((today - x[3]).days))) \
    #         .toDF([user_id, "u_event2_days_30d", "u_event2_avg_days_30d", "u_last_event2_days_30d"])

    # todo: 分别统计两个品类相关特征

    # 用户行为涉及最多的top3个品类
    # user_event1_category_counts = user_event_df.filter(
    #     (user_event_df[trade_type] == event_code) & (user_event_df[trade_date] >= one_year_ago)) \
    #     .groupby([user_id, fund_type]) \
    #     .agg(F.count(user_id).alias('u_event1_counts'))
    # w = Window.partitionBy(user_event1_category_counts[user_id]).orderBy(user_event1_category_counts['u_event1_counts'].desc())
    # top = user_event1_category_counts.withColumn('rank', F.row_number().over(w)).where('rank<=3')
    # df1 = top.groupby(top[user_id]).agg(F.collect_list(top[fund_type]))
    #
    # def paddle_zero(x, index):
    #     if len(x) < 3:
    #         for j in range(3 - len(x)):
    #             x.append('0')
    #     return x[index]
    #
    # def paddle_zero_udf(index):
    #     return F.udf(lambda x: paddle_zero(x, index))
    #
    # top_list = ['u_buy_cat_top1', 'u_buy_cat_top2', 'u_buy_cat_top3']
    # for i in range(3):
    #     df1 = df1.withColumn(top_list[i], paddle_zero_udf(i)(df1[f'collect_list({fund_type})']))
    #
    # user_event1_top_cat = df1.select([user_id, 'u_buy_cat_top1', 'u_buy_cat_top2', 'u_buy_cat_top3'])

    # 拼接用户特征
    user_feature_list = user_list.join(user_event1_counts_30d,
                                       user_list[user_id_sample] == user_event1_counts_30d[user_id], 'left').drop(
        user_id_sample)
    user_feature_list = user_feature_list.join(user_event1_days_30d, user_id, 'left')
    # user_feature_list = user_feature_list.join(user_event1_top_cat, user_id, 'left')
    # if len(event_code_list) == 2:
    #     user_feature_list = user_feature_list.join(user_event2_counts_30d, user_id)
    #     user_feature_list = user_feature_list.join(user_event2_days_30d, user_id)
    return user_feature_list


def get_user_feature(user_table_name, user_columns):
    user_feature = spark_client.get_starrocks_table_df(user_table_name)
    user_label_feature = user_feature.select(user_columns).distinct().rdd

    dict_edu = genDict(user_label_feature.map(lambda x: x[2]))
    write_hdfs_dict(dict_edu, "edu", hdfs_client)

    dict_risk = genDict(user_label_feature.map(lambda x: x[3]))
    write_hdfs_dict(dict_risk, "risk", hdfs_client)

    dict_natn = genDict(user_label_feature.map(lambda x: x[4]))
    write_hdfs_dict(dict_natn, "natn", hdfs_client)

    dict_occu = genDict(user_label_feature.map(lambda x: x[5]))
    write_hdfs_dict(dict_occu, "occu", hdfs_client)

    user_feature_final = user_label_feature.map(lambda x: (x[0], x[1], dict_edu.get(x[2]), dict_risk.get(x[3]), dict_natn.get(x[4]), dict_occu.get(x[5]), x[6]))\
                        .toDF(user_columns)
    return user_feature_final

def genDict(inp):
    """
    inp: 输入一个rdd，单列
    return: 1. 将生成的dict存储hdfs，返回地址；2. 返回这个dict
    """
    sig_list = inp.distinct().collect()
    res_dict = dict()
    for index, term in enumerate(sig_list):
        res_dict[term] = index
    return res_dict

# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)

# dict先写本地，再写入hdfs
def write_hdfs_dict(content, file_name, hdfs_client):
    local_path = "dict.{}.{}".format(today, file_name)
    hdfs_path = "/user/ai/aip/zq/gaoqian/enum_dict/{}/{}".format(today, file_name)

    with open(local_path, "w") as f:
        for key in content:
            f.write("{}\t{}\n".format(key, content[key]))
    write_hdfs_path(local_path, hdfs_path, hdfs_client)

# todo: 编码load到本地使用