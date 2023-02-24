#!/usr/bin/env python3
# encoding: utf-8


import math
import numpy as np
import builtins
import random
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.time_helper as time_helper


def start_sample_selection(event_code_buy, pos_sample_proportion=0.5, pos_sample_num=200000):
    spark_client = SparkClient.get()
    columns = ["cust_code", "event_time", "event_code", "product_id"]
    buy_code = event_code_buy
    user_id = columns[0]
    trade_date = columns[1]
    trade_type = columns[2]
    item_id = columns[3]
    # TODO：数据取较大范围
    today = time_helper.get_today_str()
    thirty_days_ago = time_helper.n_days_ago_str(365)

    data = spark_client.get_starrocks_table_df("zq_standard.dm_cust_subs_redm_event_df")
    data = data.select(columns) \
        .filter((data[trade_date] >= thirty_days_ago) & (data[trade_date] <= today)) \
        .filter(data[trade_type] == buy_code)
    # 确定采样总条数
    if data.count() < pos_sample_num:
        pos_sample_num = data.count()
    neg_sample_num = int(pos_sample_num / pos_sample_proportion * (1 - pos_sample_proportion))
    round = getattr(builtins, "round")
    pos_neg_relation = round((1 - pos_sample_proportion) / pos_sample_proportion, 2)

    # 正样本全量
    pos_rdd = data.select([user_id, item_id]).rdd \
        .map(lambda x: (x[0], [x[1]])) \
        .reduceByKey(lambda a, b: a + b)

    # 负样本全量
    item_counts = data.select([user_id, item_id]).rdd \
        .map(lambda x: (x[1], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(keyfunc=(lambda x: x[1]), ascending=False)

    max_pos_counts = pos_rdd.map(lambda x: len(x[1])).reduce(lambda a, b: a if a >= b else b)
    item_list_counts = item_counts.count()
    # TODO：合理选取item集合范围
    max_top_n = np.min([5000, int(0.5 * item_list_counts)])
    min_top_n = np.max([1000, int(max_pos_counts * (pos_neg_relation + 1))])
    if max_top_n < min_top_n:
        top_n = max_top_n
    else:
        top_n = int((max_top_n + min_top_n) / 2)
    item_list = item_counts.map(lambda x: x[0]).take(top_n)

    # 负样本候选集
    neg_rdd = pos_rdd.map(lambda x: (x[0], random.sample(list(set(item_list).difference(set(x[1]))),
                                                         np.min([len(list(set(item_list).difference(set(x[1])))),
                                                                 math.ceil(len(x[1]) * pos_neg_relation)]))))
    # 随机采样构成正负样本集合
    pos_rdd = pos_rdd.flatMapValues(lambda x: x) \
        .map(lambda x: (x[0], x[1], random.random()))
    pos_threshold = pos_sample_num / pos_rdd.count()
    pos_rdd = pos_rdd.filter(lambda x: x[2] <= pos_threshold).map(lambda x: (x[0], x[1], 1))

    neg_rdd = neg_rdd.flatMapValues(lambda x: x) \
        .map(lambda x: (x[0], x[1], random.random()))
    neg_threshold = neg_sample_num / neg_rdd.count()
    neg_rdd = neg_rdd.filter(lambda x: x[2] <= neg_threshold).map(lambda x: (x[0], x[1], 0))
    sample = pos_rdd.union(neg_rdd)
    col = ['user_id', 'item_id', 'label']
    # TODO：后续改为动态表名
    sample_table_name = "algorithm.tmp_aip_sample"
    sample = sample.toDF(col)
    sample.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name, col
