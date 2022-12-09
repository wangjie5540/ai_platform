#!/usr/bin/env python3
# encoding: utf-8
'''
@file: sample_select.py
@time: 2022/12/7 17:47
@desc:
'''


import math
import numpy as np
import builtins
import random
import digitforce.aip.common.utils.spark_helper as spark_helper

def start_sample_selection(data_table_name, columns, event_code, pos_sample_proportion=0.5, pos_sample_num=200000):
    spark_client = spark_helper.SparkClient()
    buy_code = event_code.get("buy")
    user_id = columns[0]
    item_id = columns[1]
    trade_type = columns[2]

    data = spark_client.get_session().sql(f"select * from {data_table_name}")
    data = data.filter(data[trade_type] == buy_code)

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

    max_pos_counts = pos_rdd.map(lambda x: len(x[1])).reduce(lambda a, b: a if a>=b else b)
    item_list_counts = item_counts.count()
    max_top_n = np.min([5000, 0.5 * item_list_counts])
    min_top_n = np.max([1000, max_pos_counts * (pos_neg_relation + 1)])
    if max_top_n < min_top_n:
        top_n = max_top_n
    top_n = int((max_top_n + min_top_n) / 2)
    # top_n = 300000
    item_list = item_counts.map(lambda x: x[0]).take(top_n)

    # for i in range(10000):
    #     item_list.append(f"P{i}")
    # 负样本候选集
    neg_rdd = pos_rdd.map(lambda x: (x[0], random.sample(list(set(item_list).difference(set(x[1]))),
                                                       np.min([len(list(set(item_list).difference(set(x[1])))),math.ceil(len(x[1]) * pos_neg_relation)]))))
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
    sample_table_name = "algorithm.tmp_aip_sample"
    sample = sample.toDF(col)
    sample.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name, col

if __name__ == '__main__':
    pass