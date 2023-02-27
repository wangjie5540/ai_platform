import datetime
import random

import digitforce.aip.common.utils.spark_helper as spark_helper
import digitforce.aip.common.utils.time_helper as time_helper
from utils import getActiveDays

DATE_FORMAT = "%Y-%m-%d"
spark_client = spark_helper.SparkClient()

def sample_create(trade_table_name, trade_columns, event_table_name, event_columns, event_code, category, train_period, predict_period):
    window_test_days = 1
    window_train_days = 3
    now = datetime.datetime.now()
    end_date = now - datetime.timedelta(days=predict_period+2)
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)
    # today = get_today_str(DATE_FORMAT)
    # today = time_helper.get_today_str()

    # 数据起始日期：基于start_data, 过去n天， start_date - n
    active_start_date = (datetime.datetime.strptime(start_date, DATE_FORMAT) - datetime.timedelta(days=train_period)).strftime(DATE_FORMAT)
    # 数据戒指日期；基于end_date, 未来m天， end_date + m
    buy_end_date = (datetime.datetime.strptime(end_date, DATE_FORMAT) + datetime.timedelta(days=predict_period)).strftime(DATE_FORMAT)

    event_data = spark_client.get_starrocks_table_df(event_table_name)
    event_data = event_data.select(event_columns)\
        .filter((event_data['dt'] >= active_start_date) & (event_data['dt'] <= buy_end_date))

    trade_data = spark_client.get_starrocks_table_df(trade_table_name)
    trade_data = trade_data.select(trade_columns)\
        .filter((trade_data['dt'] >= active_start_date) & (trade_data['dt'] <= buy_end_date))

    #1. 样本生成
    ##1.1 客户号 -> 活跃日期：set(str)
    user_active_days = event_data.rdd.filter(lambda x: x[1] and x[1] > 0).map(lambda x: (x[0], {x[2]}))\
        .reduceByKey(lambda a, b: a | b)
    ## 1.2 客户号 -> 申购日期：set(str)
    user_buy_days = trade_data.rdd.filter(lambda x: x[1] == event_code and x[2] == category).map(lambda x: (x[0], {x[3]}))\
        .reduceByKey(lambda a, b: a | b)

    ## 1.3 客户号->(日期，过去n天活跃天数，未来m天申购天数）
    user_days = user_active_days.leftOuterJoin(user_buy_days)\
        .map(lambda x: (x[0], getActiveDays(x[1][0], x[1][1], start_date, end_date, train_period, predict_period)) if x[1][1] else (x[0], getActiveDays(x[1][0], set({}), start_date, end_date, train_period, predict_period)))\
        .flatMapValues(lambda x: x)

    ## 1.4 客户号 -> (日期， label)
    ##-过去n天至少活跃一次：
    ## --未来m天未买入该类产品：label=0
    ## --未来m天买入该类产品：label=1
    sample_all = user_days.filter(lambda x: x[1][1] >= 1)\
        .map(lambda x: (x[0], (x[1][0], 0)) if x[1][1] == 0 else (x[0], (x[1][0], 1)))

    ## 1.5 采样
    all_cnt = sample_all.count()
    label_cnt = 100000
    sample_rate = label_cnt * 1.0 / all_cnt
    # 最终结果：客户号， 日期， label
    sample_columns = ['custom_id', 'date', 'label']
    sample = sample_all.map(lambda x: (x[0], (x[1], random.random())))\
        .filter(lambda x: x[1][1] < sample_rate)\
        .map(lambda x: (x[0], x[1][0][0], x[1][0][1]))\
        .toDF(sample_columns)

    # todo: dynamic change table name
    sample_table_name = 'algorithm.tmp_aip_sample_gaoqian'
    print(sample.show(20))
    sample.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name, sample_columns


