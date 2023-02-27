#!/usr/bin/env python3
# encoding: utf-8

import datetime
import random
from digitforce.aip.common.utils.spark_helper import SparkClient
from digitforce.aip.common.utils.time_helper import get_today_str
import utils

DATE_FORMAT = "%Y%m%d"
spark_client = SparkClient.get()

def start_sample_selection(active_before_days, active_after_days,
                           active_days_threshold,
                           label_count=300000):
    window_test_days = 5
    window_train_days = 30
    now = datetime.datetime.now()
    end_date = now - datetime.timedelta(days=active_after_days + 2)
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)
    today = get_today_str(DATE_FORMAT)

    # 活跃度数据起始日期：基于start_date，过去n天，即 start_date - n
    active_start_date = (datetime.datetime.strptime(start_date, '%Y%m%d') - datetime.timedelta(
        days=active_before_days)).strftime("%Y%m%d")
    # active_start_date = get_date_n_days_ago_str(get_day_datetime(start_date, DATE_FORMAT), active_before_days,
    #                                             DATE_FORMAT)

    # 活跃度数据结束日期：基于end_date，未来m天，即，end_date + m
    active_end_date = (
            datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=active_after_days)).strftime(
        "%Y%m%d")
    # active_end_date = get_date_n_days_ago_str(get_day_datetime(end_date, DATE_FORMAT), -active_after_days, DATE_FORMAT)
    print("The data source time range is from {} to {}".format(active_start_date, active_end_date))

    # 客户号，日期，客户是否登录
    spark_client.get_starrocks_table_df("zq_standard.dm_cust_traf_behv_aggregate_df").createOrReplaceTempView("dm_cust_traf_behv_aggregate_df")
    table_app = spark_client.get_session().sql(
        """select cust_code, replace(dt,'-','') as dt, is_login from dm_cust_traf_behv_aggregate_df where replace(dt,'-','') between '{}' and '{}'""".format(
            active_start_date, active_end_date))
    # print("table_app")
    # print(table_app.count())
    # 1. 样本生成
    # 1.1 客户号 --> 活跃日期:set(str)
    user_days = table_app.rdd.filter(lambda x: x[2] and x[2] > 0).map(lambda x: (x[0], {x[1]})). \
        reduceByKey(lambda a, b: a | b)
    # print("user_days")
    # print(user_days.count())
    # 1.2 客户号 --> (日期，过去n天活跃的天数，未来m天活跃的天数)
    user_active_days = user_days. \
        map(lambda x: (x[0], utils.getActiveDays(x[1], start_date, end_date, active_before_days, active_after_days))). \
        flatMapValues(lambda x: x)
    # print("user_active_days")
    # print(user_active_days.count())
    # 1.3 全量：客户号 --> (日期，label)

    # - 过去n天至少活跃【active_days_threshold】次:
    #  -- 未来m天连续不活跃: label=1
    #  -- 未来m天至少活跃一次: label=0
    custom_label_all = user_active_days.filter(lambda x: x[1][1] >= active_days_threshold). \
        map(lambda x: (x[0], (x[1][0], 1)) if x[1][1] == 0 else (x[0], (x[1][0], 0)))
    # print("custom_label_all")
    # print(custom_label_all.count())
    # 1.4 采样
    # 总数据量
    all_cnt = custom_label_all.count()

    # 采样率
    cy_rate = label_count * 1.0 / all_cnt
    # 采样后的客户：客户号, 日期，label, 分区
    custom_label = custom_label_all.map(lambda x: (x[0], (x[1], random.random()))). \
        filter(lambda x: x[1][1] < cy_rate). \
        map(lambda x: (x[0], x[1][0][0], x[1][0][1], today))
    # print("custom_label")
    # print(custom_label.count())
    sample_table_name = "algorithm.aip_zq_liushi_custom_label"
    custom_label_df = custom_label.toDF(['custom_id', 'date', "label", "dt"])
    write_hive(custom_label_df, sample_table_name, "dt")
    return sample_table_name


def write_hive(inp_df, table_name, partition_col):
    check_table = spark_client.get_session()._jsparkSession.catalog().tableExists(table_name)

    if check_table:  # 如果存在该表
        print("table:{} exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").insertInto(table_name)

    else:  # 如果不存在
        print("table:{} not exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").partitionBy(partition_col).saveAsTable(table_name)
