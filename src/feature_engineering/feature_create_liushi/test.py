#!/usr/bin/env python3
# encoding: utf-8
import datetime

from digitforce.aip.common.utils.spark_helper import spark_client
from decimal import Decimal
import utils

sample_table_name = "algorithm.aip_zq_liushi_custom_label"
active_before_days = 3
active_after_days = 5
feature_days=30
DATE_FORMAT = "%Y%m%d"

today = datetime.datetime.today().strftime(DATE_FORMAT)
window_test_days = 5
window_train_days = 30
now = datetime.datetime.now()
end_date = now - datetime.timedelta(days=active_after_days + 2)
mid_date = end_date - datetime.timedelta(days=window_test_days)
start_date = mid_date - datetime.timedelta(days=window_train_days)
end_date = end_date.strftime(DATE_FORMAT)
mid_date = mid_date.strftime(DATE_FORMAT)
start_date = start_date.strftime(DATE_FORMAT)
# 活跃度数据起始日期：基于start_date，过去n天，即 start_date - n
active_start_date = (datetime.datetime.strptime(start_date, '%Y%m%d') - datetime.timedelta(
    days=active_before_days)).strftime("%Y%m%d")
# 活跃度数据结束日期：基于end_date，未来m天，即，end_date + m
active_end_date = (
        datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=active_after_days)).strftime(
    "%Y%m%d")
# 特征数据最早日期：基于start_date，使用过去k天的数据，即，start_date - k
feature_date = (datetime.datetime.strptime(start_date, '%Y%m%d') - datetime.timedelta(days=feature_days)).strftime(
    "%Y%m%d")
print("The data source time range is from {} to {}".format(active_start_date, active_end_date))
spark_client.get_starrocks_table_df("algorithm.sample_zjls_zxr").createOrReplaceTempView("sample_zjls_zxr")
table_zj = spark_client.get_session().sql(
    "select cust_code, replace(dt,'-','') as dt, zc_money, zr_money, zc_cnt, zr_cnt from sample_zjls_zxr where replace(dt,'-','') between '{}' and '{}'".format(
        feature_date, end_date))
# 2.2 客户号 --> [(资金变动日期，资金转出金额，资金转入金额，资金转出笔数，资金转入笔数),(),()...]
zj_feature = table_zj.rdd.filter(lambda x: x[4] and (x[4] > 0 or x[5] > 0)). \
    map(lambda x: (x[0], [[int(x[1]), x[2], x[3], x[4], x[5]]])). \
    reduceByKey(lambda a, b: a + b). \
    map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=True)))  # 按日期降序排列

# 读取样本数据：客户号 --> (日期，lable)
custom_label = spark_client.get_session(). \
    sql("select custom_id, date, label from {} where dt = '{}'".format(sample_table_name, today)). \
    rdd.map(lambda x: (x[0], (x[1], x[2])))

# 3.2  最近一次资金变动距今天数，最近一次资金变动金额，最近3/7/15/30天的资金变动
merge_feature2 = custom_label.leftOuterJoin(zj_feature). \
    map(lambda x: (x[0], (x[1][0][0][0], x[1][0][0][1], x[1][0][1][0], x[1][0][1][1], x[1][0][1][2],
                          utils.get_zj_feature(int(x[1][0][0][0]), x[1][1])))). \
    map(lambda x: (x[0], (x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5][0], x[1][5][1], x[1][5][2])))

print(1)