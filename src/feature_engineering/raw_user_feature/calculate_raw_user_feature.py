#!/usr/bin/env python3
# encoding: utf-8
import datetime

from digitforce.aip.common.utils.spark_helper import SparkClient
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql.functions import *

from digitforce.aip.common.utils.time_helper import *

def calculate_raw_user_feature(raw_user_feature_table_name):
    spark_client = SparkClient.get()
    user_table_columns = ['cust_code', 'age', 'sex', 'city_name', 'province_name', 'investor_type']
    order_table_columns = ["cust_code", "event_time", "event_code", "product_id", "product_amt"]

    # TODO：临时数据，重复、不更新，增加临时处理逻辑，后续删除
    spark_client.get_starrocks_table_df('zq_standard.dm_cust_label_base_attributes_df').select(["dt"]).createOrReplaceTempView("dm_cust_label_base_attributes_df")
    approximate_dt_sql = f'''
                        select
                              dt,
                              abs(DATEDIFF(dt,'{get_today_str()}')) as datediffs
                        from dm_cust_label_base_attributes_df
                        group by dt
                        order by datediffs asc
                        limit 1
                    '''
    approximate_dt_df = spark_client.get_session().sql(approximate_dt_sql).toPandas()
    approximate_dt = approximate_dt_df['dt'][0]

    standard_user_data = spark_client.get_starrocks_table_df('zq_standard.dm_cust_label_base_attributes_df')
    standard_user_data = standard_user_data.filter(standard_user_data["dt"] == approximate_dt).select(user_table_columns).dropDuplicates()

    standard_order_table = spark_client.get_starrocks_table_df("zq_standard.dm_cust_subs_redm_event_df")
    standard_order_data = standard_order_table.select(order_table_columns)

    # 3. 构造用户原始特征
    user_order_feature_dataframe = \
        calculate_raw_user_feature_from_order_feature(standard_order_data)

    # 4. 拼接特征，存入hive表
    raw_user_feature_dataframe = standard_user_data.join(user_order_feature_dataframe, "cust_code", "left")
    raw_user_feature_dataframe = raw_user_feature_dataframe.withColumnRenamed("cust_code", "user_id")

    raw_user_feature_dataframe.write.format("hive").mode("overwrite").saveAsTable(raw_user_feature_table_name)

    return raw_user_feature_table_name


def calculate_raw_user_feature_from_order_feature(standard_order_dataframe):
    # TODO: 构建不同时间段行为统计特征
    today = datetime.datetime.today()
    today_str = get_today_str()
    # TODO：数据原因，暂时取近一年构造特征
    thirty_days_ago_str = n_days_ago_str(365)
    # TODO：后续统一规范event_code
    buy_code = "申购"
    # 构建列名

    trade_type = "event_code"
    trade_date = "event_time"
    trade_money = "product_amt"

    user_id_dataframe = standard_order_dataframe.select("cust_code").distinct()

    user_buy_df = standard_order_dataframe.filter(standard_order_dataframe[trade_type] == buy_code)

    user_buy_counts_30d = user_buy_df.filter((user_buy_df[trade_date] >= thirty_days_ago_str)&(user_buy_df[trade_date] <= today_str)) \
        .groupby("cust_code") \
        .agg(F.count(trade_money).alias('u_buy_counts_30d'),
             F.sum(trade_money).alias('u_amount_sum_30d'),
             F.avg(trade_money).alias('u_amount_avg_30d'),
             F.min(trade_money).alias('u_amount_min_30d'),
             F.max(trade_money).alias('u_amount_max_30d'))

    user_buy_days_30d = user_buy_df.filter(user_buy_df[trade_date] >= thirty_days_ago_str) \
        .select(["cust_code", trade_date]) \
        .groupby(["cust_code"]) \
        .agg(countDistinct(trade_date),
             F.min(trade_date),
             F.max(trade_date)) \
        .rdd \
        .map(lambda x: (x[0], x[1], ((datetime.datetime.strptime(x[3], "%Y-%m-%d") - datetime.datetime.strptime(x[2], "%Y-%m-%d")).days / x[1]), (today - datetime.datetime.strptime(x[3], "%Y-%m-%d")).days)) \
        .toDF(["cust_code", "u_buy_days_30d", "u_buy_avg_days_30d", "u_last_buy_days_30d"])

    user_order_fund_list_dataframe = user_buy_df.select(["cust_code", "product_id", trade_date]) \
        .rdd \
        .sortBy(keyfunc=(lambda x: x[2]), ascending=False) \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: (x[0], "|".join(x[1][:5]))) \
        .toDF(["cust_code", "u_buy_list"])

    # 5. 拼接用户特征
    raw_user_feature_dataframe = user_id_dataframe.join(user_buy_counts_30d, "cust_code")
    raw_user_feature_dataframe = raw_user_feature_dataframe.join(user_buy_days_30d, "cust_code")
    raw_user_feature_dataframe = raw_user_feature_dataframe.join(user_order_fund_list_dataframe, "cust_code")

    return raw_user_feature_dataframe
