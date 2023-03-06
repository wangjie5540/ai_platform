# encoding: utf-8

import datetime
from dateutil.relativedelta import relativedelta
from digitforce.aip.common.utils.spark_helper import SparkClient
import os
# os.environ['SPARK_HOME'] = '/opt/spark-2.4.8-bin-hadoop2.7'
import findspark
findspark.init()
# 需要固定位置的import 后添加 # NOQA: E402， 表示忽略模块级别导入不在文件顶部的错误
from pyspark.sql import window as W  # NOQA: E402
from pyspark.sql import functions as F  # NOQA: E402
from pyspark.sql import types as T  # NOQA: E402
from utils import *  # NOQA: E402


def start_sample_selection(
    dixiao_before_days: int,
    dixiao_after_days: int,
    right_zc_threshold=10000,
    avg_zc_threshold=10000,
    event_tag='login',
):
    """划分关键时间点，用户打标签，存hive

    Args:
        dixiao_before_days (int): dixiaohu before days
        dixiao_after_days (int): dixiaohu after days
        zc_threshold (int, optional): dixiaohu zc threshold. Defaults to 10000.

    Returns:
        _type_: _description_
    """
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    DATE_FORMAT = "%Y%m%d"
    zc_table = "zq_standard.dm_cust_ast_redm_event_df"
    app_table = "zq_standard.dm_cust_traf_behv_aggregate_df"
    jy_table = "zq_standard.dm_cust_subs_redm_event_aggregate_df"
    zc_view = zc_table[zc_table.find(".")+1:]
    app_view = app_table[app_table.find(".")+1:]
    jy_view = jy_table[jy_table.find(".")+1:]

    # TODO: 日期排除节假日因素

    # 1.获取关键时间点
    window_test_days = 3
    window_train_days = 5
    now = datetime.datetime.now()
    dixiao_end_date = now - datetime.timedelta(days=2)  # 低效户结束日期
    end_date = dixiao_end_date - \
        datetime.timedelta(days=dixiao_after_days)  # 低效户结束日期
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    dixiao_start_date = start_date - datetime.timedelta(
        days=dixiao_before_days
    )  # 低效户开始日期

    now = now.strftime(DATE_FORMAT)
    dixiao_end_date = dixiao_end_date.strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)
    dixiao_start_date = dixiao_start_date.strftime(DATE_FORMAT)

    # 2.给数据打标签
    spark_client.get_starrocks_table_df(
        zc_table).createOrReplaceTempView(zc_view)
    spark_client.get_starrocks_table_df(
        app_table).createOrReplaceTempView(app_view)
    spark_client.get_starrocks_table_df(
        jy_table).createOrReplaceTempView(jy_view)

    # 非交易日列表
    noexchangedate_list = noexchange_days(
        start_date=datetime.datetime.strptime(
            dixiao_start_date, '%Y%m%d').strftime('%Y-%m-%d'),
        end_date=datetime.datetime.strptime(
            dixiao_end_date, '%Y%m%d').strftime('%Y-%m-%d'),
    )
    print("11111111111111")
    # 资产数据
    zzc_sample = get_zc_sample(
        spark,
        zc='total_ast',
        zc_view=zc_view,
        dixiao_start_date=dixiao_start_date,
        dixiao_end_date=dixiao_end_date,
        start_date=start_date,
        end_date=end_date,
        dixiao_before_days=dixiao_before_days,
        dixiao_after_days=dixiao_after_days,
        noexchangedate_list=noexchangedate_list
    )
    # 检查数据是否为空
    if zzc_sample.isEmpty():
        _schema1 = T.StructType([
            T.StructField('cust_code', T.StringType(), True),
            T.StructField('right_zzc', T.FloatType(), True),
            T.StructField('past_avg_zzc', T.FloatType(), True),
            T.StructField('future_max_zzc', T.FloatType(), True),
            T.StructField('dt', T.StringType(), True)
        ])
        zc_sample_df = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), _schema1)
    else:
        zc_sample_df = zzc_sample.toDF(
            ["cust_code", "right_zzc", "past_avg_zzc", "future_max_zzc", "dt"])
    print("2222222")
    # 登录数据
    login_sample = (
        spark.sql(
            f"""
                select cust_code,is_login,replace(dt,'-','') as dt
                from {app_view} 
                where replace(dt,'-','') between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd
        .map(lambda x: (x[0], [(x[2], int(x[1]))]))
        .reduceByKey(lambda a, b: a+b)
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        # 得到当前日期，过去登陆天数
        .map(lambda x: (x[0], get_login_days(x[1], start_date, end_date, dixiao_before_days, noexchangedate_list)))
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[0], x[1][1], x[1][0]))
    )
    # 检查数据是否为空
    if login_sample.isEmpty():
        _schema1 = T.StructType([
            T.StructField('cust_code', T.StringType(), True),
            T.StructField('before_login_days', T.LongType(), True),
            T.StructField('dt', T.StringType(), True)
        ])
        login_sample_df = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), _schema1)
    else:
        login_sample_df = login_sample.toDF(
            ["cust_code", "before_login_days", "dt"])

    # 交易数据
    exchange_sample = (
        spark.sql(
            f"""
                select cust_code,total_tran_cnt,replace(dt,'-','') as dt
                from {jy_view} 
                where replace(dt,'-','') between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd
        .map(lambda x: (x[0], [(x[2], int(x[1]))]))
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        .reduceByKey(lambda a, b: a+b)
        # 得到当前日期，过去登陆天数
        .map(lambda x: (x[0], get_exchange_days(x[1], start_date, end_date, dixiao_before_days, noexchangedate_list)))
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[0], x[1][1], x[1][0]))
    )
    # 检查数据是否为空
    if exchange_sample.isEmpty():
        _schema1 = T.StructType([
            T.StructField('cust_code', T.StringType(), True),
            T.StructField('before_exchange_days', T.LongType(), True),
            T.StructField('dt', T.StringType(), True)
        ])
        exchange_sample_df = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), _schema1)
    else:
        exchange_sample_df = exchange_sample.toDF(
            ["cust_code", "before_exchange_days", "dt"])
    print("333333333333333")
    # 合并，按照条件过滤
    sample_df = (
        zc_sample_df
        .join(login_sample_df, on=['cust_code', 'dt'], how='left')
        .join(exchange_sample_df, on=['cust_code', 'dt'], how='left')
        .filter(
            F.expr(
                f"""
                right_zzc<{right_zc_threshold} and right_zzc>0 and past_avg_zzc<{avg_zc_threshold}
                """
            )
        )  # 过滤时点资产和平均资产
    )
    if event_tag == 'login':
        sample_df = sample_df.filter(F.expr(f"before_login_days>0"))
    elif event_tag == 'exchange':
        sample_df = sample_df.filter(F.expr(f"before_exchange_days>0"))

    sample_df = (
        sample_df.withColumn(
            "label",
            F.expr(f"IF(future_max_zzc>{right_zc_threshold},'1','0')")
        )
        .select("cust_code", "label", "dt")
    )
    print()
    # 3.write to hive
    # TODO:动态存表
    sample_table_name = "algorithm.aip_zq_dixiaohu_custom_label_standarddata"
    write_hive(spark, sample_df, sample_table_name, "dt")
    return sample_table_name


def write_hive(spark, inp_df, table_name, partition_col):
    check_table = (
        spark._jsparkSession.catalog().tableExists(table_name)
    )

    if check_table:  # 如果存在该表
        print("table:{} exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").insertInto(table_name)

    else:  # 如果不存在
        print("table:{} not exist......".format(table_name))
        inp_df.write.format("orc").mode("overwrite").partitionBy(
            partition_col
        ).saveAsTable(table_name)


def get_zc_sample(
    spark,
    zc: str,
    zc_view: str,
    dixiao_start_date: str,
    dixiao_end_date: str,
    start_date: str,
    end_date: str,
    dixiao_before_days: int,
    dixiao_after_days: int,
    noexchangedate_list: list
):

    sample_rdd = (
        spark.sql(
            f"""
                select cust_code,{zc},replace(dt,'-','') as dt
                from {zc_view} 
                where replace(dt,'-','') between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd
        .map(lambda x: (x[0], [(x[2], float(x[1]))]))
        .reduceByKey(lambda a, b: a+b)
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        # 得到当前日期，当前资产，过去平均资产，未来平均资产
        .map(lambda x: (x[0], get_zc_jf(x[1], start_date, end_date, dixiao_before_days, dixiao_after_days, noexchangedate_list)))
        .flatMapValues(lambda x: x)
        # 客户号，时点总资产，过去平均资产，未来最大资产，dt
        .map(lambda x: (x[0], x[1][1], x[1][2], x[1][3], x[1][0]))
    )
    return sample_rdd
