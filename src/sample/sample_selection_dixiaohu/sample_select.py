import findspark
findspark.init()
import datetime
from digitforce.aip.common.utils.spark_helper import SparkClient

from pyspark.sql import functions as F
from pyspark.sql import types as T
from utils import noexchange_days, get_login_days, get_exchange_days, get_zc_jf
from digitforce.aip.common.utils.time_helper import DATE_FORMAT


def start_sample_selection(
        dixiao_before_days: int,
        dixiao_after_days: int,
        right_zc_threshold=10000,
        avg_zc_threshold=10000,
        event_tag="login",
):
    """
    划分关键时间点，用户打标签，存hive

    Args:
        dixiao_before_days: 低效户前置时间
        dixiao_after_days: 低效户后置时间
        right_zc_threshold: 时点资产阈值
        avg_zc_threshold: 前置时间平均资产阈值
        event_tag: 事件标签

    Returns:

    """
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    zc_table = "zq_standard.dm_cust_ast_redm_event_df"
    app_table = "zq_standard.dm_cust_traf_behv_aggregate_df"
    jy_table = "zq_standard.dm_cust_subs_redm_event_aggregate_df"
    zc_view = zc_table[zc_table.find(".") + 1:]
    app_view = app_table[app_table.find(".") + 1:]
    jy_view = jy_table[jy_table.find(".") + 1:]

    # 1.获取关键时间点
    window_test_days = 3
    window_train_days = 5
    now = datetime.datetime.now()
    dixiao_end_date = now - datetime.timedelta(days=2)  # 低效户结束日期
    end_date = dixiao_end_date - datetime.timedelta(days=dixiao_after_days)  # 低效户结束日期
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
    spark_client.get_starrocks_table_df(zc_table).createOrReplaceTempView(zc_view)
    spark_client.get_starrocks_table_df(app_table).createOrReplaceTempView(app_view)
    spark_client.get_starrocks_table_df(jy_table).createOrReplaceTempView(jy_view)

    # 非交易日列表
    noexchangedate_list = noexchange_days(
        start_date=dixiao_start_date,
        end_date=dixiao_end_date,
    )
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime(
        "%Y%m%d"
    )  # 修改日期表达字符串
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime(
        "%Y%m%d"
    )  # 修改日期表达字符串
    # 资产数据
    zzc_sample = get_zc_sample(
        spark,
        zc="total_ast",
        zc_view=zc_view,
        dixiao_start_date=dixiao_start_date,
        dixiao_end_date=dixiao_end_date,
        start_date=start_date,
        end_date=end_date,
        dixiao_before_days=dixiao_before_days,
        dixiao_after_days=dixiao_after_days,
        noexchangedate_list=noexchangedate_list,
    )
    # 检查数据是否为空
    if zzc_sample.isEmpty():
        _schema1 = T.StructType(
            [
                T.StructField("cust_code", T.StringType(), True),
                T.StructField("right_zzc", T.FloatType(), True),
                T.StructField("past_avg_zzc", T.FloatType(), True),
                T.StructField("future_max_zzc", T.FloatType(), True),
                T.StructField("dt", T.StringType(), True),
            ]
        )
        zc_sample_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), _schema1)
    else:
        zc_sample_df = zzc_sample.toDF(
            ["cust_code", "right_zzc", "past_avg_zzc", "future_max_zzc", "dt"]
        )
    print("资产数据规模", len(zc_sample_df.toPandas()))
    print("资产数据", zc_sample_df.toPandas())

    # 登录数据
    login_sample = (
        spark.sql(
            f"""
                select cust_code,is_login,replace(dt,'-','') as dt
                from {app_view} 
                where dt between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd.map(lambda x: (x[0], [(x[2], int(x[1]))]))
        .reduceByKey(lambda a, b: a + b)
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        # 得到当前日期，过去登陆天数
        .map(
            lambda x: (
                x[0],
                get_login_days(
                    x[1], start_date, end_date, dixiao_before_days, noexchangedate_list
                ),
            )
        )
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[0], x[1][1], x[1][0]))
    )
    # 检查数据是否为空
    if login_sample.isEmpty():
        _schema1 = T.StructType(
            [
                T.StructField("cust_code", T.StringType(), True),
                T.StructField("before_login_days", T.LongType(), True),
                T.StructField("dt", T.StringType(), True),
            ]
        )
        login_sample_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), _schema1)
    else:
        login_sample_df = login_sample.toDF(["cust_code", "before_login_days", "dt"])
    print("登录数据规模", len(login_sample_df.toPandas()))

    # 交易数据
    exchange_sample = (
        spark.sql(
            f"""
                select cust_code,total_tran_cnt,replace(dt,'-','') as dt
                from {jy_view} 
                where dt between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd.map(lambda x: (x[0], [(x[2], int(x[1]))]))
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        .reduceByKey(lambda a, b: a + b)
        # 得到当前日期，过去登陆天数
        .map(
            lambda x: (
                x[0],
                get_exchange_days(
                    x[1], start_date, end_date, dixiao_before_days, noexchangedate_list
                ),
            )
        )
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[0], x[1][1], x[1][0]))
    )
    # 检查数据是否为空
    if exchange_sample.isEmpty():
        _schema1 = T.StructType(
            [
                T.StructField("cust_code", T.StringType(), True),
                T.StructField("before_exchange_days", T.LongType(), True),
                T.StructField("dt", T.StringType(), True),
            ]
        )
        exchange_sample_df = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), _schema1
        )
    else:
        exchange_sample_df = exchange_sample.toDF(
            ["cust_code", "before_exchange_days", "dt"]
        )
    print("交易数据规模", len(exchange_sample_df.toPandas()))

    print("data generated, start joining")
    # 合并，按照条件过滤
    sample_df = (
        zc_sample_df.join(login_sample_df, on=["cust_code", "dt"], how="left")
        .join(exchange_sample_df, on=["cust_code", "dt"], how="left")
        .filter(
            F.expr(
                f"""
                right_zzc<{right_zc_threshold} and right_zzc>0 and past_avg_zzc<{avg_zc_threshold}
                """
            )
        )  # 过滤时点资产和平均资产
    )

    if event_tag == "login":
        sample_df = sample_df.filter(F.expr(f"before_login_days>0"))
    elif event_tag == "exchange":
        sample_df = sample_df.filter(F.expr(f"before_exchange_days>0"))

    sample_df = (
        sample_df.withColumn(
            "label", F.expr(f"IF(future_max_zzc>{right_zc_threshold},'1','0')")
        )
        .select("cust_code", "label", "dt")
        .withColumn(
            "dt",
            F.from_unixtime(
                F.unix_timestamp(timestamp=F.col("dt"), format="yyyymmdd"),
                "yyyy-mm-dd",
            ),
        )
    )
    print("标签表规模", len(sample_df.toPandas()))
    print("标签表数据", sample_df.toPandas())
    # 3.write to hive
    sample_table_name = "algorithm.aip_zq_dixiaohu_custom_label"
    sample_df.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name


def write_hive(spark, inp_df, table_name, partition_col):
    check_table = spark._jsparkSession.catalog().tableExists(table_name)

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
        noexchangedate_list: list,
):
    sample_rdd = (
        spark.sql(
            f"""
                select cust_code,{zc},replace(dt,'-','') as dt
                from {zc_view} 
                where dt between '{dixiao_start_date}' and '{dixiao_end_date}'
                """
        )
        .rdd.map(lambda x: (x[0], [(x[2], float(x[1]))]))
        .reduceByKey(lambda a, b: a + b)
        # 每个x[0] 下面按照日期排序
        .map(lambda x: (x[0], sorted(x[1], key=lambda y: int(y[0]), reverse=False)))
        # 得到当前日期，当前资产，过去平均资产，未来平均资产
        .map(
            lambda x: (
                x[0],
                get_zc_jf(
                    x[1],
                    start_date,
                    end_date,
                    dixiao_before_days,
                    dixiao_after_days,
                    noexchangedate_list,
                ),
            )
        )
        .flatMapValues(lambda x: x)
        # 客户号，时点总资产，过去平均资产，未来最大资产，dt
        .map(lambda x: (x[0], x[1][1], x[1][2], x[1][3], x[1][0]))
    )
    return sample_rdd
