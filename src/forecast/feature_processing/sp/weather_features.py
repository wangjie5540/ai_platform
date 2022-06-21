# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
from common.common_helper import *
from pyspark.sql.functions import mean, max, min


def col_agg_last_days(sparkdf, col_key, col_weather, col_time, param):
    """过去天气的聚合-天粒度"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_last_{2}d".format(col_weather, agg, w),
                                         globals()[agg](psf.col(col_weather)).over(windowOpt))
    return sparkdf


def col_agg_future_days(sparkdf, col_key, col_weather, col_time, param):
    """未来天气的聚合-天粒度"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=Window.currentRow,
                                                                                        end=days(w - 1))
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_future_{2}d".format(col_weather, agg, w),
                                         globals()[agg](psf.col(col_weather)).over(windowOpt))
    return sparkdf


def col_ptp_last_days(sparkdf, col_key, col_weather, col_time, param):
    """过去最高与最低的差值-天粒度"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_ptp_last_{2}d".format(col_weather, agg, w),
                                         max(psf.col(col_weather)).over(windowOpt) - min(psf.col(col_weather)).over(
                                             windowOpt))
    return sparkdf


def col_ptp_future_days(sparkdf, col_key, col_weather, col_time, param):
    """未来最高与最低的差值-天粒度"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=Window.currentRow,
                                                                                        end=days(w - 1))
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_ptp_future_{2}d".format(col_weather, agg, w),
                                         max(psf.col(col_weather)).over(windowOpt) - min(psf.col(col_weather)).over(
                                             windowOpt))
    return sparkdf


def build_weather_daily_feature(param):
    spark = param['spark']
    col_key = param['col_key']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_weather_list = param['col_weather_list']
    dict_agg_func = eval(param['dict_agg_func'])
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)
    for col_weather in col_weather_list:
        for dict_key in dict_agg_func:
            sparkdf = globals()[dict_key](sparkdf, col_key, col_weather, col_time, dict_agg_func[dict_key])
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))
    save_table(sparkdf, output_table)
    return sparkdf


def build_weather_weekly_feature(param):
    spark = param['spark']
    col_key = param['col_key']
    sdate = param['sdate']
    edate = param['edate']
    col_weather_list = param['weather_list']
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)
    ff = lambda cond: psf.sum(psf.when(cond, 1).otherwise(0))
    cond_sunny = (sparkdf['weatherfrom'] == '晴') & (sparkdf['weatherto'] == '晴')
    sparkdf = sparkdf.groupby(col_key).agg(mean(col_weather_list[0]).alias("{0}_mean_w".format(col_weather_list[0])),
                                           max(col_weather_list[1]).alias("{0}_max_w".format(col_weather_list[1])),
                                           min(col_weather_list[2]).alias("{0}_min_w".format(col_weather_list[2])),
                                           ff(cond_sunny).alias("sunny_counts")
                                           )
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))
    save_table(sparkdf, output_table)
    return sparkdf


# sparkdf = spark.sql("""select *,to_unix_timestamp(recordtime,'yyyy-MM-dd') sdt,replace(recordtime,'-','') dt from ai_dm.lbs_weather_history_partition where recordtime>='2021-01-01' and recordtime<='2021-12-31'""")
# build_weather_daily_feature(sparkdf,{'col_agg_last_days':(['mean','max'],[2,3]),'col_ptp_future_days':(['mean','max'],[2,3])},['province','city','district'],['aqi','hightemperature'],'sdt','20210101','20211231').show()
# sparkdf = spark.sql(
#     """select *,to_unix_timestamp(recordtime,'yyyy-MM-dd') sdt,replace(recordtime,'-','') dt,year(recordtime) year,weekofyear(recordtime) week from ai_dm.lbs_weather_history_partition where recordtime>='2021-01-01' and recordtime<='2021-12-31'""")
# build_weather_weekly_feature(sparkdf, ['province', 'city', 'district', 'year', 'week'], '20210101', '20211231').show()