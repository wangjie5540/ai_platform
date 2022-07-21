# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
from forecast.common.reference_package import *
from digitforce.aip.common.data_helper import *
from digitforce.aip.common.spark_helper import *


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
                                         psf.max(psf.col(col_weather)).over(windowOpt) - psf.min(psf.col(col_weather)).over(
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
                                         psf.max(psf.col(col_weather)).over(windowOpt) - psf.min(psf.col(col_weather)).over(
                                             windowOpt))
    return sparkdf


def build_weather_daily_feature(spark, param):
    col_key = param['col_key']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_weather_list = param['col_weather_list']
    dict_agg_func = eval(param['dict_agg_func'])
    input_site_table = param['input_site_table']
    input_weather_table = param['input_weather_table']
    output_table = param['weather_feature_daily_table']
    shops = param['shop_list']
    weather_data_sql = param['weather_data_sql']
    site_data_sql = param['site_data_sql']
    col_origin_name = param['col_origin_name']
    sparkdf_weather = forecast_spark_helper.read_origin_table(input_weather_table, weather_data_sql, col_origin_name, shops)
    sparkdf_site = forecast_spark_helper.read_origin_table(input_site_table,site_data_sql, col_origin_name, shops)
    for col_weather in col_weather_list:
        for dict_key in dict_agg_func:
            sparkdf_weather = globals()[dict_key](sparkdf_weather, col_key, col_weather, col_time,
                                                  dict_agg_func[dict_key])
    sparkdf_weather = sparkdf_weather.filter(date_filter_condition(sdate, edate))
    sparkdf_weather = sparkdf_weather.join(sparkdf_site, on=col_key, how='inner')
    forecast_spark_helper.save_table(spark, sparkdf_weather, output_table)
    return 'SUCCESS'


def build_weather_weekly_feature(param):
    col_key = param['col_key']
    join_key = param['join_key']
    sdate = param['sdate']
    edate = param['edate']
    col_weather_list = param['weather_list']
    input_weather_table = param['input_weather_table']
    input_site_table = param['input_site_table']
    output_table = param['weather_feature_weekly_table']
    shops = param['shop_list']
    weather_data_sql = param['weather_data_sql']
    site_data_sql = param['site_data_sql']
    col_origin_name = param['col_origin_name']
    sparkdf_weather = forecast_spark_helper.read_origin_table(input_weather_table, weather_data_sql, col_origin_name, shops)
    sparkdf_site = forecast_spark_helper.read_origin_table(input_site_table, site_data_sql, col_origin_name, shops)
    ff = lambda cond: psf.sum(psf.when(cond, 1).otherwise(0))
    cond_sunny = (sparkdf_weather['weatherfrom'] == '晴') & (sparkdf_weather['weatherto'] == '晴')
    sparkdf_weather = sparkdf_weather.groupby(col_key).agg(
        psf.mean(col_weather_list[0]).alias("{0}_mean_w".format(col_weather_list[0])),
        psf.max(col_weather_list[1]).alias("{0}_max_w".format(col_weather_list[1])),
        psf.min(col_weather_list[2]).alias("{0}_min_w".format(col_weather_list[2])),
        ff(cond_sunny).alias("sunny_counts")
        )

    sparkdf_weather = sparkdf_weather.withColumnRenamed("week_dt", "dt")
    sparkdf_weather = sparkdf_weather.filter(date_filter_condition(sdate, edate))
    sparkdf_weather = sparkdf_weather.join(sparkdf_site, on=join_key, how='inner')

    forecast_spark_helper.save_table(sparkdf_weather, output_table)
    return 'SUCCESS'


def build_weather_monthly_feature(param):
    col_key = param['col_key']
    join_key = param['join_key']
    sdate = param['sdate']
    edate = param['edate']
    col_weather_list = param['weather_list']
    input_weather_table = param['input_weather_table']
    input_site_table = param['input_site_table']
    output_table = param['weather_feature_monthly_table']
    weatherfrom = param['weatherfrom']
    weatherto = param['weatherto']
    weatherfrom_value = param['weatherfrom_value']
    weatherto_value = param['weatherto_value']
    shops = param['shop_list']
    weather_data_sql = param['weather_data_sql']
    site_data_sql = param['site_data_sql']
    col_origin_name = param['col_origin_name']
    sparkdf_weather = forecast_spark_helper.read_origin_table(input_weather_table,weather_data_sql, col_origin_name, shops)
    sparkdf_site = forecast_spark_helper.read_origin_table(input_site_table, site_data_sql, col_origin_name, shops)
    ff = lambda cond: psf.sum(psf.when(cond, 1).otherwise(0))
    cond_weather = (sparkdf_weather[weatherfrom] == weatherfrom_value) & (sparkdf_weather[weatherto] == weatherto_value)
    sparkdf_weather = sparkdf_weather.groupby(col_key).agg(
        psf.mean(col_weather_list[0]).alias("{0}_mean_w".format(col_weather_list[0])),
        psf.max(col_weather_list[1]).alias("{0}_max_w".format(col_weather_list[1])),
        psf.min(col_weather_list[2]).alias("{0}_min_w".format(col_weather_list[2])),
        ff(cond_weather).alias("weather_counts")
    )

    sparkdf_weather = sparkdf_weather.withColumnRenamed("month_dt", "dt")
    sparkdf_weather = sparkdf_weather.filter(date_filter_condition(sdate, edate))
    sparkdf_weather = sparkdf_weather.join(sparkdf_site, on=join_key, how='inner')

    forecast_spark_helper.save_table(sparkdf_weather, output_table)
    return 'SUCCESS'

# sparkdf = spark.sql("""select *,to_unix_timestamp(recordtime,'yyyy-MM-dd') sdt,replace(recordtime,'-','') dt from ai_dm.lbs_weather_history_partition where recordtime>='2021-01-01' and recordtime<='2021-12-31'""")
# build_weather_daily_feature(sparkdf,{'col_agg_last_days':(['mean','max'],[2,3]),'col_ptp_future_days':(['mean','max'],[2,3])},['province','city','district'],['aqi','hightemperature'],'sdt','20210101','20211231').show()
# sparkdf = spark.sql(
#     """select *,to_unix_timestamp(recordtime,'yyyy-MM-dd') sdt,replace(recordtime,'-','') dt,year(recordtime) year,weekofyear(recordtime) week from ai_dm.lbs_weather_history_partition where recordtime>='2021-01-01' and recordtime<='2021-12-31'""")
# build_weather_weekly_feature(sparkdf, ['province', 'city', 'district', 'year', 'week'], '20210101', '20211231').show()