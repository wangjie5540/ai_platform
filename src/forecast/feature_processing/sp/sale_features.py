# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
# from pyspark.sql import Window
# from pyspark.sql.functions import udf
# import pyspark.sql.functions as psf
# import pandas as pd
# import numpy as np
# from pyspark.sql.types import DoubleType
from forecast.common.common_helper import *

from pyspark.sql.functions import sum, count, lit,mean


def zero_turn_nan(col_value):
    if not pd.isna(col_value) and col_value > 0:
        return col_value
    else:
        return None


def weekends_turn_nan(col_value, dayofweek):
    if dayofweek == 1 or dayofweek == 7:
        return None
    else:
        return col_value


def weekdays_turn_nan(col_value, dayofweek):
    if dayofweek != 1 and dayofweek != 7:
        return None
    else:
        return col_value


def col_agg_days(sparkdf, col_key, col_qty, col_time, param):
    """过去N天col_qty的agg"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_{2}d".format(col_qty, agg, w),
                                         globals()[agg](psf.col(col_qty)).over(windowOpt))
    return sparkdf


def col_gtz_agg_days(sparkdf, col_key, col_qty, col_time, param):
    """ 过去N天有销量agg"""
    aggs = param[0]
    ws = param[1]
    zero_turn_nan_udf = udf(zero_turn_nan, DoubleType())
    sparkdf = sparkdf.withColumn("{0}_copy".format(col_qty), zero_turn_nan_udf(sparkdf[col_qty]))
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_{2}_{3}d".format(col_qty, 'gtz', agg, w),
                                         globals()[agg](psf.col("{0}_copy".format(col_qty))).over(windowOpt))
    sparkdf = sparkdf.drop(*["{0}_copy".format(col_qty)])
    return sparkdf


def col_gtz_count_days(sparkdf, col_key, col_qty, col_time, param):
    """ 过去N天有销售天数***变量方法待修改***"""
    #     aggs = param[0]
    ws = param[1]
    zero_turn_nan_udf = udf(zero_turn_nan, DoubleType())
    sparkdf = sparkdf.withColumn("{0}_copy".format(col_qty), zero_turn_nan_udf(sparkdf[col_qty]))
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                        end=Window.currentRow)
        sparkdf = sparkdf.withColumn("{0}_{1}_{2}_{3}d".format(col_qty, 'gtz', 'count', w),
                                     count(psf.col("{0}_copy".format(col_qty))).over(windowOpt))
    sparkdf = sparkdf.drop(*["{0}_copy".format(col_qty)])
    return sparkdf


def col_agg_weekdays(sparkdf, col_key, col_qty, col_time, param):
    """ 过去N周工作日销量"""
    aggs = param[0]
    ws = param[1]
    weekends_turn_nan_udf = udf(weekends_turn_nan, DoubleType())
    sparkdf = sparkdf.withColumn("{0}_copy".format(col_qty),
                                 weekends_turn_nan_udf(sparkdf[col_qty], sparkdf["dayofweek"]))
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w * 7 - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_{2}_{3}".format(col_qty, agg, 'weekdays', w),
                                         globals()[agg](psf.col("{0}_copy".format(col_qty))).over(windowOpt))
    return sparkdf


def col_agg_weekend(sparkdf, col_key, col_qty, col_time, param):
    """ 过去N周周末销量"""
    aggs = param[0]
    ws = param[1]
    weekdays_turn_nan_udf = udf(weekdays_turn_nan, DoubleType())
    sparkdf = sparkdf.withColumn("{0}_copy".format(col_qty),
                                 weekdays_turn_nan_udf(sparkdf[col_qty], sparkdf["dayofweek"]))
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w * 7 - 1),
                                                                                        end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_{2}_{3}".format(col_qty, agg, 'weekend', w),
                                         globals()[agg](psf.col("{0}_copy".format(col_qty))).over(windowOpt))
    return sparkdf


def col_hb_days(sparkdf, col_key, col_qty, col_time, param):
    """过去N天环比"""
    #     （当期-前期）/ 前期   nan
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt1 = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                         end=Window.currentRow)
        windowOpt2 = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(2 * (w - 1)),
                                                                                         end=Window.currentRow)
        sparkdf = sparkdf.withColumn("{0}_hb_{1}d".format(col_qty, w), sum(psf.col(col_qty)).over(windowOpt1) / (
                    sum(psf.col(col_qty)).over(windowOpt2) - sum(psf.col(col_qty)).over(windowOpt1)))
    return sparkdf


def col_tb_days(sparkdf, col_key, col_qty, col_time, param):
    """过去N天同比"""
    sparkdf_last = sparkdf.withColumn("dt", sparkdf.dt.cast('int') + lit(10000))
    sparkdf_last = sparkdf_last.withColumn("dt", sparkdf_last.dt.cast('string'))
    ws = param[1]
    for w in ws:
        windowOpt1 = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w - 1),
                                                                                         end=Window.currentRow)
        sparkdf_last = sparkdf_last.withColumn("{0}_tb_{1}_last".format(col_qty, w),
                                               sum(psf.col(col_qty)).over(windowOpt1))
        #         windowOpt2 = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w-1), end=Window.currentRow)
        sparkdf = sparkdf.withColumn("{0}_tb_{1}_current".format(col_qty, w), sum(psf.col(col_qty)).over(windowOpt1))
        sparkdf = sparkdf.join(sparkdf_last.select("shop_id", "goods_id", "dt", "{0}_tb_{1}_last".format(col_qty, w)),
                               on=col_key + ["dt"], how='left_outer')
        sparkdf = sparkdf.withColumn("{0}_tb_{1}d".format(col_qty, w), (
                    sparkdf["{0}_tb_{1}_current".format(col_qty, w)] - sparkdf["{0}_tb_{1}_last".format(col_qty, w)]) /
                                     sparkdf["{0}_tb_{1}_last".format(col_qty, w)])
        sparkdf = sparkdf.drop(*["{0}_tb_{1}_last".format(col_qty, w), "{0}_tb_{1}_current".format(col_qty, w)])
    return sparkdf


# def feature_sales_train(func,last_days,weeks):
#     agg_func = {'col_agg_days':(['avg','sum','min','max'],[2,3,4,5,8])}

#     """过去N天销量agg col_agg_days      qty_mean_2
#        过去N天有销量agg  col_gtz_agg_days     qty_gtz_mean_2
#        过去N天有销售天数 col_gtz_days     qty_gtz_count_2
#        过去N天环比   col_hb_days       qty_hb_2
#        过去N天同比   col_tb_days       qty_tb_2
#        过去N周工作日销量agg col_agg_weekdays  qty_mean_weekdays_2
#        过去N周周末销量agg   col_agg_weekend   qty_mean_weekend_2
#     """
#     sparkdf.groupby(col_key).agg("qty","sum")


def col_agg_weeks(sparkdf, col_key, col_qty, col_time, param):
    """过去N天col_qty的agg"""
    aggs = param[0]
    ws = param[1]
    for w in ws:
        windowOpt = Window.partitionBy(col_key).orderBy(col_time).rowsBetween(start=-(w - 1), end=Window.currentRow)
        for agg in aggs:
            sparkdf = sparkdf.withColumn("{0}_{1}_{2}w".format(col_qty, agg, w),
                                         globals()[agg](psf.col(col_qty)).over(windowOpt))
    return sparkdf


def build_sales_features_daily(spark, param):
    """
    dict_agg_func:字段聚合字典
    col_qty:聚合的列
    col_time:时间戳字段
    """
    col_key = param['col_key']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_qty = param['col_qty']
    dict_agg_func = eval(param['sales_feature_daily_func'])
    input_table = param['no_sales_adjust_table']
    output_table = param['sales_features_daily_table']
    sparkdf = read_table(spark, input_table,sdt='N')
    for dict_key in dict_agg_func:
        sparkdf = globals()[dict_key](sparkdf, col_key, col_qty, col_time, dict_agg_func[dict_key])
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))    
    save_table(spark,sparkdf, output_table)
    return 'SUCCESS'


def build_sales_features_weekly(spark, param):
    col_key = param['col_key']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_qty = param['col_qty']
    dict_agg_func = eval(param['sales_feature_weekly_func'])
    input_table = param['qty_aggregation_weekly_table']
    output_table = param['sales_features_weekly_table']
    sparkdf = read_table(spark, input_table)
    for dict_key in dict_agg_func:
        sparkdf = globals()[dict_key](sparkdf, col_key, col_qty, col_time, dict_agg_func[dict_key])
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))    
    save_table(spark,sparkdf, output_table)
    return 'SUCCESS'


# sparkdf = spark.sql("""select shop_id,goods_id,year,week,min(dt) as dt,sum(qty) qty from (select *,year(from_unixtime(unix_timestamp(cast(dt as string),'yyyyMMdd'),'yyyy-MM-dd')) year,weekofyear(from_unixtime(unix_timestamp(cast(dt as string),'yyyyMMdd'),'yyyy-MM-dd')) week   from ai_dm.poc_feat_y  ) t group by shop_id,goods_id,year,week""")
# build_sales_features_weekly(sparkdf, {'col_agg_weeks':(['mean','sum'],[2,3,4])},['shop_id','goods_id'],'qty',['year','week'],'20200101','20221231').where("shop_id=9029 and goods_id=1").show(1000)
# sparkdf = spark.sql(
#     """select *,to_unix_timestamp(cast(dt as string),'yyyyMMdd') sdt,dayofweek(from_unixtime(unix_timestamp(cast(dt as string),'yyyyMMdd'),'yyyy-MM-dd')) dayofweek from ai_dm.poc_feat_y """)
# build_sales_features_daily(sparkdf, {'col_gtz_agg_days': (['mean', 'sum'], [2, 3])}, ['shop_id', 'goods_id'], 'qty',
#                            'sdt', '20200101', '20221231').where("shop_id=9029 and goods_id=1").show(1000)