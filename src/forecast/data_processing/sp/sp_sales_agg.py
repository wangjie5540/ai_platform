# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
from common.common_helper import *

def sales_aggregation_by_custom(sparkdf, other_agg_dim, col_custom):
    """自定义"""
    for x in col_custom:
        other_agg_dim.append(x)
    return sparkdf, other_agg_dim


def sales_aggregation_by_day(sparkdf, other_agg_dim, col_day):
    other_agg_dim.append(col_day)
    return sparkdf, other_agg_dim


def sales_aggregation_by_month(sparkdf, other_agg_dim, col_month, month_type):
    """
    solar_week:阳历月
    lunar_week:阴历月
    roll_week:滚动月
    """
    if month_type != 'roll_month':
        other_agg_dim.append(col_month)
        return sparkdf, other_agg_dim
    else:
        """待处理"""
        #         sparkdf = sparkdf.withColumn("roll_month",***)
        return sparkdf, other_agg_dim.append('roll_month')


def sales_aggregation_by_week(sparkdf, other_agg_dim, week_type):
    """
    solar_week:阳历周
    lunar_week:阴历周
    roll_week:滚动周
    """
    if week_type == 'solar_week':
        return sparkdf, other_agg_dim.append('solar_week')
    elif week_type == 'lunar_week':
        return sparkdf, other_agg_dim.append('lunar_week')
    else:
        """待处理"""
        return sparkdf, other_agg_dim.append('roll_week')


def sales_aggregation_by_time(sparkdf, other_agg_dim, time_agg_param):
    """时段聚合"""

    sql_str = "case "
    # time_change_fun = udf(time_change, StringType())
    #     sparkdf = sparkdf.withColumn('time_agg_param',lit(time_agg_param))
    for time in time_agg_param:
        start = int(time.split('-')[0])
        end = int(time.split('-')[1])
        sql_str += " when pay_hour>={0} and pay_hour<{1} then '{2}'".format(start, end, time)
    sql_str += " end time_agg_type"
    sparkdf = sparkdf.selectExpr("*", sql_str)
    return sparkdf, other_agg_dim.append('time_agg_type')


def sales_aggregation(sparkdf, granularity_table, other_agg_dim, func_dict, col_qty, sdate, edate):
    """销售聚合
    dict_key = {'shop':'shop_id','sku':'goods_id','day':'dt'}

    1.读表
    2.for [[shop,cate4,day],[shop,sku,week],[city,cate4,month]]:
         granuliarity = []
         dim = eval([shop,cate4,day])
         if i in dict_key.keys():
            granuliarity.append(dict_key[i])
            groupby()
         insert overwrite table

    """

    sparkdf = sparkdf.filter("dt>={0} and dt<={1}".format(sdate, edate))
    for dict_key in func_dict:
        sparkdf, group_key = globals()[dict_key](sparkdf, other_agg_dim, func_dict[dict_key])
        sparkdf_group = sparkdf.groupby(group_key).agg(sum(col_qty).alias("sum_{0}".format(col_qty)))
    return sparkdf_group.filter(date_filter_condition(sdate, edate))


# print("ready")
# sparkdf = sales_aggregation(sparkdf, ['shop_id', 'goods_id'], {'sales_aggregation_by_day': 'sdt'}, "qty", "20220101",
#                             "20220110")
# sparkdf.show(10)