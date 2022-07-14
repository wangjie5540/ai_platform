# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
from forecast.common.common_helper import *


def sales_aggregation_by_custom(sparkdf, other_agg_dim, col_custom):
    """自定义"""
    for x in col_custom:
        other_agg_dim.append(x)
    return sparkdf, other_agg_dim


def sales_aggregation_by_day(sparkdf, other_agg_dim, col_time, agg_type):
    other_agg_dim.append(col_time)
    return sparkdf, other_agg_dim


def sales_aggregation_by_month(sparkdf, other_agg_dim, col_time, agg_type):
    """
    solar_month:阳历月
    lunar_month:阴历月
    roll_month:滚动月
    """
    if agg_type == 'solar_month':
        func = udf(lambda x: (datetime.date(year=datetime.datetime.strptime(x, "%Y%m%d").year,
                                            month=datetime.datetime.strptime(x, "%Y%m%d").month, day=1)).strftime(
            "%Y%m%d"), StringType())
        sparkdf = sparkdf.withColumn(col_time, func(col_time))
        other_agg_dim += [col_time]
    elif agg_type == 'lunar_month':
        pass
    else:
        """待处理"""
        pass
    return sparkdf, other_agg_dim


def sales_aggregation_by_week(sparkdf, other_agg_dim, col_time, agg_type):
    """
    solar_week:阳历周
    lunar_week:阴历周
    roll_week:滚动周
    """
    if agg_type == 'solar_week':
        sparkdf = sparkdf.withColumn("solar_week", psf.weekofyear(psf.to_date(psf.col(col_time), "yyyyMMdd")))
        func = udf(lambda x: (datetime.datetime.strptime(x, "%Y%m%d") - datetime.timedelta(
            days=datetime.datetime.strptime(x, "%Y%m%d").weekday())).strftime("%Y%m%d"), StringType())
        sparkdf = sparkdf.withColumn(col_time, func(col_time))
        other_agg_dim += [col_time, 'solar_week']
    elif agg_type == 'lunar_week':
        pass
    else:
        """待处理"""
        pass
    return sparkdf, other_agg_dim


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


def sales_aggregation(spark, param):
    """销售聚合
    sparkdf, granularity_table, other_agg_dim, func_dict, col_qty, sdate, edate
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
    func_dict = eval(param['agg_func'])
    other_agg_dim = param['col_key']
    col_qty = param['col_qty']
    sdate = param['sdate']
    edate = param['edate']
    input_table = param['input_table']
    output_table = param['output_table']
    shop_list = param['shop_list']
    agg_type = param['agg_type']
    sparkdf = read_table(spark, input_table, shop_list=shop_list)
    sparkdf = sparkdf.filter("dt>={0} and dt<={1}".format(sdate, edate))
    #     print(sparkdf.show())
    for dict_key in func_dict:
        sparkdf, group_key = globals()[dict_key](sparkdf, other_agg_dim, func_dict[dict_key], agg_type)
        print(group_key, col_qty, "sum_{}".format(col_qty))

        if agg_type == 'day':
            sparkdf_group = sparkdf.groupby(group_key).agg(psf.sum(col_qty).alias("sum_{}".format(col_qty)))
        else:
            print(group_key)
            func = udf(lambda x: x.strftime('%Y%m%d'), StringType())
            sparkdf_group = sparkdf.groupby(group_key).agg(psf.sum(col_qty).alias("sum_{}".format(col_qty)))

    #     print(sparkdf_group.show(10))
    sparkdf_group = sparkdf_group.filter(date_filter_condition(sdate, edate))
    save_table(spark, sparkdf_group, output_table)
    return "SUCCESS"

# print("ready")
# sparkdf = sales_aggregation(sparkdf, ['shop_id', 'goods_id'], {'sales_aggregation_by_day': 'sdt'}, "qty", "20220101",
#                             "20220110")
# sparkdf.show(10)