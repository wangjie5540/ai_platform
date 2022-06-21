# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    规则：销量分类
"""
from common.date_helper import *
from pyspark.sql import Window
from pyspark.sql.functions import *
import copy
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from data_split.sp.data_prepare import get_calmonth,get_sales_data

def get_tail_judgement_condition(sales_data,param):
    """
    获取判断sku是否为尾部的条件
    :param sales_data: 销量数据
    :param param: 参数
    :return:包含尾部判断条件的数据
    """
    sale_classify_param=param['method_param']['sale_classify']
    sale_classify_key_cols=sale_classify_param['sale_classify_key_cols']
    w=sale_classify_param['w']
    y_col=param['sale_data']['y_col']
    threshold_count=sale_classify_param['threshold_count']
    sales_data_window=Window.partitionBy(sale_classify_key_cols).orderBy(col('date_timestamp')).rangeBetween(
        start=-(86400*w-1),end=Window.currentRow)
    sales_data_tail=sales_data.withColumn('s_wm',sum(y_col).over(sales_data_window))
    sales_data_tail_count=sales_data_tail.groupBy(sale_classify_key_cols).count()
    sales_data_tail_count=sales_data_tail_count.withColumnRenamed('count','count_all')  # 如果不新命名则count列类型为method
    sales_data_tail_threshold=sales_data_tail.filter(sales_data_tail['s_wm'] > threshold_count).groupBy(sale_classify_key_cols).count()
    sales_data_tail_threshold=sales_data_tail_threshold.withColumnRenamed('count','threshold_count')
    sales_data_tail_count_join=sales_data_tail_count.join(sales_data_tail_threshold,on=sale_classify_key_cols,how='left')
    sales_data_tail_count_join=sales_data_tail_count_join.fillna(0)  # null值进行补0
    sales_data_tail_count_join=sales_data_tail_count_join.withColumn(
        "count_proportion",sales_data_tail_count_join.threshold_count/sales_data_tail_count_join.count_all)
    sales_data_tail_count_join=sales_data_tail_count_join.drop('threshold_count').drop('count_all')
    return sales_data_tail_count_join

def month_add_str_udf(step_len):
    """
    注册函数：当前月前n个月日期，到月份
    :param step_len: 时长
    :return:
    """
    def month_add_str_value(month_str):
        month_str=month_add_str(month_str,step_len)
        return month_str
    return udf(month_add_str_value,StringType())

def get_pre_month_date(sales_data_groupby,sale_classify_key_cols,biz_dt_cols):
    """
    获取前1个月、2月、3月和上年同期对应日期
    :param sales_data_groupby: 销量数据进行groupby
    :param sale_classify_key_cols: 销量分类的关键字段
    :param biz_dt_cols: 日期列
    :return: 加上日期的数据
    """
    windowSpec=Window.partitionBy(sale_classify_key_cols).orderBy(desc(biz_dt_cols[0]))
    sales_data_row_num=sales_data_groupby.withColumn('row_number',row_number().over(windowSpec))
    sales_data_row_num=sales_data_row_num.filter(sales_data_row_num['row_number']==1)#筛选最新日期的数据
    sales_data_row_num=sales_data_row_num.withColumn("calmonth_1",month_add_str_udf(1)(sales_data_row_num[biz_dt_cols[0]]))#上个月
    sales_data_row_num=sales_data_row_num.withColumn("calmonth_2", month_add_str_udf(2)(sales_data_row_num[biz_dt_cols[0]]))#前第2个月
    sales_data_row_num=sales_data_row_num.withColumn("calmonth_3",month_add_str_udf(3)(sales_data_row_num[biz_dt_cols[0]]))#前第3个月
    sales_data_row_num=sales_data_row_num.withColumn("calmonth_12", month_add_str_udf(12)(sales_data_row_num[biz_dt_cols[0]]))#上年同期
    return sales_data_row_num

def get_pre_month_sale(sales_data_row_num,sales_data_groupby,sale_classify_key_cols,biz_dt_cols):
    """
    获取前1个月、2月、3月和上年同期对应销量
    :param sales_data_row_num:加上日期的数据
    :param sales_data_groupby:销量数据进行groupby
    :param sale_classify_key_cols:销量分类的关键字段
    :param biz_dt_cols:日期列
    :return:加上销量的数据
    """
    sale_classify_key_cols_copy = copy.copy(sale_classify_key_cols)
    sale_classify_key_cols_copy.extend(['calmonth_1', 'calmonth_2', 'calmonth_3', 'calmonth_12'])
    sales_data_row_num = sales_data_row_num.select(sale_classify_key_cols_copy)
    for i in ['1', '2', '3', '12']:#分别对应前1个月、2月、3月和上年同期
        colmonth_str='calmonth_'+i
        sales_calmonth_str='sales_calmonth_' + i
        sales_data_groupby=sales_data_groupby.withColumn(colmonth_str, sales_data_groupby[biz_dt_cols[0]])
        sale_classify_key_cols_copy=copy.copy(sale_classify_key_cols)
        sale_classify_key_cols_copy.append(colmonth_str)
        sales_data_row_num = sales_data_row_num.join(sales_data_groupby, on=sale_classify_key_cols_copy,how='left').withColumnRenamed('y_sum',sales_calmonth_str).drop(
            biz_dt_cols[0]).drop(colmonth_str)
        sales_data_groupby = sales_data_groupby.drop(colmonth_str)
    return sales_data_row_num

def get_ls_judgement_condition(spark,sales_data,param):
    """
    获取低销判断数据
    :param spark:
    :param sales_data: 销量数据
    :param param: 参数
    :return: 带有低销条件的数据
    """
    biz_dt_cols=param['biz_dt_cols']
    table_biz_dt=param['table_biz_dt']
    time_col=param['time_col']
    sale_classify_key_cols=param['method_param']['sale_classify']['sale_classify_key_cols']
    y_col=param['sales_data']['y_col']
    sales_groupby_key=copy.copy(sale_classify_key_cols)
    sales_groupby_key.extend(biz_dt_cols)
    sales_data=get_calmonth(spark,sales_data,table_biz_dt,biz_dt_cols,time_col)#增加需要的列
    sales_data_groupby=sales_data.groupby(sales_groupby_key).agg(sum(y_col).alias('y_sum'))
    sales_data_groupby=sales_data_groupby.sort(biz_dt_cols[0],ascending=False)
    sales_data_row_num=get_pre_month_date(sales_data_groupby,sale_classify_key_cols,biz_dt_cols)#获取前1个月、2月、3月和上年同期对应日期
    sales_groupby_key.append('y_sum')
    sales_data_groupby=sales_data_groupby.select(sales_groupby_key)
    sales_data_row_num=get_pre_month_sale(sales_data_row_num, sales_data_groupby, sale_classify_key_cols, biz_dt_cols)#获取前1个月、2月、3月和上年同期对应销量
    sales_data_row_num = sales_data_row_num.fillna(0)
    return sales_data_row_num

def sale_classify_udf(threshold_proportion,sale_month_quantity):
    """
    销量分类的注册函数，利用闭包
    :param threshold_proportion: 小于阈值占比
    :param sale_month_quantity: 月均销量
    :return:
    """
    def sale_tag_udf(count_proportion,sale_month_1, sale_month_2, sale_month_3, sale_month_12):
        tag='normal'
        if count_proportion < threshold_proportion:
            tag='tail'#尾部品
        elif sale_month_1<=sale_month_quantity and sale_month_2<=sale_month_quantity and sale_month_3<=sale_month_quantity \
                and sale_month_12 <= sale_month_quantity:
            tag='ls'#低消品
        return tag
    return udf(sale_tag_udf,StringType())

def get_sale_classify_tag(spark,param,master_data):
    """
    获取销量标签
    :param spark:
    :param param:参数
    :param master_data:主数据
    :return: 主键+销量分类标签
    """
    threshold_proportion=param['method_param']['sale_classify']['threshold_proportion']
    sale_month_quantity=param['method_param']['sale_classify']['sale_month_quantity']
    sales_data=get_sales_data(spark,param,master_data)#销量数据
    sales_data_tail=get_tail_judgement_condition(sales_data,param)#判断尾部品所需条件数据
    sales_data_ls=get_ls_judgement_condition(spark,sales_data,param)#判断低消品所需条件数据
    sale_classify_key_cols=param['sale_classify_key_cols']
    sales_tag=sales_data_tail.join(sales_data_ls, on=sale_classify_key_cols, how='left')
    sales_tag=sales_tag.fillna(0)
    sales_data_rule=sales_tag.withColumn("sale_classify", sale_classify_udf(threshold_proportion,sale_month_quantity)(sales_tag['count_proportion'],sales_tag['sales_calmonth_1'],
                            sales_tag['sales_calmonth_2'],sales_tag['sales_calmonth_3'],sales_tag['sales_calmonth_12']))
    sale_classify_key_cols_copy=copy.copy(sale_classify_key_cols)
    sale_classify_key_cols_copy.append('sale_classify')
    sales_data_rule=sales_data_rule.select(sale_classify_key_cols_copy)
    return sales_data_rule