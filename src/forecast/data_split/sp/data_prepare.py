# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据分组：数据准备
"""
from pyspark.sql.functions import *
from common.date_helper import date_add_str
from common.mysql import get_data_from_mysql
import copy

def get_master_data(spark,param):
    """
    获取主数据
    :param spark:
    :param param: 参数
    :return: 只包含主键的主数据
    """
    time_col=param['time_col']#时间对应的列名，如：dt
    table_master=param['table_master']#主表
    master_key_cols=param['master_key_cols']#主表对应的主键
    master_data=spark.table(table_master)
    edate=master_data.select([max(time_col)]).head(1)[0][0]#获取最大值
    sdate=date_add_str(edate,-90)#默认90天
    master_data=master_data.filter((master_data[time_col]>=sdate)&(master_data[time_col] <= edate))
    master_data=master_data.select(master_key_cols).distinct()
    return master_data

def get_calmonth(spark,sales_data,table_biz_dt,biz_dt_cols,time_col):
    """
    销量数据获取月份
    :param spark:
    :param sales_data:销量数据
    :param table_biz_dt: 含有时间列的表
    :param biz_dt_cols: 需要的列
    :param time_col: 时间列
    :return:
    """
    biz_join=copy.copy(biz_dt_cols)
    biz_join.append(time_col)
    biz_dt_table=spark.table(table_biz_dt).select(biz_join)
    sales_data=sales_data.join(biz_dt_table,on=time_col,how='left')#如：需要的月数据
    return sales_data

def get_sales_data(spark,param,master_data):
    """
    获取销量数据
    :param spark:
    :param param: 参数
    :param master_data: 主数据
    :return: 销量数据
    """
    feat_y=param['sales_data']['feat_y']
    edate=param['sales_data']['edate']
    sdate=param['sales_data']['sdate']
    time_col=param['sales_data']['time_col']
    y_type_value=param['sales_data']['y_type_value']
    y_type_col=param['sales_data']['y_type_col']
    master_key_cols=param['master_key_cols']
    sales_data=spark.table(feat_y)
    if edate=='' or sdate=='':
        edate=sales_data.select([max(time_col)]).head(1)[0][0]  #获取最大值
        sdate=date_add_str(edate,-400)#默认400天
    sales_data=sales_data.filter(sales_data[y_type_col].isin(y_type_value))
    sales_data=sales_data.filter((sales_data[time_col]>=sdate) & (sales_data[time_col]<=edate))
    sales_data=sales_data.join(master_data,on=master_key_cols,how='inner')
    sales_data=sales_data.withColumn('date_timestamp',unix_timestamp(time_col,'yyyyMMdd'))#增加时间戳
    return sales_data


def get_item_data(spark,item):
    """
    #获取item数据，包含sku的各种label
    :param spark:
    :param item: item表
    :return: sku的标签数据
    """
    query_sql='select * from {item} where deleted=0'.format(item=item)#mysql中取数据
    item_data=get_data_from_mysql(query_sql)
    item_data_columns=item_data.columns.tolist()
    # 转化为字符串,否则spark在有的字段识别不出类型
    for columns in item_data_columns:
        item_data[columns]=item_data[columns].astype(str)
    item_data = spark.createDataFrame(item_data)#转化为spark dataFrame
    item_data=item_data.withColumnRenamed('item_code','goods_id')#item表中goods_id用item_code表示
    return item_data

def get_property_data(spark,param,master_data):
    """
    获取商品属性值 PS：bound和label都来自于item表
    :param spark:
    :param param: 参数
    :param param: 主表数据
    :return:
    """
    item=param['table_master']#item表
    item_key_cols=param['item_key_cols']#item表中对应的主键
    item_data=get_item_data(spark,item)#包含所有的标签
    label_cols=param['label_cols']
    bound_cols=param['bound_cols']
    property_cols=copy.copy(label_cols)
    property_cols.extend(bound_cols)#bound和label的属性都来自于item表
    property_cols.extend(item_key_cols)
    item_data=item_data.select(property_cols)
    master_data=master_data.join(item_data,on=item_key_cols,how='left')
    master_data=master_data.fillna('')
    return master_data