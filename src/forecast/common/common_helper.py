# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
import pandas as pd
import numpy as np
from pyspark.sql import Row
import pyspark.sql.functions as psf
from pyspark.sql import Window
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, lit, concat_ws, lead, lag
from scipy import stats
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import FloatType, IntegerType, StringType, DoubleType, StructType, StructField
from forecast.common.mysql import get_data_from_mysql
from functools import reduce
import portion as P
import datetime
import chinese_calendar as calendar

def date_filter_condition(sdate, edate):
    """
    按日期过滤
    """
    if sdate == '' and edate != '':
        date_filter = " dt<={}".format(edate)
    elif sdate != '' and edate == '':
        datefilter = "dt>={}".format(sdate)
    elif sdate != '' and edate != '':
        date_filter = "dt>={0} and dt<={1}".format(sdate, edate)
    else:
        date_filter = '1=1'
    return date_filter


def rdd_format_pdf(rows):
    """
    rdd转pandas_df
    """
    row_list = list()
    for row in rows:
        row_list.append(row.asDict())
    df = pd.DataFrame(row_list)
    return df


def pdf_format_rdd(result_df):
    """
    pandas_df转rdd
    """
    resultRow = Row(*result_df.columns)
    row_list = []
    for r in result_df.values:
        row_list.append(resultRow(*r))
    return row_list


def days(i):
    return i * 86400


def sales_continue(df, edate, col_qty, col_time, col_key):
    """创建df日期连续 col_qty:处理的列"""
    c_columns = df.columns.tolist()
    c_columns.remove(col_qty)
    sdate = df[col_time].min()
    sr = pd.Series(index=pd.date_range(sdate, edate), data=np.nan)
    st = pd.Series(index=df[col_time].astype('datetime64'), data=df[col_qty].tolist())
    sr.loc[sr.index.intersection(st.index)] = st.loc[st.index.intersection(sr.index)]
    df.drop(col_qty, axis=1, inplace=True)
    dr = pd.DataFrame(sr).reset_index()
    dr.columns=[col_time,col_qty]
    df.dt = df.dt.apply(lambda x: pd.to_datetime(x))
    df = pd.merge(dr, df, on=col_time, how='left')
    #填充其他
    for column in col_key:
        df[column].fillna(method='ffill', inplace=True)
        df[column].fillna(method='bfill', inplace=True)
    return df


def compute_year_on_year_ratio(current_value, last_value):
    """计算系数"""
    # 基础倍数
    ratio_upper = 5
    ratio_lower = 0.5

    if pd.isna(current_value) or pd.isna(last_value):
        return 1
    elif current_value == 0.0:
        "整个品类下架"
        return 0
    else:
        inc_ratio = max(ratio_lower, min(ratio_upper, current_value / max(1.0, last_value)))
        return inc_ratio
    

def tuple_self(list_num):
    """
    自定义tuple
    :param list_num: 仅限 str 类型
    :return:
    """
    if len(list_num) == 1:
        return "('"+list_num[0]+"')"
    else:
        return tuple(list_num)


def is_exist_table(spark, check_table):
    """
    判断表是否存在
    :param spark:
    :param check_table: 要检查的表
    :return:
    """
    result = False
    
    try:
        if spark.table("{0}".format(check_table)):
            result = True 
    except:
        pass
    return result    


def show_columns(spark, check_table):
    columns = spark.sql("show columns in {0}".format(check_table)).toPandas()['col_name'].tolist()
    return columns
    

# def read_table(spark, table_name, sdt='Y', dt="dt", shop_list=[]):
#     """dt:分区字段
#        sdt:时间戳字段
#     """
#     filter_str = ""
#     if len(shop_list)>0:
#         filter_str = " where shop_id in {0}".format(tuple_self(shop_list))
#     sparkdf = spark.sql("""select * from {0} {1} """.format(table_name, filter_str))
#     if sdt == 'N':
#         sparkdf = sparkdf.withColumn("sdt", psf.unix_timestamp(psf.to_timestamp(psf.col(dt), 'yyyyMMdd'),
#                                                                "format='yyyy-MM-dd"))
#     return sparkdf


def read_table(spark, table_name, sdt='Y', dt="dt", partition_name='shop_id', partition_list=[]):
    """dt:分区字段
       sdt:时间戳字段
    """
    filter_str = ""
    if len(partition_list) > 0:
        filter_str = " where {0} in {1}".format(partition_name, tuple_self(partition_list))
    sparkdf = spark.sql("""select * from {0} {1} """.format(table_name, filter_str))
    if sdt == 'N':
        sparkdf = sparkdf.withColumn("sdt", psf.unix_timestamp(psf.to_timestamp(psf.col(dt), 'yyyyMMdd'),
                                                               "format='yyyy-MM-dd"))
    return sparkdf


def read_origin_category_table(spark, table_name, sdt='Y', dt="dt", shop_list=[]):
    filter_str = ""
    if len(shop_list)>0:
        filter_str = " where site_code in {0}".format(tuple_self(shop_list))
    sparkdf = spark.sql("""select site_code as shop_id,goods_code as goods_id, category4_code   
                    from {0} {1}  
                    group by site_code,goods_code,category4_code """.format(table_name,filter_str))
    return sparkdf
    

   
def read_origin_stock_table(spark, table_name, sdt='Y', dt="dt", shop_list=[]):
    """dt:分区字段
       sdt:时间戳字段
    """
    filter_str = ""
    if len(shop_list)>0:
        filter_str = " where site_code in {0}".format(tuple_self(shop_list))
    sparkdf = spark.sql("""select t1.shop_id as shop_id ,t1.goods_id as goods_id,cast(t1.stock_available as double) as opening_inv,
                    cast(t2.stock_available as double) as ending_inv,t1.dt as dt
                    from 
                  (select site_code as shop_id,goods_code as goods_id,stock_available,dt from {0} {1}) t1
                   left join
   (select site_code as shop_id,goods_code as goods_id,stock_available,regexp_replace(date_sub(from_unixtime(to_unix_timestamp(dt,'yyyyMMdd'),
   'yyyy-MM-dd'),1),"-","") dt from {0} {1}) t2
    on t1.shop_id= t2.shop_id and t1.goods_id = t2.goods_id and t1.dt = t2.dt""".format(table_name,filter_str))
    return sparkdf

    
def read_origin_sales_table(spark, table_name, sdt='Y', dt="dt", shop_list=[]):
    """dt:分区字段
       sdt:时间戳字段
    """
    filter_str = ""
    if len(shop_list)>0:
        filter_str = " and site_code in {0}".format(tuple_self(shop_list))
    sparkdf = spark.sql("""select goods_code as goods_id,order_id,quantity as qty,substring(data_date,12,2) as pay_hour,
                           sales_price,  to_unix_timestamp(dt,'yyyyMMdd') as  sdt,site_code as shop_id,dt from 
                           {0} where 	site_code<>'site_code' {1} and quantity>0""".format(table_name, filter_str))
    if sdt == 'N':
        sparkdf = sparkdf.withColumn("sdt", psf.unix_timestamp(psf.to_timestamp(psf.col(dt), 'yyyyMMdd'),
                                                               "format='yyyy-MM-dd"))
    return sparkdf


def read_origin_weather_table(spark,table_name,sdate,edate):
    sparkdf = spark.sql("""select *,to_unix_timestamp(recordtime,'yyyy-MM-dd') sdt,replace(recordtime,'-','') dt,
                   from_unixtime(to_unix_timestamp(date_add(recordtime,1 - case when dayofweek(recordtime) = 1 then 7 
                   else dayofweek(recordtime) - 1 end),'yyyy-MM-dd'),'yyyyMMdd') week_dt,weekofyear(recordtime) week,
                   from_unixtime(to_unix_timestamp(trunc(recordtime,'MM'),'yyyy-MM-dd'),'yyyyMMdd') month_dt from   {0} 
                  """.format(table_name))
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))
    return sparkdf


def read_origin_site_table(spark,table_name,shops):
    sparkdf = spark.sql("""
    select site_code as shop_id,province,city,district  from {0}  
    where site_code in {1}
    group by site_code,province,city,district
    """.format(table_name,tuple_self(shops)))
    return sparkdf


def save_table(spark, sparkdf, table_name, save_mode='overwrite', partition=["shop_id","dt"]):
    if is_exist_table(spark, table_name):
        columns = show_columns(spark, table_name)
        print(columns, table_name)
        sparkdf.repartition(1).select(columns).write.mode("overwrite").insertInto(table_name, True)
    else:  
        print("save table name", table_name)
#         print(sparkdf.show(10))
        sparkdf.write.mode(save_mode).partitionBy(partition).saveAsTable(table_name)