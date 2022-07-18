# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据准备模块：保证进入时序模型数据可用，无不连续值，空值；
"""
import traceback
from datetime import datetime

import pandas as pd
from forecast.common.log import get_logger
from forecast.common.date_helper import date_add_str
from pyspark.sql.functions import max
from forecast.common.common_helper import *

# #后续优化：check对周和月的支持 PS：在param的设置
# def data_prepare(spark,param):
#     """
#     样本选择
#     :param spark:spark
#     :param param: 选择样本参数
#     :return: 样本
#     """
#     logger_info=get_logger()
#     table_sku_grouping=param['table_sku_group']
#     ts_model_list=param['ts_model_list']
#     table_feat_y=param['feat_y']
#     y_type_list=param['y_type_list']
#     cols_sku_grouping=param['cols_sku_grouping']
#     apply_model=param['apply_model']
#     y_type=param['y_type']
#     cols_feat_y=param['cols_feat_y']
#     sample_join_key=param['sample_join_key']
#     dt=param['dt']
#     edate=param['edate']
#     sdate=param['sdate']
#
#     try:
#         #sku分类分组表
#         data_sku_grouping=spark.table(table_sku_grouping).select(cols_sku_grouping)
#         data_sku_grouping=data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))
#
#         #y值表
#         data_feat_y=spark.table(table_feat_y).select(cols_feat_y)
#         data_feat_y=data_feat_y.filter(data_feat_y[y_type].isin(y_type_list))
#         if edate == '' or sdate == '':
#             edate=data_feat_y.select([max(dt)]).head(1)[0][0]#获取最大值
#             sdate=date_add_str(edate,-365)#默认一年
#         data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt]<= edate))
#         data_result=data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')
#
#         logger_info.info("sample_select_sp 成功")
#     except Exception as e:
#         data_result=None
#         logger_info.info(traceback.format_exc())
#     return data_result

#检验数据的连续性，如果不连续则补齐
def data_precess(param,spark):
    table_sku_grouping = param['table_sku_group']
    ts_model_list = param['ts_model_list']
    table_feat_y = param['feat_y']
    y_type_list = param['y_type_list']
    cols_sku_grouping = param['cols_sku_grouping']
    apply_model = param['apply_model']
    y_type = param['y_type']
    cols_feat_y = param['cols_feat_y']
    sample_join_key = param['sample_join_key']
    edate = param['edate']
    sdate = param['sdate']
    dt = 'dt'
    y = param['col_qty']
    predict_start = param['forecast_start_date']
    try:
        #sku分类分组表
        data_sku_grouping=spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping=data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        #y值表
        data_feat_y=spark.table(table_feat_y).select(cols_feat_y)
        if edate == '' or sdate == '':
            edate=data_feat_y.select([max(dt)]).head(1)[0][0]#获取最大值
            sdate=date_add_str(edate,-365)#默认一年
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt]<= edate))
        data_result=data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')

        df = data_result.toPandas()

        df[dt] = df[dt].apply(lambda x: pd.to_datetime(x))
        ts = pd.DataFrame(pd.date_range(start=df.dt.min(), end=df.dt.max()), columns=[dt])
        ts = ts.merge(df, on=dt, how='left')

        last_day = pd.to_datetime(predict_start) - pd.Timedelta(days=1)

        if ts.dt.iloc[-1].dayofweek != last_day.dayofweek:
            diff = (last_day.dayofweek - ts.dt.iloc[-1].dayofweek) % 7
            for d in pd.date_range(end=last_day, periods=diff):
                ts = ts.append({dt: d, 'days_week': d.dayofweek}, ignore_index=True)
        ts.dt = pd.date_range(end=last_day, periods=ts.shape[0])

        ts[dt] = ts[dt].apply(lambda x: pd.to_datetime(x))
        sales_cleaned = pd.DataFrame()
        for d in range(0, 7):
            t = ts.loc[ts.dt.dt.dayofweek == d].sort_values(dt).reset_index(drop=True)
            t['dy'] = t[y].shift(-1)
            t['uy'] = t[y].shift(1)
            t.dy = t.dy.fillna(method='ffill').fillna(method='bfill')
            t.uy = t.uy.fillna(method="ffill").fillna(method="bfill")

            t[y] = t[y].fillna((t.dy + t.uy) / 2)
            t[y].fillna(method="ffill", inplace=True)
            t[y].fillna(method="bfill", inplace=True)
            t[y].fillna(t[y].rolling(7, min_periods=0, center=True).mean(), inplace=True)
            t[y].fillna(0.0, inplace=True)
            sales_cleaned = sales_cleaned.append(t)

        sales_cleaned = sales_cleaned[['shop_id', 'goods_id', 'th_y', 'apply_model', 'dt']].sort_values('dt')
        sales_cleaned = sales_cleaned.dropna()
        sales_cleaned["dt"] = sales_cleaned["dt"].apply(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d"))

        df_value = sales_cleaned.values.tolist()
        df_columns = sales_cleaned.columns
        spark_df = spark.createDataFrame(df_value, ['shop_id', 'goods_id', 'th_y', 'apply_model', 'dt'])

        logger_info.info("数据准备完成！")
    except Exception as e:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>...data prepare",e)
        status = False
        logger_info.info(traceback.format_exc())

    return spark_df


#时序模型数据准备
def data_prepared_for_model(spark,param):
    status = True
    logger_info = get_logger()
    table_sku_grouping = param['table_sku_group']
    ts_model_list = param['ts_model_list']
    table_feat_y = param['feat_y']
    y_type_list = param['y_type_list']
    cols_sku_grouping = param['cols_sku_grouping']
    apply_model = param['apply_model']
    y_type = param['y_type']
    cols_feat_y = param['cols_feat_y']
    sample_join_key = param['sample_join_key']
    edate = param['edate']
    sdate = param['sdate']
    dt = 'dt'
    # predict_start = param['forecast_start_date']
    print(">>>>>>>>>>>>>>>>>>>>>>>.param:")
    print(table_sku_grouping, ts_model_list,table_feat_y,y_type_list,cols_sku_grouping, apply_model,cols_feat_y, sample_join_key,edate,sdate,dt)
    try:
        #sku分类分组表
        data_sku_grouping=spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping=data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        #y值表
        data_feat_y=spark.table(table_feat_y).select(cols_feat_y)
        # data_feat_y=data_feat_y.filter(data_feat_y[y_type].isin(y_type_list))
        if edate == '' or sdate == '':
            edate=data_feat_y.select([max(dt)]).head(1)[0][0]#获取最大值
            sdate=date_add_str(edate,-365)#默认一年
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt]<= edate))
        data_result=data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')
        logger_info.info("sample_select_sp 成功")
        if param['time_type'] == 'day':
            data_result = data_precess(param,spark)
        else:
            pass
        parititions = param['time_col']
        prepare_data_table = param['prepare_data_table']
        save_table(spark, data_result, prepare_data_table, partition=parititions)

        logger_info.info("数据准备完成！")
    except Exception as e:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>...data prepare",e)
        status = False
        logger_info.info(traceback.format_exc())


    return status
















