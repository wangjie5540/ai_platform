# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据准备模块：保证进入时序模型数据可用，无不连续值，空值；
"""
import logging
import traceback
import datetime

from pandas import pd

from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.datetime_helper import date_add_str
import pyspark.sql.functions as psf


def data_prepared_for_model(spark, param):
    setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="data_prepare_for_time_series_sp.info", error_log_file="", info_log_file_level="INFO")
    table_sku_grouping = param['table_sku_group']
    ts_model_list = param['ts_model_list']
    table_feat_y = param['feat_y']
    cols_sku_grouping = param['cols_sku_grouping']
    apply_model = param['apply_model']
    cols_feat_y = param['cols_feat_y']
    sample_join_key = param['sample_join_key']
    edate = param['edate']
    sdate = param['sdate']
    dt = param['time_col']
    try:
        # sku分类分组表
        data_sku_grouping = spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping = data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        # y值表
        data_feat_y = spark.table(table_feat_y).select(cols_feat_y)
        if edate == '' or sdate == '':
            edate = data_feat_y.select([psf.max(dt)]).head(1)[0][0]  # 获取最大值
            sdate = date_add_str(edate, -365)  # 默认一年
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt] <= edate))
        data_result = data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')

        # parititions = param['time_col']
        # prepare_data_table = param['prepare_data_table']
        # save_table(spark, data_result, prepare_data_table, partition=parititions)

        logging.info("数据准备完成！")
    except Exception as e:
        logging.info(traceback.format_exc())

    return data_result

def data_process(df, param):
    dt = param['time_col']
    y = param['col_qty']
    key_cols = param['key_cols']

    df[dt] = df[dt].apply(lambda x: pd.to_datetime(x))
    ts = pd.DataFrame(pd.date_range(start=df.dt.min(), end=df.dt.max()), columns=[dt])
    ts = ts.merge(df, on=dt, how='left')
    for i in key_cols:
        ts.loc[:, i] = df.loc[0, i]

    ts_null = ts[ts.isnull().values]
    ts_null.index = range(len(ts_null))

    for i in range(len(ts_null)):
        cur_date = ts_null.loc[i, dt]
        start = pd.to_datetime(cur_date) - pd.Timedelta(days=14)
        end = pd.to_datetime(cur_date) + pd.Timedelta(days=14)
        temp = pd.DataFrame(pd.date_range(start, end), columns=[dt])
        temp = temp.merge(df, on=dt, how='left')
        y_ = temp[y].mean()
        ts_null.loc[i, y] = y_

    ts2 = pd.concat([df, ts_null])
    data = ts2.sort_values(by=dt, ascending=True)  # 进行排序
    data[dt] = data[dt].apply(lambda x: datetime.datetime.strftime(x, "%Y%m%d"))
    return data
