# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据准备模块：保证进入时序模型数据可用，无不连续值，空值；
"""
import logging
import traceback
from digitforce.aip.common.logging_config import setup_console_log,setup_logging
from digitforce.aip.common.datetime_helper import date_add_str
import pyspark.sql.functions as psf


def data_prepared_for_model(spark,param):
    logger_info = setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="",error_log_file="",info_log_file_level="INFO")
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
        #sku分类分组表
        data_sku_grouping=spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping=data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        #y值表
        data_feat_y=spark.table(table_feat_y).select(cols_feat_y)
        if edate == '' or sdate == '':
            edate=data_feat_y.select([psf.max(dt)]).head(1)[0][0]#获取最大值
            sdate=date_add_str(edate,-365)#默认一年
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt]<= edate))
        data_result=data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')

        # parititions = param['time_col']
        # prepare_data_table = param['prepare_data_table']
        # save_table(spark, data_result, prepare_data_table, partition=parititions)

        logger_info.info("数据准备完成！")
    except Exception as e:
        logger_info.info(traceback.format_exc())


    return data_result
















