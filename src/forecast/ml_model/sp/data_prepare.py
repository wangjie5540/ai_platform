# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习：样本选择的spark版本
"""

import traceback
import copy
from forecast.common.log import get_logger
from forecast.common.date_helper import date_add_str
from pyspark.sql.functions import max


def data_prepare_train(spark, param):
    """
    选择机器学习训练所用样本
    :param spark:spark
    :param param:样本选择
    :return:样本
    """
    logger_info = get_logger()
    table_sku_grouping = param['table_sku_group']
    ts_model_list = param['ts_model_list']
    table_feat_y = param['table_feat_y']
    y_type_list = param['y_type_list']
    cols_sku_grouping = param['cols_sku_grouping']
    apply_model = param['apply_model']
    y_type = param['y_type']
    sample_join_key_grouping = param['sample_join_key_grouping']  # 与分类分组表join的key
    table_feat_x = param['table_feat_x']
    sample_join_key_feat = param['sample_join_key_feat']
    dt = param['time_col']
    edate = param['edate']
    sdate = param['sdate']

    cols_feat_y_columns = param['cols_feat_y_columns']
    cols_feat_x_columns = param['cols_feat_x_columns']

    try:
        data_sku_grouping = spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping = data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        # y值表
        data_feat_y = spark.table(table_feat_y)
        if not cols_feat_y_columns and len(cols_feat_y_columns) > 0:
            cols_feat_y = sample_join_key_feat.copy()
            cols_feat_y.extend(cols_feat_y_columns)
            data_feat_y = data_feat_y.select(cols_feat_y)

        if edate == '' or sdate == '':
            edate = data_feat_y.select([max(dt)]).head(1)[0][0]  # 获取最大值
            sdate = date_add_str(edate, -365)  # 默认一年

        data_feat_y = data_feat_y.filter(data_feat_y[y_type].isin(y_type_list))
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt] <= edate))

        # 特征表
        data_feat_x = spark.table(table_feat_x)
        if not cols_feat_x_columns and len(cols_feat_x_columns) > 0:
            cols_feat_x = sample_join_key_feat.copy()
            cols_feat_x.extend(cols_feat_x_columns)
            data_feat_x = data_feat_x.select(cols_feat_x)

        data_feat_x = data_feat_x.filter(data_feat_x[y_type].isin(y_type_list))  # 也是筛选y_type
        data_feat_x = data_feat_x.filter((data_feat_x[dt] >= sdate) & (data_feat_x[dt] <= edate))

        data_feat = data_feat_y.join(data_feat_x, on=sample_join_key_feat, how='inner')
        data_result = data_feat.join(data_sku_grouping, on=sample_join_key_grouping, how='inner')

        logger_info.info("sample_select_sp train 成功")
    except Exception as e:
        data_result = None
        logger_info.info(traceback.format_exc())
    return data_result


def data_prepare_predict(spark, param):
    """
    选择机器学习预测所用样本
    :param spark:spark
    :param param:样本选择
    :return:样本
    """
    logger_info = get_logger()
    table_sku_grouping = param['table_sku_group']
    ts_model_list = param['ts_model_list']
    y_type_list = param['y_type_list']
    cols_sku_grouping = param['cols_sku_grouping']
    apply_model = param['apply_model']
    y_type = param['y_type']
    sample_join_key_grouping = param['sample_join_key_grouping']  # 与分类分组表join的key
    table_feat_x = param['table_feat_x']
    sample_join_key_feat = param['sample_join_key_feat']
    dt = param['time_col']

    cols_feat_x_columns = param['cols_feat_x_columns']
    forcast_start_date = param['forcast_start_date']

    try:
        data_sku_grouping = spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping = data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        # 特征表
        data_feat_x = spark.table(table_feat_x)
        if not cols_feat_x_columns and len(cols_feat_x_columns) > 0:
            cols_feat_x = sample_join_key_feat.copy()
            cols_feat_x.extend(cols_feat_x_columns)
            data_feat_x = data_feat_x.select(cols_feat_x)
        data_feat_x = data_feat_x.filter(data_feat_x[y_type].isin(y_type_list))  # 也是筛选y_type
        data_feat_x = data_feat_x.filter(data_feat_x[dt] == forcast_start_date)

        data_result = data_feat_x.join(data_sku_grouping, on=sample_join_key_grouping, how='inner')
        logger_info.info("sample_select_sp predict 成功")
    except Exception as e:
        data_result = None
        logger_info.info(traceback.format_exc())

    return data_result
