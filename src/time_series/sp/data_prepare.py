# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：样本选择的spark版本
"""
import traceback
from digitforce.aip.sof.common.log import get_logger
from digitforce.aip.sof.common.date_helper import date_add_str
from pyspark.sql.functions import max

#后续优化：check对周和月的支持 PS：在param的设置
def data_prepare(spark,param):
    """
    样本选择
    :param spark:spark
    :param param: 选择样本参数
    :return: 样本
    """
    logger_info=get_logger()
    table_sku_grouping=param['table_sku_group']
    ts_model_list=param['ts_model_list']
    table_feat_y=param['feat_y']
    y_type_list=param['y_type_list']
    cols_sku_grouping=param['cols_sku_grouping']
    apply_model=param['apply_model']
    y_type=param['y_type']
    cols_feat_y=param['cols_feat_y']
    sample_join_key=param['sample_join_key']
    dt=param['dt']
    edate=param['edate']
    sdate=param['sdate']

    try:
        #sku分类分组表
        data_sku_grouping=spark.table(table_sku_grouping).select(cols_sku_grouping)
        data_sku_grouping=data_sku_grouping.filter(data_sku_grouping[apply_model].isin(ts_model_list))

        #y值表
        data_feat_y=spark.table(table_feat_y).select(cols_feat_y)
        data_feat_y=data_feat_y.filter(data_feat_y[y_type].isin(y_type_list))
        if edate == '' or sdate == '':
            edate=data_feat_y.select([max(dt)]).head(1)[0][0]#获取最大值
            sdate=date_add_str(edate,-365)#默认一年
        data_feat_y = data_feat_y.filter((data_feat_y[dt] >= sdate) & (data_feat_y[dt]<= edate))
        data_result=data_feat_y.join(data_sku_grouping, on=sample_join_key, how='inner')

        logger_info.info("sample_select_sp 成功")
    except Exception as e:
        data_result=None
        logger_info.info(traceback.format_exc())
    return data_result