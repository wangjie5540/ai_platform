# -*- coding: utf-8 -*-
# @Time : 2022/06/29
# @Author : Arvin
from forecast.common.common_helper import *
from forecast.data_split.sp.data_prepare import data_prepare
from forecast.data_split.sp.model_selection import model_selection
from forecast.data_split.sp.model_grouping import group_category


def model_selection_grouping(spark,params_data_prepare,params_model_selection,params_model_grouping):
    """
    """
    sparkdf_config = data_prepare(spark, params_data_prepare) # 第一步：数据准备 商品配置信息、销量分层
    model_selection(spark, params_model_selection, sparkdf_config)  # 第二步：模型选择
    result = group_category(spark, params_model_grouping)  # 第三步：分组

    return result