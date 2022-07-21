# -*- coding: utf-8 -*-
# @Time : 2022/06/29
# @Author : Arvin
from forecast.common.reference_package import *
from digitforce.aip.common.spark_helper import *


def bound_identify(value, condition):
    if value is None:
        return False
    s = condition[0]
    e = condition[-1]
    condition = condition.replace('(', '').replace(')', '').replace('[', '').replace(']', '')
    s_num = condition.split(',')[0]
    e_num = condition.split(',')[1]
    if s_num == '':
        s_num = -np.inf
    else:
        s_num = int(s_num)
    if e_num == '':
        e_num = np.inf
    else:
        e_num = int(e_num)
    if s == '(' and e == ')':
        interval = P.open(s_num, e_num)
    elif s == '(' and e == ']':
        print(s_num, e_num)
        interval = P.openclosed(s_num, e_num)
    elif s == '[' and e == ')':
        interval = P.closedopen(s_num, e_num)
    else:
        interval = P.closed(s_num, e_num)
    return interval.contains(int(value))


def label_identify(value, condition):
    return value == condition


def generate_udf(model_selection_conditions, col_selection, col_bound, col_label):
    def apply_model_function(col_values):
        # columns 与 col_values顺序对应关系
        default_model = "DMS"
        for model_selection_condition in model_selection_conditions:
            for key in eval(model_selection_condition):
                print("model", key)
                conditions = eval(model_selection_condition)[key]
                for condition in conditions:
                    condition_len = len(condition.keys())
                    for condition_key in condition:
                        print("col_values", col_values)
                        value = col_values[col_selection.index(condition_key)]
                        if condition_key in col_bound and bound_identify(value, condition[condition_key]):
                            condition_len -= 1
                        if condition_key in col_label and label_identify(value, condition[condition_key]):
                            condition_len -= 1
                    if condition_len == 0:
                        default_model = key
                    else:
                        continue
        return default_model

    return udf(apply_model_function, StringType())


def model_selection(spark, params_model_selection, sparkdf_config):
    model_selection_conditions = params_model_selection['model_selection_condition']
    col_selection = params_model_selection['col_selection']
    col_bound = params_model_selection['col_bound']
    col_label = params_model_selection['col_label']
    col_key = params_model_selection['col_key']
    model_selection_table = params_model_selection['model_selection_table']
    # 用到的配置信息合并到一列
    sparkdf_config = sparkdf_config.select(col_key + col_selection).withColumn("col_selection_tmp", psf.array(col_selection))

    # 进行模型选择
    sparkdf_config = sparkdf_config.withColumn("apply_model",
                                               generate_udf(model_selection_conditions, col_selection, col_bound,
                                                            col_label)(sparkdf_config.col_selection_tmp))
    forecast_spark_helper.save_table(sparkdf_config, model_selection_table, partition=["shop_id"])
    print("模型选择已经完成！")