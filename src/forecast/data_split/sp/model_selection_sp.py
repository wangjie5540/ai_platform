# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    模型选择
"""
from pyspark.sql.functions import *
import copy
import json
from pyspark.sql.types import *
from data_split.sp.data_prepare import get_property_data,get_master_data
from data_split.sp.sale_classify_sp import get_sale_classify_tag

def get_source_data(spark,param):
    """
    数据分组所用源数据
    :param spark:
    :param param:
    :return:
    """
    bound_cols=param['bound_cols']
    label_cols=param['label_cols']
    method_cols=param['method_cols']
    select_cols=copy.copy(bound_cols)
    select_cols.extend(label_cols)
    master_data=get_master_data(spark,param)#获取主数据
    property_data=get_property_data(spark,param,master_data)#bound和label所用属性值
    method_key_cols=param['method_key_cols']#method方法中的主键
    for method in method_cols:#方法所用的属性值
        if method=='sale_classify':
            sale_classify_property=get_sale_classify_tag(spark,param,master_data)
            property_data=property_data.join(sale_classify_property,on=method_key_cols,how='left')
        else:
            pass
    property_data=property_data.fillna('')
    return property_data

def analysis_bound_condition(bound_value):
    """
    解析bound条件
    :param bound_value: bound条件值
    :return: 左值、左符合、右值、右符号
    """
    bound_value=str(bound_value).split(',')
    if len(bound_value)!=2:
        return None, None, None, None
    left_boundary=bound_value[0]
    right_boundary=bound_value[1]
    left_symbol=left_boundary[0]
    right_sysmbol=right_boundary[-1]
    try:
        left_boundary=float(left_boundary[1:])
    except:
        left_boundary=None
    try:
        right_boundary=float(right_boundary[:-1])
    except:
        right_boundary=None
    return left_boundary,left_symbol,right_boundary,right_sysmbol

def judge_bound_value(table_value,bound_value):
    """
    属性值是否满足bound值的条件
    :param table_value:属性值
    :param bound_value:bound条件
    :return:满足返回1、否则-1
    """
    meet_condition=-1
    if str(table_value).lower()=='none' or str(table_value).lower()=='' or str(table_value).lower()=='null':
        return meet_condition
    left_boundary,left_symbol,right_boundary,right_sysmbol=analysis_bound_condition(bound_value)
    if left_symbol=='[':
        try:
            left_boundary=float(left_boundary)+0.0001
        except:
            left_boundary=None
    if right_boundary==']':
        try:
            right_boundary=float(right_boundary)+0.0001
        except:
            right_boundary=None
    if left_boundary!=None and right_boundary!=None:
        if left_boundary==right_boundary:
            if float(left_boundary)==float(table_value):
                meet_condition=1
        else:
            if float(table_value)>float(left_boundary) and float(table_value)<float(right_boundary):
                meet_condition=1
    elif left_boundary!=None and right_boundary==None:
        if float(table_value)>float(left_boundary):
            meet_condition=1
    elif left_boundary==None and right_boundary!=None:
        if float(table_value)<right_boundary:
            meet_condition=1
    return meet_condition

def value_modify(value):
    """
    对值进行修正
    :param value: 字符串
    :return: 修正后的值
    """
    if str(value).lower()=='true' or str(value).lower()=='是':
        return '1'
    elif str(value).lower()=='none' or str(value).lower()=='null' or str(value).lower()=='':
        return '0'

def mrege_judge_cols(data,judge_cols):
    """
    根据判断列增加两列：列名和值 PS：目的是为了应用注册函数
    :param data:数据
    :param judge_cols:要判断的列名
    :return:增加两列后的数据
    """
    judge_cols_name='&#&'.join(judge_cols)
    data=data.withColumn('judge_cols_name', lit(judge_cols_name))#增加一列
    data=data.withColumn(
        'judge_cols_value',
        concat_ws("&#&", *[col(c) for c in data.select(judge_cols).columns])
    )
    return data


def model_judge_udf(model_selection_condition,default_model,bound_cols):
    """
    模型选择的注册函数 PS：利用闭包
    :param model_selection_condition: 模型选择条件
    :param default_model: 默认使用模型
    :param bound_cols: bound列
    :return:
    """
    def model_selection_func(master_cols_name,master_cols_value):
        apply_model=default_model
        master_cols_name=str(master_cols_name).split('&#&')#在合并的时候用的特殊符
        master_cols_value=str(master_cols_value).split('&#&')
        table_dict={}
        for i in range(len(master_cols_name)):
            table_dict[master_cols_name[i]]=master_cols_value[i]
        for element in model_selection_condition:#模型选择集合
            if isinstance(element, str):
                element=json.loads(element)
            model_name=list(element.keys())[0]
            model_conditions=list(element.values())[0]
            for condition in model_conditions:#为list集合,只要满足一个条件即可确定模型
                if isinstance(condition, str):
                    condition=json.loads(condition)
                meet_condition_list=[]#是否满足需求
                for key,value in condition.items():
                        if key in table_dict.keys():
                            table_value=table_dict[key]
                        else:
                            meet_condition_list.append(-1)
                            break
                        if key in bound_cols:
                            meet_condition=judge_bound_value(table_value,value)
                            meet_condition_list.append(meet_condition)
                        else:
                            if value_modify(table_value)!=value_modify(value):
                                meet_condition=-1
                            else:
                                meet_condition=1
                            meet_condition_list.append(meet_condition)
                if -1 not in meet_condition_list:
                    apply_model=model_name
                    break
        return apply_model
    return udf(model_selection_func,StringType())

def model_selection(spark,param):
    """
    模型选择
    :param spark:
    :param param: 参数
    :return: 包含模型选择结果的数据 PS：结果列：apply_model
    """
    source_data=get_source_data(spark, param)#获取源数据
    bound_cols=param['bound_cols']
    method_cols=param['method_cols']
    label_cols=param['label_cols']
    model_selection_condition=param['model_selection_condition']
    default_model=param['default_model']

    judge_cols=copy.copy(bound_cols)
    judge_cols.extend(method_cols)
    judge_cols.extend(label_cols)

    model_judge_udf(model_selection_condition,default_model, bound_cols)

    source_data=mrege_judge_cols(source_data,judge_cols)#根据要判断的列增加新的两列：列名合并和列值合并
    source_data=source_data.withColumn(
        'apply_model', model_judge_udf(model_selection_condition,default_model, bound_cols)\
        (source_data['judge_cols_name'],source_data['judge_cols_value'])
    )
    source_data=source_data.drop('judge_cols_name','judge_cols_value')
    return source_data
