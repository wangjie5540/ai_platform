# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    分组方法
"""
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

# 暂时保留，方便代码review时理解逻辑
# grouping_condition={
#     'lightgbm':{
#         'same_amount':{'amount':100,'partition_col':[]},
#         'bound':{'storage_life':[10,30,50],'sale_days':[40,90,10]},
#         'label':['shop_id','sale_classify']
#            }
#  }

def grouping_same_amount(data,amount,partition_col=None):
    """
    等分组
    :param data:数据
    :param amount: 分组数目
    :param partition_col: 依据的列，默认为空
    :return: 分组后的值
    """
    if partition_col==None:
        partition_col=[]
    partition_col.append('group_category')#默认分组
    w=Window.partitionBy(partition_col).orderBy(rand(seed=10000))#随机排序
    data=data.withColumn("group_category_new",(row_number().over(w))%amount)
    data=data.drop('group_category')
    data=data.withColumnRenamed('group_category_new','group_category')
    return data

def grouping_bound(data,grouping_bound):
    """
    根据bound进行分组
    :param data: 数据
    :param grouping_bound:分组对应的bound的值
    :return: 根据bound分组后的数据
    """
    output_cols=[]
    for key, value in grouping_bound.items():
        output='output_'+str(key)
        output_cols.append(output)
        splits=[-float("inf")]
        value=list(set(value))
        value.sort()
        splits.extend(value)
        splits.append(float("inf"))
        data=data.withColumn("key_label_tmp", data[key].cast(DoubleType()))
        bucketizer=Bucketizer(inputCol="key_label_tmp", outputCol=output, splits=splits, handleInvalid='keep')  # 进行分桶
        data=bucketizer.transform(data)
        data=data.drop("key_label_tmp")
    output_cols.append('group_category')#加上原来的分组
    data=grouping_label(data,output_cols)#根据label进行分组
    for col in output_cols:
        if col=='group_category':
            continue
        data=data.drop(col)
    return data

def group_category_transform_udf(group_category_dict):
    """
    对label产生的新分组转换形式
    :param group_category_dict: dict形式 key:原来的值，value：新的值
    :return: 注册函数
    """
    def group_category_transform(group_category_new):
        return str(group_category_dict[group_category_new])
    return udf(group_category_transform,StringType())

def grouping_label(data,grouping_label_cols):
    """
    根据label进行分组
    :param data: 数据
    :param grouping_label_cols:分组所用到的列
    :return: 依据列分组后的数据
    """
    data=data.withColumn(
        'group_category_new',
        concat_ws("&#&",*[col(c) for c in data.select(grouping_label_cols).columns])
    )
    group_category_new_list=data.select('group_category_new').distinct().toPandas()
    group_category_new_list=group_category_new_list.values.tolist()
    group_category_new_list_new=[i[0] for i in group_category_new_list]
    group_category_new_list_new.sort()#排序，防止运行多次结果不同
    group_category_dict={}
    for i in range(len(group_category_new_list_new)):
        group_category_dict[group_category_new_list_new[i]]=i
    data=data.withColumn('grouping_label_result',
                         group_category_transform_udf(group_category_dict)(data['group_category_new']))
    data=data.drop('group_category_new').drop('group_category')
    data=data.withColumnRenamed('grouping_label_result', 'group_category')
    return data

def group_category(data,grouping_condition):
    """
    分组策略
    :param data:数据
    :param grouping_condition:分组条件
    :return:
    """
    for apply,condition in grouping_condition.items():
        data_apply=data.select(data['apply_model']==apply)#参与分组的数据
        data_else=data.select(data['apply_model']!=apply)
        if 'same_amount' in condition.keys():
            same_amount_condition=condition['same_amount']
            if 'amount' in same_amount_condition.keys():
                amount=same_amount_condition['amount']
                if 'partition_col' in same_amount_condition.keys():
                    partition_col=same_amount_condition['partition_col']
                else:
                    partition_col=None
                data_apply=grouping_same_amount(data_apply,amount,partition_col)#等分组
        elif 'bound' in condition.keys():
            bound_condition=condition['bound']
            if len(bound_condition)>0:
                data_apply=grouping_bound(data_apply,bound_condition)#bound分组
        elif 'label' in condition.keys():
            label_condition=condition['label']
            if len(label_condition)>0:
                data_apply=grouping_label(data,label_condition)
        data=data_apply.unionAll(data_else)#最后进行合并

    return data