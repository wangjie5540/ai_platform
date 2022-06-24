# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    统计模型：bootstrap
"""
import random
import pandas as pd
import numpy as np

class BootStrapModel():

    def __init__(self,data,param=None):
        self.data=data
        self.param=param

    def allocate_weights(self,size,decay):
        """
        设置衰减权重
        :param size: 数据长度
        :param decay: 衰减率
        :return: 数组内各元素的权重
        """
        weights=[]
        for i in range(size-1,-1,-1):
            weights.append(decay ** i)
        return weights

    def resample(self,data,n,weights=None, decay=1.0):
        """
        批量重抽样
        :param data:样本
        :param n:抽样个数
        :param weights:权重
        :param decay:衰减率
        :return:抽取的样本
        """
        if weights is None:
            weights=self.allocate_weights(len(data),decay)
        return random.choices(data,weights=weights,k=n)

    def sampleperiod_every_day(self,individual,period,n=1000,decay=0.98):
        """
        销量抽样,days天每天的销量
        :param individual:单日数量（数组）
        :param period:天数
        :param n:抽样次数
        :param decay:历史数据衰减率
        :return:抽样未来每天的样本
        """
        samples=[]
        weights=self.allocate_weights(len(individual),decay)  #越往后,被选择的权重越小
        if type(period) not in [list, pd.DataFrame,pd.Series, np.ndarray]:
            for i in range(0, n):
                samples.append(self.resample(individual, int(period), weights=weights))
        else:
            days=self.resample(period, n, decay=decay)
            for d in days:
                samples.append(self.resample(individual, int(d), weights=weights))
        return samples
    
    def fit(self):
        return None

    def forcast(self,predict_len):
        """
        bootstrap抽样模型
        :param predict_len: 预测时长
        :return: 预测值
        """
        n=self.param['n']
        decay=self.param['decay']
        demand=self.sampleperiod_every_day(self.data.dropna().values,predict_len, n,decay)
        demand=np.array(demand)
        f=demand.mean(axis=0)
        preds=f.tolist()
        return preds
    