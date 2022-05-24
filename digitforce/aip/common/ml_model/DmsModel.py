# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    DMS算法
"""
import pandas as pd
import numpy as np

class DmsModel():

    def __init__(self,data,param=None):
        self.data=data
        self.param=param

        self.model=None

    def fit(self):
        return None

    def forcast(self,predict_len):
        if isinstance(self.data,pd.DataFrame):
            preds=self.data.mean()[0]
            return preds
        else:
            #目前：使用过去均值，正常：dms=当日销量*0.2+昨日dms*0.8，问题：预测时无当日销量
            preds=[np.mean(self.data) for i in range(predict_len)]
        return preds