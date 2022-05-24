# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    一次指数平滑
"""

from statsmodels.tsa.api import SimpleExpSmoothing

class ESModel():

    def __init__(self, data, param=None):
        self.data=data
        self.param=param

        self.model=SimpleExpSmoothing(self.data)
        
    def fit(self):
        
        smoothing_level=self.param['smoothing_level']
        optimized=self.param['optimized']
        
        self.model=self.model.fit(
            smoothing_level=smoothing_level,
            optimized=optimized)
        return self.model
        
    def forcast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds