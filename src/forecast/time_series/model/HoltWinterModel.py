# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Holt-winter
"""
from statsmodels.tsa.api import ExponentialSmoothing

class HoltWinterModel():

    def __init__(self,data,param=None):

        self.data=data
        self.param=param

        seasonal_periods=self.param['seasonal_periods']
        trend=self.param['trend']
        seasonal=self.param['seasonal']
        damped_trend=self.param['damped_trend']
        use_boxcox=self.param['use_boxcox']
        initialization_method=self.param['initialization_method']
        missing=self.param['missing']
        freq=self.param['freq']
        
        self.model=ExponentialSmoothing(
            self.data,
            seasonal_periods=seasonal_periods,
            trend=trend,
            seasonal=seasonal,
            damped_trend=damped_trend,
            use_boxcox=use_boxcox,
            initialization_method=initialization_method,
            missing=missing,
            freq=freq
        )
        
    def fit(self):
        self.model=self.model.fit()
        return self.model
        
    def forcast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds