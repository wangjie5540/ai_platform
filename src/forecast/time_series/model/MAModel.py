# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    MA
"""
from statsmodels.tsa.arima.model import ARIMA

class MAModel():
    def __init__(self,data,param,param_fit,q):
        self.data = data
        self.param = param
        self.param_fit = param_fit
        self.p = 0 #设置为0，区分与arima ar模型
        self.q = q #ma模型参数

        param = {
            "exog": None,
            "order": (self.p, 0, self.q),
            "seasonal_order": (0, 0, 0,0),
            "trend": None,
            "enforce_stationarity": True,
            "enforce_invertibility": True,
            "concentrate_scale": False,
            "trend_offset": 1,
            "dates": None,
            "freq": None,
            "missing": 'none',
            "validate_specification": True
        }
        param.update(self.param)

        self.model = ARIMA(self.data,**param)

    def fit(self):
        param_fit = {
            "start_params": None,
            "transformed": True,
            "includes_fixed": False,
            "method": None,
            "method_kwargs": None,
            "gls": None,
            "gls_kwargs": None,
            "cov_type": None,
            "cov_kwds": None,
            "return_params": False,
            "low_memory": False
        }
        param_fit.update(param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model

    def forecast(self,predict_len):
        pred = self.model.forecast(predict_len)
        return pred



