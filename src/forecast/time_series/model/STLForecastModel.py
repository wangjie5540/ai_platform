# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    STLforecast
"""

from statsmodels.tsa.api import STLForecast

class STLFModel():
    def __init__(self,data,param,param_fit):
        self.data = data
        self.param = param
        self.param_fit = param_fit

        param = {
            "model":"ARIMA",
            "period": None,
            "seasonal": 7,
            "trend": None,
            "low_pass": None,
            "seasonal_deg": 1,
            "trend_deg": 1,
            "low_pass_deg": 1,
            "robust": False,
            "seasonal_jump": 1,
            "trend_jump": 1,
            "low_pass_jump": 1
        }
        param.update(self.param)

        self.model = STLForecast(self.data,**param)

    def fit(self):
        param_fit={
            "inner_iter": None,
            "outer_iter": None
        }
        param_fit.update(self.param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model

    def forecast(self,predict_len):
        pred = self.model.forecast(predict_len)
        return pred
