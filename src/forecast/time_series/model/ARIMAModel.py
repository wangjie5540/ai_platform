# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    ARIMA
"""
from statsmodels.tsa.arima.model import ARIMA

class ARIMAModel():
    def __init__(self,data,param,param_fit):
        self.data = data
        self.param = param
        self.param_fit = param_fit

        param = {
            "exog": None,
            "order": (0, 0, 0),
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




