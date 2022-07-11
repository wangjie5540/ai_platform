# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    SimpleExpSmoothing
"""

from statsmodels.tsa.ar_model import AutoReg

class ARModel():
    def __init__(self,data,param,param_fit):
        self.data = data
        self.param = param
        self.param_fit = param_fit

        param={
            "trend": 'c',
            'lags':None,
            "seasonal": False,
            "exog": None,
            "hold_back": None,
            "period": None,
            "missing":'none',
            "deterministic": None,
            "old_names": False
        }
        param.update(self.param)

        self.model = AutoReg(self.data,**param)


    def fit(self):
        param_fit = {
            "cov_type": 'nonrobust',
            "cov_kwds": None,
            "use_t": False
        }
        param_fit.update(self.param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model

    def forecast(self,predict_len):
        pred = self.model.forecast(predict_len)
        return pred






