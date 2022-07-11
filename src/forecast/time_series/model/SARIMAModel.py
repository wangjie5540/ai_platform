# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    SARIMA
"""

from statsmodels.tsa.statespace.sarimax import SARIMAX

class SARIMAModel():
    def __init__(self,data,param,param_fit):
        self.param = param
        self.param_fit = param_fit
        self.data = data

        param={
            "exog": None,
            "order": (1, 0, 0),
            "seasonal_order": (0, 0, 0,0),
            "trend": None,
            "measurement_error": False,
            "time_varying_regression": False,
            "mle_regression": True,
            "simple_differencing": False,
            "enforce_stationarity": True,
            "enforce_invertibility": True,
            "hamilton_representation": False,
            "concentrate_scale": False,
            "trend_offset": 1,
            "use_exact_diffuse": False,
            "dates": None,
            "freq": None,
            "missing": 'none',
            "validate_specification": True
        }
        param.update(self.param)

        self.model = SARIMAX(self.data,**param)

    def fit(self):
        param_fit={
            "start_params": None,
            "transformed": True,
            "includes_fixed": False,
            "cov_type": None,
            "cov_kwds": None,
            "method": 'lbfgs',
            "maxiter": 50,
            "full_output": 1,
            "disp": 5,
            "callback": None,
            "return_params": False,
            "optim_score": None,
            "optim_complex_step": None,
            "optim_hessian": None,
            "flags": None,
            "low_memory": False
        }
        param_fit.update(self.param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model

    def forecast(self,predict_len):
        pred = self.model.forecast(predict_len)
        return pred



