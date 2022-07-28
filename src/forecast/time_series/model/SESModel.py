# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    SimpleExpSmoothing
"""

from statsmodels.tsa.holtwinters import SimpleExpSmoothing

class SESModel():
    def __init__(self,data,param,param_fit):
        self.data = data
        self.param = param
        self.param_fit = param_fit

        param = {
        "initialization_method": "legacy-heuristic",
        "initial_level": None
        }

        param.update(self.param)
        self.model = SimpleExpSmoothing(self.data,**param)

    def fit(self):
        param_fit={
           "smoothing_level": None,
            "optimized": True,
            "start_params": None,
            "initial_level": None,
            "use_brute": True,
            "use_boxcox": None,
            "remove_bias": False,
            "method": None,
            "minimize_kwargs": None
        }
        param_fit.update(self.param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model

    def forecast(self,predict_len):
        pred = self.model.forecast(predict_len)
        return pred











