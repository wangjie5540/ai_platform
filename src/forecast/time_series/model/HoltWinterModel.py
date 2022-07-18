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
                 # ,param_fit=None):

        self.data=data
        self.param=param
        # self.param_fit = param_fit

        param = {
            "trend": None,
            "damped_trend": False,
            "seasonal": None,
            "seasonal_periods": None,
            "initialization_method": "estimated",
            "initial_level": None,
            "initial_trend": None,
            "initial_seasonal": None,
            "use_boxcox": False,
            "bounds": None,
            "freq": None,
            "missing": "none",
            "dates": None
        }
        param.update(self.param)

        self.model = ExponentialSmoothing(
            self.data,
            **param
        )


    def fit(self):

        # param_fit = {
        #     "smoothing_level": None,
        #     "smoothing_trend": None,
        #     "smoothing_seasonal":None,
        #     "damping_trend": None,
        #     "optimized":True,
        #     "remove_bias": False,
        #     "start_params": None,
        #     "method": None,
        #     "minimize_kwargs": None,
        #     "use_brute": True,
        #     "use_boxcox": None,
        #     "use_basinhopping": None,
        #     "initial_level": None,
        #     "initial_trend":None
        # }
        # param_fit.update(self.param_fit)

        self.model=self.model.fit()
            # **param_fit)
        return self.model
        
    def forecast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds

