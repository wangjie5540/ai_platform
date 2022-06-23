# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Holt-winter
"""

from statsmodels.tsa.api import ExponentialSmoothing

class HoltWinterModel():

    def __init__(self,data,param=None,param_fit=None):

        self.data=data
        self.param=param
        self.param_fit = param_fit

        default_param = {
            "trend": None,
            "damped_trend": False,
            "seasonal": None,
            "seasonal_periods": None,
            "initialization_method": "estimated",
            "inittial_level": None,
            "inittial_trend": None,
            "initial_seasonal": None,
            "use_boxcox": False,
            "bounds": None,
            "freq": None,
            "missing": "none",
            "dates": None
        }.update(self.param)

        seasonal_periods = default_param['seasonal_periods']
        trend = default_param["trend"]
        seasonal = default_param["seasonal"]
        damped_trend = default_param["damped_trend"]
        use_boxcox = default_param["use_boxcox"]
        initialization_method = default_param["initialization_method"]
        missing = default_param["missing"]
        freq = default_param["freq"]


        self.model = ExponentialSmoothing(
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

        default_fit_param = {
            "smoothing_level": None,
            "smoothing_trend": None,
            "smoothing_seasonal":None,
            "damping_trend": None,
            "optimized":True,
            "remove_bias": False,
            "start_params": None,
            "method": None,
            "minimize_kwargs": None,
            "use_brute": True,
            "use_boxcox": None,
            "use_basinhopping": None,
            "initial_level": None,
            "initial_trend":None
        }.update(self.param_fit)

        smoothing_level = default_fit_param["smoothing_level"]
        smoothing_trend = default_fit_param["smoothing_trend"]
        smoothing_seasonal = default_fit_param["smoothing_seasonal"]
        damping_trend = default_fit_param["damping_trend"]
        optimized = default_fit_param["optimized"]
        remove_bias = default_fit_param["remove_bias"]
        start_params = default_fit_param["start_params"]
        method = default_fit_param["method"]
        minimize_kwargs = default_fit_param["minimize_kwargs"]
        use_brute = default_fit_param["use_brute"]
        use_boxcox = default_fit_param["use_boxcox"]
        use_basinhopping = default_fit_param["use_basinhopping"]
        initial_level = default_fit_param["initial_level"]
        initial_trend = default_fit_param["initial_trend"]

        self.model=self.model.fit(smoothing_level,smoothing_trend,smoothing_seasonal,damping_trend,optimized,remove_bias,
                                  start_params,method,minimize_kwargs,use_brute,use_boxcox,
                                  use_basinhopping,initial_level,initial_trend)
        return self.model
        
    def forcast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds

