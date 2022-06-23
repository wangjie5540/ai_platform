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

        trend = None
        damped_trend = False
        seasonal = None
        seasonal_periods = None
        initialization_method = "estimated"
        inittial_level = None
        inittial_trend = None
        initial_seasonal = None
        use_boxcox = False
        bounds = None
        freq = None
        missing = "none"
        dates = None

        if self.param['seasonal_periods'] is not None:
            seasonal_periods = self.param['seasonal_periods']
        if self.param['trend'] is not None:
            trend = self.param['trend']
        if self.param['seasonal'] is not None:
            seasonal = self.param['seasonal']
        if self.param['damped_trend'] is not None:
            damped_trend = self.param['damped_trend']
        if self.param['use_boxcox'] is not None:
            use_boxcox = self.param['use_boxcox']
        if self.param['initialization_method'] is not None:
            initialization_method = self.param['initialization_method']
        if self.param['missing'] is not None:
            missing = self.param['missing']
        if self.param['freq'] is not None:
            freq = self.param['freq']

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
        smoothing_level = None
        smoothing_trend = None
        smoothing_seasonal = None
        damping_trend = None
        optimized = True
        remove_bias = False
        start_params = None
        method = None
        minimize_kwargs = None
        use_brute = True
        use_boxcox = None
        use_basinhopping = None
        initial_level = None
        initial_trend = None
        if self.param['smoothing_level'] is not None:
            smoothing_level = self.param['smoothing_level']
        if self.param['smoothing_trend'] is not None:
            smoothing_trend = self.param['smoothing_trend']
        if self.param['smoothing_seasonal'] is not None:
            smoothing_seasonal = self.param['smoothing_seasonal']
        if self.param['damping_trend'] is not None:
            damping_trend = self.param['damping_trend']
        if self.param['optimized'] is not None:
            optimized = self.param['optimized']
        if self.param['remove_bias'] is not None:
            remove_bias = self.param['remove_bias']
        if self.param['start_params'] is not None:
            start_params = self.param['start_params']
        if self.param['method'] is not None:
            method = self.param['method']
        if self.param['minimize_kwargs'] is not None:
            minimize_kwargs = self.param['minimize_kwargs']
        if self.param['use_brute'] is not None:
            use_brute = self.param['use_brute']
        if self.param['use_boxcox'] is not None:
            use_boxcox = self.param['use_boxcox']
        if self.param['use_basinhopping'] is not None:
            use_basinhopping = self.param['use_basinhopping']
        if self.param['initial_level'] is not None:
            initial_level = self.param['initial_level']
        if self.param['initial_trend'] is not None:
            initial_trend = self.param['initial_trend']

        self.model=self.model.fit(smoothing_level,smoothing_trend,smoothing_seasonal,damping_trend,optimized,remove_bias,start_params,method,minimize_kwargs,use_brute,use_boxcox,use_basinhopping,initial_level,initial_trend)
        return self.model
        
    def forcast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds