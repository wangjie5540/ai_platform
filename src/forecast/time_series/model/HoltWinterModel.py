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

        if self.param['seasonal_periods'] is not None:
            if self.param['trend'] is not None:
                if self.param['seasonal'] is not None:
                    if self.param['damped_trend'] is not None:
                        if self.param['use_boxcox'] is not None:
                            if self.param['initialization_method'] is not None:
                                if self.param['missing'] is not None:
                                    if self.param['freq'] is not None:
                                        self.model = ExponentialSmoothing(
                                            self.data,
                                            seasonal_periods=self.param['seasonal_periods'],
                                            trend=self.param['trend'],
                                            seasonal=self.param['seasonal'],
                                            damped_trend=self.param['damped_trend'],
                                            use_boxcox=self.param['use_boxcox'],
                                            initialization_method=self.param['initialization_method'],
                                            missing=self.param['missing'],
                                            freq=self.param['freq']
                                        )
                                    else:
                                        self.model = ExponentialSmoothing(
                                            self.data,
                                            seasonal_periods=self.param['seasonal_periods'],
                                            trend=self.param['trend'],
                                            seasonal=self.param['seasonal'],
                                            damped_trend=self.param['damped_trend'],
                                            use_boxcox=self.param['use_boxcox'],
                                            initialization_method=self.param['initialization_method'],
                                            missing=self.param['missing']
                                        )
                                else:
                                    self.model = ExponentialSmoothing(
                                        self.data,
                                        seasonal_periods=self.param['seasonal_periods'],
                                        trend=self.param['trend'],
                                        seasonal=self.param['seasonal'],
                                        damped_trend=self.param['damped_trend'],
                                        use_boxcox=self.param['use_boxcox'],
                                        initialization_method=self.param['initialization_method']
                                    )
                            else:
                                self.model = ExponentialSmoothing(
                                    self.data,
                                    seasonal_periods=self.param['seasonal_periods'],
                                    trend=self.param['trend'],
                                    seasonal=self.param['seasonal'],
                                    damped_trend=self.param['damped_trend'],
                                    use_boxcox=self.param['use_boxcox']
                                )
                        else:
                            self.model = ExponentialSmoothing(
                                self.data,
                                seasonal_periods=self.param['seasonal_periods'],
                                trend=self.param['trend'],
                                seasonal=self.param['seasonal'],
                                damped_trend=self.param['damped_trend']
                            )
                    else:
                        self.model = ExponentialSmoothing(
                            self.data,
                            seasonal_periods=self.param['seasonal_periods'],
                            trend=self.param['trend'],
                            seasonal=self.param['seasonal']
                        )
                else:
                    self.model = ExponentialSmoothing(
                        self.data,
                        seasonal_periods=self.param['seasonal_periods'],
                        trend=self.param['trend']
                    )
            else:
                self.model = ExponentialSmoothing(
                    self.data,
                    seasonal_periods=self.param['seasonal_periods']
                )
        else:
            self.model = ExponentialSmoothing(
                self.data
            )


    def fit(self):
        self.model=self.model.fit()
        return self.model
        
    def forcast(self,predict_len):
        preds=self.model.forecast(predict_len)
        return preds