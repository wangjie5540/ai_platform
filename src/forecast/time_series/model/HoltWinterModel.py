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

        seasonal_periods=self.param['seasonal_periods'] if self.param['seasonal_periods'] is not None else None
        trend=self.param['trend'] if self.param['trend'] is not None else None
        seasonal=self.param['seasonal'] if self.param['seasonal'] is not None else None
        damped_trend=self.param['damped_trend'] if self.param['damped_trend'] is not None else None
        use_boxcox=self.param['use_boxcox'] if self.param['use_boxcox'] is not None else None
        initialization_method=self.param['initialization_method'] if self.param['initialization_method'] is not None else None
        missing=self.param['missing'] if self.param['missing'] is not None else None
        freq=self.param['freq'] if self.param['freq'] is not None else None

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
                                            seasonal_periods=seasonal_periods,
                                            trend=trend,
                                            seasonal=seasonal,
                                            damped_trend=damped_trend,
                                            use_boxcox=use_boxcox,
                                            initialization_method=initialization_method,
                                            missing=missing,
                                            freq=freq
                                        )
                                    else:
                                        self.model = ExponentialSmoothing(
                                            self.data,
                                            seasonal_periods=seasonal_periods,
                                            trend=trend,
                                            seasonal=seasonal,
                                            damped_trend=damped_trend,
                                            use_boxcox=use_boxcox,
                                            initialization_method=initialization_method,
                                            missing=missing
                                        )
                                else:
                                    self.model = ExponentialSmoothing(
                                        self.data,
                                        seasonal_periods=seasonal_periods,
                                        trend=trend,
                                        seasonal=seasonal,
                                        damped_trend=damped_trend,
                                        use_boxcox=use_boxcox,
                                        initialization_method=initialization_method
                                    )
                            else:
                                self.model = ExponentialSmoothing(
                                    self.data,
                                    seasonal_periods=seasonal_periods,
                                    trend=trend,
                                    seasonal=seasonal,
                                    damped_trend=damped_trend,
                                    use_boxcox=use_boxcox
                                )
                        else:
                            self.model = ExponentialSmoothing(
                                self.data,
                                seasonal_periods=seasonal_periods,
                                trend=trend,
                                seasonal=seasonal,
                                damped_trend=damped_trend
                            )
                    else:
                        self.model = ExponentialSmoothing(
                            self.data,
                            seasonal_periods=seasonal_periods,
                            trend=trend,
                            seasonal=seasonal
                        )
                else:
                    self.model = ExponentialSmoothing(
                        self.data,
                        seasonal_periods=seasonal_periods,
                        trend=trend
                    )
            else:
                self.model = ExponentialSmoothing(
                    self.data,
                    seasonal_periods=seasonal_periods
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