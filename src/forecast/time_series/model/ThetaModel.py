# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Theta
"""

from statsmodels.tsa.forecasting.theta import ThetaModel

class Theta_Model():
    def __init__(self,data,param,param_fit):
        self.data = data
        self.param = param
        self.param_fit = param_fit

        param = {
            "period": None,
            "deseasonalize": True,
            "use_test": True,
            "method": 'auto',
            "difference": False
        }
        param.update(self.param)

        self.model = ThetaModel(self.data,**param)

    def fit(self):
        param_fit={
            "use_mle": False,
            "disp": False
        }

        param_fit.update(self.param_fit)

        self.model = self.model.fit(**param_fit)

        return self.model



