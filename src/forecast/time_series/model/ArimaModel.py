# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    arima模型
"""
from pmdarima.arima import auto_arima

class ArimaModel():
    
    def __init__(self,data,param=None):
        self.data = data
        self.param = param
        
        start_p=self.param['start_p']
        start_q=self.param['start_q']
        max_p=self.param['max_p']
        max_q=self.param['max_q']
        m=self.param['m']
        seasonal=self.param['seasonal']
        d=self.param['d']
        trace=self.param['trace']
        suppress_warnings=self.param['suppress_warnings']
        stepwise=self.param['stepwise']

        self.model=auto_arima(data,start_p=start_p, start_q=start_q, max_p=max_p, max_q=max_q, m=m,
                           seasonal=seasonal, d=d, trace=trace, error_action='ignore',
                           suppress_warnings=suppress_warnings, stepwise=stepwise)
        
    def fit(self):
        return None
    
    def forcast(self,predict_len):
        preds=self.model.predict(predict_len)
        return preds
    