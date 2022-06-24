# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    价格弹性模型
"""
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols


class PriceElasticity(object):

    def __init__(self, sdate, edate):
        self.sdate = sdate
        self.edate = edate
        pass

    def linear_model(self, data):
        data = data[(data['sdt'] <= self.edate) & (data['sdt'] >= self.sdate)]
        model = ols("sales_qty ~ price+predict", data=data).fit()
        print(model.summary())
        # todo: 返回的是模型还是价格弹性参数
        return None

    def log_log_model(self, data):
        data = data[(data['sdt'] <= self.edate) & (data['sdt'] >= self.sdate)]
        data['log_sales_qty'] = np.log(data['sales_qty'])
        data['log_price'] = np.log(data['price'])
        data['log_predict'] = np.log(data['predict'])
        model = ols("log_sales_qty ~ log_price+log_predict", data=data).fit()
        print(model.summary())
        # todo: 返回的是模型还是价格弹性参数
        return None

    def cross_price_elasticity_model(self):
        pass
