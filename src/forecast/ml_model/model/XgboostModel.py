# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Xgboost模型
"""

import xgboost as xgb

class XgboostModel():

    def __init__(self,train_x,train_y,param=None):
        self.train_x=train_x
        self.train_y=train_y
        self.param=param

        max_depth=self.param['max_depth']
        n_estimators=self.param['n_estimators']
        learning_rate=self.param['learning_rate']
        random_state=self.param['random_state']
        seed=self.param['seed']
        subsample=self.param['subsample']
        colsample_bytree=self.param['colsample_bytree']
        reg_lambda=self.param['reg_lambda']
        gamma=self.param['gamma']

        xgb_model=xgb.XGBRegressor(max_depth=max_depth,
                                   n_estimators=n_estimators,
                                   learning_rate=learning_rate,
                                   random_state=random_state,
                                   seed=seed,
                                   subsample=subsample,
                                   colsample_bytree=colsample_bytree,
                                   reg_lambda=reg_lambda,
                                   gamma=gamma)
        self.model=xgb_model

    def fit(self):
        self.model=self.model.fit(self.train_x,self.train_y)
        return self.model

    def forcast(self,predict_x):
        preds=self.model.predict(predict_x)
        return preds