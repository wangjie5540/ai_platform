# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    Lightgbm模型
"""

import lightgbm as gbm

class LightgbmModel():

    def __init__(self,train_x,train_y,param=None):
        self.train_x=train_x
        self.train_y=train_y
        self.param=param

        boosting_type=self.param['boosting_type']
        objective=self.param['objective']
        importance_type=self.param['importance_type']
        n_jobs=self.param['n_jobs']
        subsample=self.param['subsample']
        learning_rate =self.param['learning_rate']
        n_estimators= self.param['n_estimators']
        random_seed=self.param['random_seed']
        metric=self.param['metric'] # 评估函数
        feature_fraction=self.param['feature_fraction'] # 建树的特征选择比例
        max_depth=self.param['max_depth']
        reg_alpha=self.param['reg_alpha']

        lgb_model=gbm.LGBMRegressor(
            boosting_type=boosting_type,
            objective=objective,
            importance_type=importance_type,
            n_jobs=n_jobs,
            subsample=subsample,
            learning_rate=learning_rate,
            feature_fraction=feature_fraction,
            n_estimators=n_estimators,
            random_seed=random_seed,
            metric=metric,
            max_depth=max_depth,
            reg_alpha=reg_alpha
        )
        self.model=lgb_model

    def fit(self):
        self.model=self.model.fit(self.train_x,self.train_y)
        return self.model

    def forcast(self,predict_x):
        preds=self.model.predict(predict_x)
        return preds