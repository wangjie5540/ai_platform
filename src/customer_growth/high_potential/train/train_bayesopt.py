import os
from typing import Dict
# from unicodedata import category
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import KFold, train_test_split,cross_val_score
from sklearn.metrics import roc_auc_score
from bayes_opt import BayesianOptimization
import pickle

import time

import logging
import warnings
warnings.filterwarnings("ignore")

train_set = None

def process_feats(data, tag_name):
    labels = np.array(data[tag_name].astype(np.int32)).reshape((-1,))
    features = data.drop(['user_id',tag_name], axis=1)
    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.2)
    print('Train shape:', x_train.shape)
    print('Test shape:', x_test.shape)
    feats_train = np.array(x_train)
    feats_test = np.array(x_test)
    labels_train = y_train[:]
    labels_test = y_test[:]
    train_set = lgb.Dataset(feats_train, label=labels_train)
    return train_set, feats_train, labels_train, feats_test, labels_test



def lgb_cv(n_estimators, num_leaves, max_depth, learning_rate, reg_alpha, reg_lambda, bagging_fraction, bagging_freq, colsample_bytree):
    global train_set
    params = {
        'boosting_type': 'gbdt',
        'objective': 'binary',
        'verbosity': -1,
        'n_estimators': int(n_estimators),
        'num_leaves': int(num_leaves),
        'max_depth': int(max_depth),
        'learning_rate': learning_rate,
        'reg_alpha': reg_alpha,
        'reg_lambda': reg_lambda,
        'bagging_fraction': bagging_fraction,
        'bagging_freq': int(bagging_freq),
        'colsample_bytree': colsample_bytree,
    }
    cv_res = lgb.cv(params, train_set, num_boost_round=10000, nfold=3, early_stopping_rounds=100, metrics='auc', seed=42, verbose_eval=3000)
    best_score = np.max(cv_res['auc-mean'])
    return best_score

def train(dataset, output_file):
    global train_set
    start = time.time()
    train_set, feats_train, labels_train, feats_test, labels_test = process_feats(dataset, 'label')
    lgb_opt = BayesianOptimization(
        lgb_cv,
        {
            'n_estimators': (10, 200),
            'num_leaves': (2, 100),
            'max_depth': (1, 40),
            'learning_rate': (0.1, 1),
            'reg_alpha': (0.1, 1),
            'reg_lambda': (0.1, 1),
            'bagging_fraction': (0.5, 1),
            'bagging_freq': (1, 5),
            'colsample_bytree': (0.6, 1)
        }
    )

    lgb_opt.maximize()
    best_params = lgb_opt.max['params']
    best_params['n_estimators'] = int(best_params['n_estimators'])
    best_params['num_leaves'] = int(best_params['num_leaves'])
    best_params['max_depth'] = int(best_params['max_depth'])
    best_params['bagging_freq'] = int(best_params['bagging_freq'])
#             print(best_params)
    logging.info('Bayes_optimalization for best_params:')
    logging.info(best_params)
    best_lgb_model = lgb.LGBMClassifier(n_jobs=-1, boosting_type='gbdt', objective='binary', random_state=42, **best_params)
    best_lgb_model.fit(feats_train, labels_train)

    preds = best_lgb_model.predict_proba(feats_test)[:, 1]
    auc = roc_auc_score(labels_test, preds)
#             print('The best model from Bayes optimization scores {:.5f} AUC ROC on the test set.'.format(auc))

    # store the model
    pickle.dump(best_lgb_model, open(output_file, 'wb'))
    end = time.time()
    print(f'Time of running:{end-start}')



