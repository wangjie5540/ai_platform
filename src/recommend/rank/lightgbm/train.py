import logging
import os
import time
import datetime

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, recall_score
import lightgbm as lgb
import numpy as np
from bayes_opt import BayesianOptimization

train_set = None


def process_feats(data, tag_name, test_size=0.2):
    global train_set
    labels = np.array(data[tag_name].astype(np.int32)).reshape((-1,))
    features = data.drop([tag_name], axis=1)
    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=test_size)

    feats_train = np.array(x_train)
    feats_test = np.array(x_test)
    labels_train = y_train[:]
    labels_test = y_test[:]
    train_set = lgb.Dataset(feats_train, label=labels_train)
    return train_set, feats_train, labels_train, feats_test, labels_test


def lgb_cv(n_estimators, num_leaves, max_depth, learning_rate, reg_alpha, reg_lambda,
           bagging_fraction, bagging_freq, colsample_bytree):
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
    cv_res = lgb.cv(params, train_set, num_boost_round=10000, nfold=3, early_stopping_rounds=100, metrics='auc',
                    seed=42, verbose_eval=3000)
    best_score = np.max(cv_res['auc-mean'])
    return best_score


def train(file_path, model_path):
    global train_set
    try:
        start = time.time()

        dataset = pd.read_csv(file_path)

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

        logging.info('Bayes_optimalization for best_params:')
        logging.info(best_params)
        best_lgb_model = lgb.LGBMClassifier(n_jobs=-1, boosting_type='gbdt', objective='binary', random_state=42,
                                            **best_params)
        best_lgb_model.fit(feats_train, labels_train)
        preds = best_lgb_model.predict_proba(feats_test)[:, 1]
        auc = roc_auc_score(labels_test, preds)
        recall = recall_score(labels_test, np.around(preds, 0).astype(int))
        logging.info('model_path: ', model_path)
        model_name = f'lgb__auc_{auc:.2f}__recall_{recall:.2f}.txt'
        logging.info('model_name: ', model_name)
        logging.info(f"auc: {auc}, recall: {recall}")
        model_path = os.path.join(model_path, model_name)
        logging.info('join_model_path: ', model_path)
        dir_name = os.path.dirname(model_path)
        logging.info(f"dir_name:{dir_name}")
        if dir_name and not os.path.exists(dir_name):
            logging.info(f"mkdir -p {dir_name}")
            os.system(f"mkdir -p {dir_name}")

        logging.info(f"save model to {model_path}")
        best_lgb_model.booster_.save_model(model_path)
        end = time.time()
        logging.info(f"auc: {auc}, time: {end-start}")
    except:
        logging.error('Raise errors when response to platform', exc_info=1)
