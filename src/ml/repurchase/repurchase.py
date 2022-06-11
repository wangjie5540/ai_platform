import os
from typing import Dict
import json
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from bayes_opt import BayesianOptimization
import pickle
from create_dataset import CreateDataset
import time
import logging
import traceback
import warnings
warnings.filterwarnings("ignore")

train_set = None

def process_feats(data, tag_name):
    labels = np.array(data[tag_name].astype(np.int32)).reshape((-1,))
    features = data.iloc[:, 1:].drop([tag_name], axis=1)
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
    """
    贝叶斯优化的目标函数
    :param n_estimators:
    :param num_leaves:
    :param max_depth:
    :param learning_rate:
    :param reg_alpha:
    :param reg_lambda:
    :param bagging_fraction:
    :param bagging_freq:
    :param colsample_bytree:
    :return:
    """
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

def train(input_params:Dict):
    """
    :param input_params: 输入配置参数
    {"taskid":52,
      "category":["蔬菜","水果"],
      "userData":
          {"is_consume_online":"",
           "is_new":"",
           "city":"",
           "sex":"sex",
           "consume_level":"",
           "yuliu_id":"",
           "tableName":"labelx.push_user",
           "dt":"dt",
           "recent_view_day":"recent_view_day",
           "province":"",
           "user_id":"user_id",
           "online_signup_time":"",
           "age":"age"},
      "trafficData":
          {"cart_remove":"",
           "cart_add":"",
           "click":"CLICK",
           "tableName":"labelx.push_traffic_behavior",
           "duration":"duration",
           "search":"",
           "exposure":"EXPOSURE",
           "card_add":"CART_ADD",
           "user_id":"user_id",
           "event_code":"event_code",
           "sku":"sku",
           "collect":"COLLECT",
           "event_time":"event_time",
           "browse":""},
      "orderData":
          {"user_id":"user_id",
           "order_time":"order_time",
           "sku":"sku",
           "order_id":"order_id",
           "sale_quantity":"",
           "sale_amount":"",
           "tableName":"labelx.push_order_behavior"},
      "goodsData":
          {"dt":"dt",
           "cate":"cate",
           "sku":"sku",
           "tableName":"labelx.push_goods"},
      "trainingScope":"过去60天",
      "forecastPeriod":"未来60天",
      "eventCode":
          {"event_code":
               {"search":"",
                "cart_remove":"",
                "exposure":"EXPOSURE",
                "cart_add":"",
                "click":"CLICK",
                "collect":"COLLECT",
                "browse":""}}}
    :return:
    """
    global train_set
    try:
        start = time.time()
        create_data = CreateDataset()
        cate_list = []
        for cateid in input_params['category']:
            cate = '"'+cateid+'"'
            cate_list.append(cate)
        catestr = "("+",".join(cate_list)+")"


        dataset = create_data.ConstructFeatures(
                                    input_params['trainingScope'],
                                    input_params['forecastPeriod'],
                                    catestr,
                                    True,
                                    input_params['orderData']['tableName'],
                                    input_params['trafficData']['tableName'],
                                    input_params['userData']['tableName'],
                                    input_params['goodsData']['tableName'],
                                    input_params['orderData'],
                                    input_params['trafficData'],
                                    input_params['userData'],
                                    input_params['goodsData'],
                                    input_params['eventCode'][input_params['trafficData']['event_code']],
                                     None)
        if len(dataset) == 0:#可合并
            return

        # 暂时处理
        if len(dataset[dataset['label']==1]) == len(dataset):
            dataset.loc[:len(dataset)//2,'label'] = 0
        elif len(dataset[dataset['label']==0]) == len(dataset):
            dataset.loc[:len(dataset)//2,'label'] = 1

        if (len(dataset[dataset['label']==0]) == len(dataset)) or (len(dataset[dataset['label']==1]) == len(dataset)):
            return
        else:
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
            print('The best model from Bayes optimization scores {:.5f} AUC ROC on the test set.'.format(auc))

            # store the model
            filepath = str(input_params['taskid'])+r".pkl"
            print(filepath)
            pickle.dump(best_lgb_model, open(filepath, 'wb'))
            end = time.time()
            return 'success'
    except:
        logging.error('Raise errors when response to platform',exc_info=1)
        return
