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
import datetime
from construct_features_v2 import CreateDataset
import time
from utils import *
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

def size_format(size):
    if size < 1024:
        return '%i' % size + 'B'
    elif 1024 <= size < (1024 ** 2):
        return '%.1f' % float(size/(1024 ** 1)) + 'KB'
    elif (1024 ** 2) <= size < (1024 ** 3):
        return '%.1f' % float(size/(1024 ** 2)) + 'MB'
    elif (1024 ** 3) <= size < (1024 ** 4):
        return '%.1f' % float(size/(1024 ** 3)) + 'GB'
    elif (1024 ** 4) <= size:
        return '%.1f' % float(size/(1024 ** 4)) + 'TB'

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

def train(input_params:Dict, path):
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
        if len(dataset)==0:#可合并
            return {'generateTime':"", "taskId":input_params['taskid'], 'auc':"", "fileSize":"", 'elapsed':0, "fileDownloadUrl":"-1"}

        # 暂时处理
        if len(dataset[dataset['label']==1]) == len(dataset): 
            dataset.loc[:len(dataset)//2,'label'] = 0
        elif len(dataset[dataset['label']==0]) == len(dataset):
            dataset.loc[:len(dataset)//2,'label'] = 1

        if (len(dataset[dataset['label']==0]) == len(dataset)) or (len(dataset[dataset['label']==1]) == len(dataset)):
            return {'generateTime':"", "taskId":input_params['taskid'], 'auc':"", "fileSize":"", 'elapsed':0, "fileDownloadUrl":"-1"}
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
#             print('The best model from Bayes optimization scores {:.5f} AUC ROC on the test set.'.format(auc))

            # store the model
            outputpath = os.path.join(path,"model")
            fname=str(input_params['taskid'])+r".pkl"
            filepath = os.path.join(outputpath,fname)
            pickle.dump(best_lgb_model, open(filepath, 'wb'))
            end = time.time()
            cur_time = datetime.datetime.now()
            statinfo = os.stat(filepath)

            # upload model and parameters
            model_target_file_path = os.path.join('hdfs:///usr/algorithm/cd/fugou/model', str(input_params['taskid'])+'.pkl')
            upload_flag = upload_hdfs(filepath, model_target_file_path)
            if upload_flag:
                model_file_url = upload_flag
                os.remove(filepath)
            else:
                model_file_url = '-1'



            res = {}
            res['generateTime'] = datetime.datetime.strftime(cur_time,'%Y-%m-%d %H:%M:%S')
            res['taskId'] = input_params['taskid']
            res['auc'] = "%.2f%%" % (auc * 100)
            res['fileSize'] = size_format(statinfo.st_size)
            res['elapsed'] = int(end - start)
            res['fileDownloadUrl'] = model_file_url
            return res
    except:
        logging.error('Raise errors when response to platform',exc_info=1)
        return {'generateTime':"", "taskId":input_params['taskid'], 'auc':"", "fileSize":"", 'elapsed':0, "fileDownloadUrl":"-1"}
