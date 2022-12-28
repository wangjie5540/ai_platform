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
import pyhdfs
from param_test import hdfs_path
warnings.filterwarnings("ignore")
import digitforce.aip.common.utils.config_helper as config_helper
hdfs_config = config_helper.get_module_config("hdfs")



train_set = None

def upload_hdfs(filepath, target_file_path):
    try:
        cli = pyhdfs.HdfsClient(hosts="{}".format(hdfs_config['hosts']))
        if cli.exists(target_file_path):
            cli.delete(target_file_path)
        cli.copy_from_local(filepath, target_file_path)
        return target_file_path
    except:
        print(traceback.format_exc())
        return None

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

def train(input_params, solutionId):
    """
    :param input_params: 输入配置参数
    {"taskid":11,"category":["女装","男装","童装"],"userData":{"dt":"dt","city":"city_code","user_id":"vip_id","online_signup_time":"signup_date","sex":"sex_id","age":"age","tableName":"labelx.push_rpt_member_labels"},"trafficData":{"duration":"duration","exposure":"EXPOSURE","card_add":"BROWSE","user_id":"vip_id","event_code":"event_code","sku":"sku","collect":"EXPOSURE","click":"CLICK","event_time":"event_time","tableName":"labelx.push_event_vip_traffic"},"orderData":{"user_id":"vip_id","order_time":"order_time","sku":"sku","sale_quantity":"sale_quantity","order_id":"order_id","sale_amount":"sale_amount","tableName":"labelx.push_event_vip_order"},"goodsData":{"dt":"dt","cate":"category_large","sku":"sku","tableName":"labelx.push_event_vip_order"},"trainingScope":"过去15天","forecastPeriod":"未来15天","eventCode":{"event_code":{}}}
    :return:
    """
    global train_set

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

    # 暂时处理
    if (dataset.empty) or (len(dataset) == 0):#可合并
        print("Empty dataset for traning!")
        return

    if len(dataset[dataset['lab'] == 1]) == len(dataset):
        dataset.loc[:len(dataset) // 2, 'lab'] = 0
    elif len(dataset[dataset['lab'] == 0]) == len(dataset):
        dataset.loc[:len(dataset) // 2, 'lab'] = 1
    print(dataset)
    if (len(dataset) == 0) or (len(dataset[dataset['lab']==0]) == len(dataset)) or (len(dataset[dataset['lab']==1]) == len(dataset)):
        print("Train dataset just has one class, however this task required 2 classes at least! Please try another parameters!")
        return
    else:
        train_set, feats_train, labels_train, feats_test, labels_test = process_feats(dataset, 'lab')
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
        print('Bayes_optimalization for best_params:')
        print(best_params)
        best_lgb_model = lgb.LGBMClassifier(n_jobs=-1, boosting_type='gbdt', objective='binary', random_state=42, **best_params)
        best_lgb_model.fit(feats_train, labels_train)

        preds = best_lgb_model.predict_proba(feats_test)[:, 1]
        auc = roc_auc_score(labels_test, preds)
        print('The best model from Bayes optimization scores {:.5f} AUC ROC on the test set.'.format(auc))

        # store the model
        modelfilepath = str(solutionId)+r".pkl"
        pickle.dump(best_lgb_model, open(modelfilepath, 'wb'))
        params_str = json.dumps(input_params, ensure_ascii=False)
        paramfilepath = str(solutionId) + r'.txt'
        with open(paramfilepath, 'w') as f:
            f.write(params_str)
        model_target_file_path = os.path.join(hdfs_path, str(solutionId) + '.pkl')
        param_target_file_path = os.path.join(hdfs_path, str(solutionId) + '.txt')
        upload_flag1 = upload_hdfs(modelfilepath, model_target_file_path)
        upload_flag2 = upload_hdfs(paramfilepath, param_target_file_path)
        if upload_flag1 and upload_flag2:
            os.remove(modelfilepath)
            os.remove(paramfilepath)
            print('Success!!')
        else:
            print('Upload files failed!!')
        return

