import os
from typing import Dict
import  json
# from unicodedata import category
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from bayes_opt import BayesianOptimization
import pickle
import datetime
from create_dataset import CreateDataset
import time
import logging
import traceback
import digitforce.aip.common.hdfs_helper as HdfsClient
from digitforce.aip.common.filesize_helper import get_filesize
import warnings
warnings.filterwarnings("ignore")

train_set = None



def upload_hdfs(filepath, target_file_path):
    try:
        cli = HdfsClient(hosts="bigdata-server-08:9870")
        if cli.exists(target_file_path):
            cli.delete(target_file_path)
        cli.copy_from_local(filepath, target_file_path)
        return target_file_path
    except:
        # print(traceback.format_exc())
        logging.info(traceback.format_exc())
        return ""

def download_hdfs(local_path, ModelFileUrl):
    try:
        cli = HdfsClient(hosts='10.100.0.82:4008')
        if cli.exists(ModelFileUrl):
            cli.copy_to_local(ModelFileUrl, local_path)
            return local_path
        else:
            return None
    except:
        # print(traceback.format_exc())
        logging.info(traceback.format_exc())
        return ""

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

def train(input_params:Dict, upload_filepath_head):
    """
    :param input_params: 输入配置参数
    :param upload_filepath_head: 模型文件保存位置文件头
    :return: 接口交互部分参数
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
            filepath = str(input_params['taskid'])+r".pkl"
            pickle.dump(best_lgb_model, open(filepath, 'wb'))
            end = time.time()
            cur_time = datetime.datetime.now()


            # upload model and parameters
            model_target_file_path = os.path.join(upload_filepath_head, str(input_params['taskid'])+'.pkl')
            upload_flag = upload_hdfs(filepath, model_target_file_path)
            if upload_flag:
                model_file_url = upload_flag
                os.remove(filepath)
            else:
                model_file_url = '-1'



            res = {}
            res['generateTime'] = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
            res['taskId'] = input_params['taskid']
            res['auc'] = "%.2f%%" % (auc * 100)
            res['fileSize'] = get_filesize(filepath)
            res['elapsed'] = int(end - start)
            res['fileDownloadUrl'] = model_file_url
            return res
    except:
        logging.error('Raise errors when response to platform',exc_info=1)
        return {'generateTime':"", "taskId":input_params['taskid'], 'auc':"", "fileSize":"", 'elapsed':0, "fileDownloadUrl":"-1"}


def predict_main(dataset: pd.DataFrame, model_path: str, targetLimit):
    targetLimit = json.loads(targetLimit)
    user_id = dataset.iloc[:, 1]
    feats = dataset.iloc[:, 1:]
    feats.fillna(value=0, inplace=True)
    model = pickle.load(open(model_path, 'rb'))
    prediction = model.predict_proba(feats)[:, 1]
    res_total = pd.concat([user_id, pd.DataFrame(prediction, columns=['pred'], index=user_id.index)], axis=1)
    # print(res_total)
    res_fugou = res_total.sort_values(by='pred', ascending=False)
    # print(res_fugou)
    #     print(targetLimit)
    if targetLimit['targetLimitType'] == 1:
        return res_fugou.iloc[:targetLimit['targetLimitValue'], 0]
    elif targetLimit['targetLimitType'] == 2:
        return res_fugou[res_fugou['pred'] >= targetLimit['targetLimitValue'] / 100].iloc[:, 1]
    else:
        return res_fugou.iloc[:, 1]


def predict(input_params: Dict,  upload_filepath_head):
    try:
        # Download model and parameters form hdfs
        model_path = input_params['modelFileUrl']
        params_train_path = model_path[:model_path.index('.pkl')] + r'.txt'
        taskid = model_path[model_path.index('model') + 6:model_path.index('.pkl')]
        model_tmp_local_path = 'tmp' + str(taskid) + r'.pkl'
        params_tmp_local_path = 'tmp' + str(taskid) + r'.txt'
        model_tmp_local_path = download_hdfs(model_tmp_local_path, model_path)
        params_tmp_local_path = download_hdfs(params_tmp_local_path, params_train_path)
        logging.info('加载模型和参数：')

        with open(params_tmp_local_path, 'r') as file:
            params_trained = file.readline()
        params_trained = json.loads(params_trained)
        logging.info(params_trained)

        cate_list = []
        for cateid in params_trained['category']:
            cate = '"' + cateid + '"'
            cate_list.append(cate)
        catestr = "(" + ",".join(cate_list) + ")"

        create_data = CreateDataset()
        dataset = create_data.ConstructFeatures(params_trained['trainingScope'],
                                                params_trained['forecastPeriod'],
                                                catestr,
                                                False,
                                                params_trained['orderData']['tableName'],
                                                params_trained['trafficData']['tableName'],
                                                params_trained['userData']['tableName'],
                                                params_trained['goodsData']['tableName'],
                                                params_trained['orderData'],
                                                params_trained['trafficData'],
                                                params_trained['userData'],
                                                params_trained['goodsData'],
                                                params_trained['eventCode'][
                                                    params_trained['trafficData']['event_code']],
                                                input_params['where']
                                                )

        if len(dataset) == 0:
            logging.info('Empty dataset for predicting')
            return ''
        else:
            res = predict_main(dataset, model_tmp_local_path, input_params['targetLimit'])
            #     print(res)
            filepath =  str(input_params['crowdid']) + r'.csv'
            res.to_csv(filepath, header=False, index=False)

            # upload result to hdfs
            res_target_path = os.path.join(upload_filepath_head, str(input_params['crowdid']) + '.csv')
            upload_flag = upload_hdfs(filepath, res_target_path)
            logging.info(upload_flag)
            if upload_flag:
                logging.info('Upload successfully!')
                model_file_url = upload_flag
                os.remove(model_tmp_local_path)
                os.remove(params_tmp_local_path)
            else:
                model_file_url = '-1'
            return model_file_url
    except:
        logging.error('Raise error when predicting', exc_info=1)
        return '-1'