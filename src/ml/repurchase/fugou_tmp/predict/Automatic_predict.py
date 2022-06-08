import os
import logging
from typing import Dict
from construct_features_v2 import CreateDataset
import pandas as pd
import numpy as np
import datetime
import pickle
import json
from utils import *

def predict(dataset:pd.DataFrame, model_path:str, targetLimit):
    targetLimit = json.loads(targetLimit)
    user_id = dataset['user_id']
    feats = dataset.drop('user_id', axis=1)
    feats.fillna(value=0, inplace=True)
    model = pickle.load(open(model_path, 'rb'))
    prediction = model.predict_proba(feats)[:,1]
    res_total = pd.concat([user_id, pd.DataFrame(prediction, columns=['pred'], index=user_id.index)], axis=1)
    # print(res_total)
    res_fugou = res_total.sort_values(by='pred', ascending=False)
    # print(res_fugou)
#     print(targetLimit)
    if targetLimit['targetLimitType'] == 1:
        return res_fugou.iloc[:targetLimit['targetLimitValue'], 0]
    elif targetLimit['targetLimitType'] == 2:
        return res_fugou[res_fugou['pred'] >= targetLimit['targetLimitValue']/100]['user_id']
    else:
        return res_fugou['user_id']

def predict_main(input_params:Dict, path):
    try:
        # Download model and parameters form hdfs
        model_path = input_params['modelFileUrl']
        params_path = model_path[:model_path.index('.pkl')] + r'.txt'
        taskid = model_path[model_path.index('model')+6:model_path.index('.pkl')]
        model_tmp_local_path = os.path.join(path, 'tmp', str(taskid) + r'.pkl')
        params_tmp_local_path = os.path.join(path, 'tmp',str(taskid) + r'.txt')
        model_tmp_local_path = download_hdfs(model_tmp_local_path, model_path)
        params_tmp_local_path = download_hdfs(params_tmp_local_path, params_path)
        logging.info('加载模型和参数：')

        with open(params_tmp_local_path, 'r') as file:
            params_trained = file.readline()
        params_trained  = json.loads(params_trained)
        logging.info(params_trained)

        cate_list = []
        for cateid in params_trained['category']:
            cate = '"'+cateid+'"'
            cate_list.append(cate)
        catestr = "("+",".join(cate_list)+")"

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
                                                params_trained['eventCode'][params_trained['trafficData']['event_code']],
                                                input_params['where']
                                                )
    
        if len(dataset)==0:
            logging.info('Empty dataset for predicting')
            return ''
        else:
            res = predict(dataset, model_tmp_local_path, input_params['targetLimit'])
        #     print(res)
            outputpath = os.path.join(path, 'output')
            fname = str(input_params['crowdid']) + r'.csv'
            filepath = os.path.join(outputpath, fname)
            res.to_csv(filepath, header=False, index=False)

            # upload result to hdfs
            res_target_path = os.path.join('hdfs:///usr/algorithm/cd/fugou/result', str(input_params['crowdid'])+'.csv')
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
        logging.error('Raise error when predicting',exc_info=1)
        return '-1'