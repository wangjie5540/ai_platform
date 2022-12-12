# !/usr/bin/env python3
'''
@file: model_train.py
@time: 2022/3/25 10:37
@desc: lookalike-model_train.py
'''

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from torch.nn.utils.rnn import pad_sequence
from sklearn.model_selection import train_test_split
# from construct_features import CreateDataset
from preprocessing.inputs import SparseFeat, DenseFeat, VarLenSparseFeat
from preprocessing.utils import size_format
from model.dssm import DSSM
# from spark_env import SparkEnv

import pickle
import collections
import json
import time
import os

import warnings

warnings.filterwarnings('ignore')  # "error", "ignore", "always", "default", "module" or "once"


def filter_features(features):
    '''
    用于筛选构建双塔特征所需特征集合
    :param features: 数据集特征
    :return: 构建双塔模型所需特征集合
    '''
    sparse_features = ['user_id', 'sku', 'gender', 'life_stage', 'consume_lvl',
                       'city', 'province', 'membership_level', 'brand', 'cate']
    dense_features = ['age', 'sign_on_days', 'latest_view_days', 'user_click_ratio_1m', 'od_ct_1m',
                      'avg_jg_1m', 'cate_ct_1m', 'qty_1m', 'qty_avg_1m', 'amt_1m', 'amt_avg_1m',
                      'user_exposure_ct_1m', 'user_exposure_days_1m', 'user_click_ct_1m', 'user_click_days_1m',
                      'user_shopcar_ct_1m', 'user_shopcar_days_1m', 'user_last_buy_diff',
                      'item_click_ratio_1m', 'item_exposure_cnt_1m', 'item_click_cnt_1m',
                      'item_shopcar_cnt_1m', 'it_od_ct_1m', 'it_qty_1m', 'it_amt_1m', 'it_us_ct_1m']
    sequence_features = ['buy_item_list', 'buy_cate_list', 'buy_brand_list',
                         'click_item_list', 'click_cate_list', 'click_brand_list']
    target = ['tag']
    user_sparse_features, user_dense_features = ['user_id', 'gender', 'life_stage', 'consume_lvl', 'city',
                                                 'province', 'membership_level'], \
                                                ['age', 'sign_on_days', 'latest_view_days', 'user_click_ratio_1m', 'od_ct_1m',
                                                 'avg_jg_1m', 'cate_ct_1m', 'qty_1m', 'qty_avg_1m', 'amt_1m',
                                                 'amt_avg_1m',
                                                 'user_exposure_ct_1m', 'user_exposure_days_1m', 'user_click_ct_1m',
                                                 'user_click_days_1m',
                                                 'user_shopcar_ct_1m', 'user_shopcar_days_1m', 'user_last_buy_diff']
    item_sparse_features, item_dense_features = ['sku', 'brand', 'cate'], ['item_click_ratio_1m',
                                                                           'item_exposure_cnt_1m', 'item_click_cnt_1m',
                                                                           'item_shopcar_cnt_1m', 'it_od_ct_1m',
                                                                           'it_qty_1m', 'it_amt_1m', 'it_us_ct_1m']
    user_sequence_features, item_sequence_features = ['buy_item_list', 'buy_cate_list', 'buy_brand_list',
                                                      'click_item_list', 'click_cate_list', 'click_brand_list'], []
    sparse_features = list(np.intersect1d(features, sparse_features))
    dense_features = list(np.intersect1d(features, dense_features))
    sequence_features = list(np.intersect1d(features, sequence_features))
    user_sparse_features = list(np.intersect1d(features, user_sparse_features))
    user_dense_features = list(np.intersect1d(features, user_dense_features))
    item_sparse_features = list(np.intersect1d(features, item_sparse_features))
    item_dense_features = list(np.intersect1d(features, item_dense_features))
    user_sequence_features = list(np.intersect1d(features, user_sequence_features))

    return sparse_features, dense_features, sequence_features, target, user_sparse_features, user_dense_features, item_sparse_features, item_dense_features, user_sequence_features, item_sequence_features


def data_process(train, test):
    data = pd.concat([train, test], ignore_index=True)

    test = data.loc[data["sku"].isnull()]

    # 修改'tag'
    # data = data.drop('tag', axis=1)
#     train['tag'] = train.apply(lambda x: function(x.dianji_30cnt), axis=1)

    # 修改数据类型
    for li in list(data):
        if data[li].dtype not in [np.dtype('float64'), np.dtype('int64')]:
            data[li] = data[li].astype('str')

    train_frc = train.sample(frac=1)
    train_frc = train_frc.sample(frac=1)
    # 删除指定列
    # data_new = data_frc.drop('Unnamed: 0', axis=1)
    # print(data_new.columns)
    # 填充、替换
    train_new = train_frc.fillna(0)
    train_new = train_new.replace(to_replace=-1, value=0)
    test_new = test.fillna(0)
    test_new = test_new.replace(to_replace=-1, value=0)
    data_new = data.fillna(0)
    data_new = data_new.replace(to_replace=-1, value=0)

    # # drop sequence features
    # data_new['goumai_cate_list'].drop
    # print(train_new.info())

    # 划分训练集和测试集
    # train = data_new.iloc[:int(len(data_new) * 0.8)].copy()
    # print(train['tag'].value_counts())
    # test = data_new.iloc[int(len(data_new) * 0.8):].copy()
    # print(test['tag'].value_counts())
    return train_new, test_new, data_new


def get_var_feature(data, col):
    key2index = {}

    def split(x):
        x = str(x)
        key_ans = x.split('|')
        for key in key_ans:
            if key not in key2index:
                # Notice : input value 0 is a special "padding",\
                # so we do not use 0 to encode valid feature for sequence input
                key2index[key] = len(key2index) + 1
        return list(map(lambda x: key2index[x], key_ans))

    var_feature = list(map(split, data[col].values))
    # print(var_feature)
    var_feature_length = np.array(list(map(len, var_feature)))
    max_len = max(var_feature_length)
    var_feature = pad_sequence([torch.from_numpy(np.array(x)) for x in var_feature], batch_first=True).numpy()
    # print(key2index)
    # print(var_feature)
    # print(max_len)
    return key2index, var_feature, max_len


def get_test_var_feature(data, col, key2index, max_len):
    # print("user_hist_list: \n")

    def split(x):
        x = str(x)
        key_ans = x.split('|')
        for key in key_ans:
            if key not in key2index:
                # Notice : input value 0 is a special "padding",
                # so we do not use 0 to encode valid feature for sequence input
                key2index[key] = len(key2index) + 1
        return list(map(lambda x: key2index[x], key_ans))

    test_hist = list(map(split, data[col].values))
    test_hist = pad_sequence([torch.from_numpy(np.array(x)) for x in test_hist], batch_first=True).numpy()
    return test_hist


# 获取训练集、测试集输入
def get_train_test_input(sample, user_list, sparse_features, dense_features, sequence_feature,
                         user_sparse_features, user_dense_features,
                         item_sparse_features, item_dense_features,
                         user_sequence_feature, item_sequence_feature,
                         encoder_path, scaler_path):
    print("数据预处理...")
    train, test, data = data_process(sample, user_list)

    print("特征处理...")
    # 1.Label Encoding for sparse features,and process sequence features
    for feat in sparse_features:
        lbe = LabelEncoder()
        if feat in user_sparse_features:
            lbe.fit(data[feat])
        else:
            lbe.fit(train[feat])
        train[feat] = lbe.transform(train[feat])
        if feat in user_sparse_features:
            test[feat] = lbe.transform(test[feat])
        # 保存encode
        output = open(encoder_path + '{}_encoder.pkl'.format(str(feat)), 'wb')
        pickle.dump(lbe, output)
        output.close()

    mms = MinMaxScaler(feature_range=(0, 1))
    mms.fit(train[dense_features])
    train[dense_features] = mms.transform(train[dense_features])
    output = open(scaler_path + 'scaler.pkl', 'wb')
    pickle.dump(mms, output)
    output.close()
    

    # 2.preprocess the sequence feature
    tree = lambda: collections.defaultdict(tree)
    sequence_feature_dic = tree()
    for v in sequence_feature:
        v_tmp = '_'.join(v.split('_')[0:2])
        sequence_feature_dic[v][v_tmp + '_key2index'] = get_var_feature(train, v)[0]
        sequence_feature_dic[v]['train_' + v_tmp] = get_var_feature(train, v)[1]
        sequence_feature_dic[v][v_tmp + '_maxlen'] = get_var_feature(train, v)[2]

    user_feature_columns = [SparseFeat(feat, data[feat].nunique(), embedding_dim=4)
                            for i, feat in enumerate(user_sparse_features)] + [DenseFeat(feat, 1, ) for feat in
                                                                               user_dense_features]

    item_feature_columns = [SparseFeat(feat, data[feat].nunique(), embedding_dim=4)
                            for i, feat in enumerate(item_sparse_features)] + [DenseFeat(feat, 1, ) for feat in
                                                                               item_dense_features]
    print("生成模型输入格式数据...")
    # 3.generate input data for model
    for user_v in user_sequence_feature:
        v_tmp = '_'.join(user_v.split('_')[0:2])
        user_varlen_feature_columns = [
            VarLenSparseFeat(SparseFeat(user_v, vocabulary_size=34700, embedding_dim=4),
                             maxlen=sequence_feature_dic[user_v][v_tmp + '_maxlen'], combiner='mean', length_name=None)]
        user_feature_columns += user_varlen_feature_columns
    for item_v in item_sequence_feature:
        v_tmp = '_'.join(item_v.split('_')[0:2])
        item_varlen_feature_columns = [
            VarLenSparseFeat(SparseFeat(item_v, vocabulary_size=34700, embedding_dim=4),
                             maxlen=sequence_feature_dic[item_v][v_tmp + '_maxlen'], combiner='mean', length_name=None)]
        item_feature_columns += item_varlen_feature_columns

    # add user history as user_varlen_feature_columns
    train_model_input = {name: train[name] for name in sparse_features + dense_features}
    for v in sequence_feature:
        v_tmp = '_'.join(v.split('_')[0:2])
        train_model_input[v] = sequence_feature_dic[v]['train_' + v_tmp]

    # 测试集
    test[dense_features] = mms.transform(test[dense_features])

    sequence_feature_dic_test = {}
    for v in user_sequence_feature:
        v_tmp = '_'.join(v.split('_')[0:2])
        sequence_feature_dic_test[v] = get_test_var_feature(test, v, sequence_feature_dic[v][v_tmp + '_key2index'],
                                                            sequence_feature_dic[v][v_tmp + '_maxlen'])

    test_model_input = {name: test[name] for name in user_sparse_features + user_dense_features}

    for test_v in sequence_feature_dic_test:
        test_model_input[test_v] = sequence_feature_dic_test[test_v]

    return train, test, data, user_feature_columns, item_feature_columns, train_model_input, test_model_input

def upload_user_embedding(spark, taskid, user_embedding_df):
    sql_lastest_version = '''
        select
            max(version)
        from
            algorithm.lookalike_cdp_result
        where
            taskid = '{0}'
    '''.format(taskid)
    lastest_version_df = spark.sql(sql_lastest_version).toPandas()
    if len(lastest_version_df) == 0 or lastest_version_df.iloc[0, 0] is None:
        cur_version = 1
    else:
        cur_version = int(lastest_version_df.iloc[0, 0]) + 1

    embedding_spark_df = spark.createDataFrame(user_embedding_df)
    embedding_spark_df.createOrReplaceTempView('user_embedding_tmp')

    sql_result = '''
        insert into algorithm.lookalike_cdp_result
        select 
            user_id,
            embedding,
            {0} as taskid,
            {1} as version
        from
            user_embedding_tmp
    '''.format(taskid, cur_version)
    spark.sql(sql_result)

    
if __name__ == '__main__':
    data_dict = {"taskId":40,
            "userData":{"city":"","sex":"sex","consume_level":"consume_level","yuliu_id":"","tableName":"labelx.push_user","dt":"dt","recent_view_day":"","membership_level":"","province":"","user_id":"user_id","online_signup_time":"online_signup_time","life_stage":"life_stage","age":"age"},
            "trafficData":{"cart_remove":"","cart_add":"","click":"CLICK","tableName":"labelx.push_traffic_behavior","dt":"","search":"SEARCH","exposure":"EXPOSURE","card_add":"CART_ADD","user_id":"user_id","event_code":"event_code","sku":"sku","card_remove":"CART_REMOVE","collect":"COLLECT","event_time":"event_time","browse":"BROWSE"},
            "orderData":{"dt":"","user_id":"user_id","order_time":"order_time","sku":"sku","sale_quantity":"sale_quantity","order_id":"order_id","sale_amount":"sale_amount","tableName":"labelx.push_order_behavior"},
            "goodsData":{"dt":"dt","cate":"cate","sku":"sku","brand":"brand","tags":"","tableName":"labelx.push_goods"},
            "eventCode":{"event_code":{"search":"SEARCH","cart_remove":"","exposure":"EXPOSURE","cart_add":"","click":"CLICK","collect":"COLLECT","browse":"BROWSE"}}}
    taskid = data_dict.get("taskId")
    userData  = data_dict.get("userData")
    bhData = data_dict.get("trafficData")
    orderData = data_dict.get("orderData")
    goodsData = data_dict.get("goodsData")
    eventCode = data_dict.get('eventCode').get(list(data_dict.get('eventCode').keys())[0])

    # 设置路径
    task_path = "../model/{}/".format(taskid)
    encoder_path = task_path + "encoder/"
    scaler_path = task_path + "scaler/"
    model_path = task_path + "model.pth"
    
    
    # 检查文件夹
    if os.path.exists(task_path):
        pass
    else:
        os.makedirs(task_path)
    if os.path.exists(encoder_path):
        pass
    else:
        os.makedirs(encoder_path)
    if os.path.exists(scaler_path):
        pass
    else:
        os.makedirs(scaler_path)

    #     # 根据参数构建训练所需数据集
    #     cd = CreateDataset()
    #     is_train = True
    #     sample = cd.ConstructFeatures(is_train, userData, bhData, orderData, goodsData, eventCode)
    #     is_train = False
    #     user_list = cd.ConstructFeatures(is_train, userData, bhData, orderData, goodsData, eventCode)
    #     user_id_list = user_list['id'].copy()

    sample = pd.read_csv("测试数据.csv", sep='\t')
    user_list = pd.read_csv("全量用户.csv", sep='\t')
    user_id_list = user_list['user_id'].copy()

    # 筛选构建模型所需特征
    sparse_features, dense_features, sequence_features, target, user_sparse_features, user_dense_features, item_sparse_features, item_dense_features, user_sequence_features, item_sequence_features = filter_features(sample.columns)
    
    # 构建模型所需数据、特征及相应输入
    train, test, data, user_feature_columns, item_feature_columns, train_model_input, test_model_input = \
    get_train_test_input(sample, user_list, sparse_features, dense_features, sequence_features,
                         user_sparse_features, user_dense_features,
                         item_sparse_features, item_dense_features,
                         user_sequence_features, item_sequence_features,
                         encoder_path, scaler_path)

    # 3、定义模型，训练、预测、评估
    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        print('cuda ready...')
        device = 'cuda:0'

    # 定义
    model = DSSM(user_feature_columns, item_feature_columns, l2_reg_dnn=1e-5, l2_reg_embedding=1e-5,
                 dnn_dropout=0.2, task='binary', device=device)

    model.compile("adam", "binary_crossentropy",metrics=['auc', 'accuracy', 'precision', 'recall', 'f1_score'])

    print("开始训练：")
    start = time.time()
    model.fit(train_model_input, train[target].values, epochs=3, verbose=2, validation_split=0.2)
    end = time.time()
    print("训练结束，共用时：{}秒".format(end - start))

    # 评估
    eval_tr = model.evaluate(train_model_input, train[target].values)
    print("模型评估：",'\n',eval_tr)

    
    # 4、预测
    # 训练过程保存了最佳模型，需要先加载模型
    state_dict = torch.load("./model_zoo/model.pth")

    model = DSSM(user_feature_columns, item_feature_columns, task='binary', device=device)
    model.load_state_dict(state_dict['model_state_dict'], strict=False)
    dict_trained = model.state_dict()  # trained model
    trained_lst = list(dict_trained.keys())

    # 获取单塔 user tower
    model_user = DSSM(user_feature_columns, [], task='binary', device=device)
    dict_user = model_user.state_dict()
    for key in dict_user:
        dict_user[key] = dict_trained[key]
    model_user.load_state_dict(dict_user)  # load trained model parameters of user tower
    user_feature_name = user_sparse_features + user_dense_features
    user_model_input = {name: test[name] for name in user_feature_name}
    for user_seq in user_sequence_features:
        user_model_input[user_seq] = test_model_input[user_seq]
    user_embedding = model_user.predict(user_model_input, batch_size=256)
    # print("single user embedding shape: ", user_embedding[0:2])

#     test = pd.read_csv(test_path, sep='\t')
    user_embedding_dic = {}
    for i in range(len(user_embedding)):
        k = user_id_list['user_id'][i]
        user_embedding_dic[str(k)] = user_embedding[i].tolist()

    user_embedding_df = pd.DataFrame.from_dict(user_embedding_dic, orient='index', columns=['embedding'])
    user_embedding_df = user_embedding_df.reset_index().rename(columns={'index': 'user_id'})
    print("上传用户向量")
    spark = SparkEnv("test_lookalike_train")
    upload_user_embedding(spark, taskid, user_embedding_df)
    spark.stop()


