#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.spark_helper as spark_helper
from preprocessing.inputs import SparseFeat, DenseFeat, VarLenSparseFeat
import pickle
import numpy as np
import time
import torch
from model.dssm import DSSM
from sklearn.preprocessing import LabelEncoder
import torch.optim.adam as adam


def start_model_train(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_path,
                      train_data_columns, user_data_columns,
                      dnn_hidden_units=(256, 128, 64), dnn_dropout=0.2,
                      batch_size=256, lr=0.01):
    spark_client = spark_helper.SparkClient()
    train_data = spark_client.get_session().sql(
        f"""select {",".join(train_data_columns)} from {train_data_table_name}""").toPandas()
    test_data = spark_client.get_session().sql(
        f"""select {",".join(train_data_columns)} from {test_data_table_name}""").toPandas()
    user_data = spark_client.get_session().sql(
        f"""select {",".join(user_data_columns)} from {user_data_table_name}""").toPandas()

    feature_columns = train_data.columns
    sparse_features, dense_features, sequence_features, \
    target, user_sparse_features, user_dense_features, \
    item_sparse_features, item_dense_features, \
    user_sequence_features, item_sequence_features = filter_features(
        feature_columns)
    user_feature_columns, item_feature_columns, train_model_input, test_model_input, user_model_input = \
        get_train_test_input(train_data, test_data, user_data,
                             sparse_features, dense_features, sequence_features,
                             user_sparse_features, user_dense_features,
                             item_sparse_features, item_dense_features,
                             user_sequence_features, item_sequence_features,
                             hdfs_path)

    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        print('cuda ready...')
        device = 'cuda:0'
    model = DSSM(user_feature_columns, item_feature_columns, dnn_hidden_units=dnn_hidden_units,
                 dnn_dropout=dnn_dropout, task='binary', device=device)
    optim = adam.Adam(model.parameters(), lr=lr)
    model.compile(optim, "binary_crossentropy", metrics=['auc'])

    print("开始训练：")
    start = time.time()
    model.fit(train_model_input, train_data[target].values, batch_size=batch_size,
              epochs=3, verbose=2, validation_split=0.2)
    end = time.time()
    print("训练结束，共用时：{}秒".format(end - start))

    # TODO：上传模型至hdfs

    # 评估
    state_dict = torch.load("./model_zoo/model.pth")
    model = DSSM(user_feature_columns, item_feature_columns, dnn_hidden_units=(256, 128, 64),
                 dnn_dropout=0.2, task='binary', device=device)
    optim = adam.Adam(model.parameters(), lr=lr)
    model.compile(optim, "binary_crossentropy", metrics=['auc'])
    model.load_state_dict(state_dict['model_state_dict'], strict=False)

    eval_tr = model.evaluate(test_model_input, test_data[target].values)
    print(eval_tr)

    dict_trained = model.state_dict()  # trained model
    # 获取单塔 user tower
    model_user = DSSM(user_feature_columns, [], dnn_hidden_units=(256, 128, 64),
                      dnn_dropout=0.2, task='binary', device=device)
    dict_user = model_user.state_dict()
    for key in dict_user:
        dict_user[key] = dict_trained[key]
    model_user.load_state_dict(dict_user, strict=False)  # load trained model parameters of user tower

    user_embedding = model_user.predict(user_model_input, batch_size=batch_size)

    print(user_embedding[0])


def filter_features(features):
    '''
    用于筛选构建双塔特征所需特征集合
    :param features: 数据集特征
    :return: 构建双塔模型所需特征集合
    '''
    # TODO：特征灵活配置
    sparse_features = ['user_id', 'item_id', 'i_fund_type', 'i_management', 'i_custodian', 'i_invest_type', 'u_gender',
                       'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN',
                       'u_OCCU', 'u_IS_VAIID_INVST']
    dense_features = ['i_buy_counts_30d', 'i_amount_sum_30d', 'i_amount_avg_30d', 'i_amount_min_30d',
                      'i_amount_max_30d', 'u_buy_counts_30d',
                      'u_amount_sum_30d', 'u_amount_avg_30d', 'u_amount_min_30d', 'u_amount_max_30d', 'u_buy_days_30d',
                      'u_buy_avg_days_30d', 'u_last_buy_days_30d']
    sequence_features = ['u_buy_list']
    target = ['label']
    user_sparse_features, user_dense_features = ['user_id', 'u_gender', 'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN',
                                                 'u_OCCU', 'u_IS_VAIID_INVST'], ['u_buy_counts_30d',
                                                                                 'u_amount_sum_30d', 'u_amount_avg_30d',
                                                                                 'u_amount_min_30d', 'u_amount_max_30d',
                                                                                 'u_buy_days_30d',
                                                                                 'u_buy_avg_days_30d',
                                                                                 'u_last_buy_days_30d']
    item_sparse_features, item_dense_features = ['item_id', 'i_fund_type', 'i_management', 'i_custodian',
                                                 'i_invest_type'], ['i_buy_counts_30d', 'i_amount_sum_30d',
                                                                    'i_amount_avg_30d', 'i_amount_min_30d',
                                                                    'i_amount_max_30d']
    user_sequence_features, item_sequence_features = ['u_buy_list'], []
    sparse_features = list(np.intersect1d(features, sparse_features))
    dense_features = list(np.intersect1d(features, dense_features))
    sequence_features = list(np.intersect1d(features, sequence_features))
    user_sparse_features = list(np.intersect1d(features, user_sparse_features))
    user_dense_features = list(np.intersect1d(features, user_dense_features))
    item_sparse_features = list(np.intersect1d(features, item_sparse_features))
    item_dense_features = list(np.intersect1d(features, item_dense_features))
    user_sequence_features = list(np.intersect1d(features, user_sequence_features))
    item_sequence_features = list(np.intersect1d(features, item_sequence_features))

    return sparse_features, dense_features, sequence_features, target, user_sparse_features, user_dense_features, item_sparse_features, item_dense_features, user_sequence_features, item_sequence_features


# 获取训练集、测试集输入
def get_train_test_input(train_data, test_data, user_data,
                         sparse_features, dense_features, sequence_feature,
                         user_sparse_features, user_dense_features,
                         item_sparse_features, item_dense_features,
                         user_sequence_feature, item_sequence_feature,
                         hdfs_path):
    mapping_dict = {"u_buy_list":"item_id"}

    with open(hdfs_path + "sparse_features_dict.pkl", "rb") as file:
        sparse_features_dict = pickle.load(file)
    with open(hdfs_path + "id_features_dict.pkl", "rb") as file:
        id_features_dic = pickle.load(file)
    user_feature_columns = [SparseFeat(feat, len(sparse_features_dict[feat].keys()) if feat != "user_id" else len(id_features_dic[feat].keys()), embedding_dim=4)
                            for i, feat in enumerate(user_sparse_features)] + [DenseFeat(feat, 1, ) for feat in
                                                                               user_dense_features]

    item_feature_columns = [SparseFeat(feat, len(sparse_features_dict[feat].keys()) if feat != "item_id" else len(id_features_dic[feat].keys()), embedding_dim=4)
                            for i, feat in enumerate(item_sparse_features)] + [DenseFeat(feat, 1, ) for feat in
                                                                               item_dense_features]
    print("生成模型输入格式数据...")
    # 3.generate input data for model
    for user_v in user_sequence_feature:
        maxlen = len(train_data[user_v][0].split("|"))
        mapping_feat = mapping_dict.get(user_v)
        user_varlen_feature_columns = [
            VarLenSparseFeat(SparseFeat(user_v, len(id_features_dic[mapping_feat].keys()), embedding_dim=4),
                             maxlen=maxlen, combiner='mean', length_name=None)]
        user_feature_columns += user_varlen_feature_columns
    for item_v in item_sequence_feature:
        maxlen = len(train_data[item_v][0].split("|"))
        mapping_feat = mapping_dict.get(item_v)
        item_varlen_feature_columns = [
            VarLenSparseFeat(SparseFeat(item_v, len(id_features_dic[mapping_feat].keys()), embedding_dim=4),
                             maxlen=maxlen, combiner='mean', length_name=None)]
        item_feature_columns += item_varlen_feature_columns

    # add user history as user_varlen_feature_columns
    train_model_input = {name: train_data[name] for name in sparse_features + dense_features}
    for v in sequence_feature:
        train_model_input[v] = np.array(
            list(train_data['u_buy_list'].map(lambda x: [int(i) for i in x.split("|")]).values))

    # 测试集
    test_model_input = {name: test_data[name] for name in
                        sparse_features + dense_features}
    for v in sequence_feature:
        test_model_input[v] = np.array(
            list(test_data['u_buy_list'].map(lambda x: [int(i) for i in x.split("|")]).values))

    # 全量用户
    user_model_input = {name: user_data[name] for name in user_sparse_features + user_dense_features}
    for v in user_sequence_feature:
        user_model_input[v] = np.array(
            list(user_data['u_buy_list'].map(lambda x: [int(i) for i in x.split("|")]).values))

    return user_feature_columns, item_feature_columns, train_model_input, test_model_input, user_model_input
