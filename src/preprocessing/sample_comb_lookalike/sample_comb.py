#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.spark_helper as spark_helper
import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from torch.nn.utils.rnn import pad_sequence
from sklearn.model_selection import train_test_split
import pickle
import collections




def sample_comb(sample_table_name, user_feature_table_name, item_feature_table_name):
    spark_client = spark_helper.SparkClient()
    sample = spark_client.get_session().sql(f"select * from {sample_table_name}")
    user_feature = spark_client.get_session().sql(f"select * from {user_feature_table_name}")
    item_feature = spark_client.get_session().sql(f"select * from {item_feature_table_name}")

    col_sample = sample.columns
    user_id_sample = col_sample[0]
    item_id_sample = col_sample[1]

    col_user = user_feature.columns
    user_id = col_user[0]

    col_item = item_feature.columns
    item_id = col_item[0]

    user_feature = user_feature.withColumnRenamed(user_id, user_id_sample)
    item_feature = item_feature.withColumnRenamed(item_id, item_id_sample)

    data = sample.join(item_feature, item_id_sample)
    data = data.join(user_feature, user_id_sample,"left")
    data = data.toPandas()
    user_data = user_feature.toPandas()

    # TODO：后续完善hdfs_helper组件
    encoder_hdfs_path = "test1234"
    columns = data.columns
    data = train_data_preprocessing(data, columns, encoder_hdfs_path)

    train_data_table_name = "algorithm.tmp_aip_train_data"
    data.write.format("hive").mode("overwrite").saveAsTable(train_data_table_name)

    user_data = user_data_preprocessing(user_data, col_user, encoder_hdfs_path, sequence_feature_dic)

    user_data_table_name = "algorithm.tmp_aip_user_data"
    user_data.write.format("hive").mode("overwrite").saveAsTable(user_data_table_name)

    return train_data_table_name, columns, encoder_hdfs_path


def train_data_preprocessing(data, columns, hdfs_path):
    sparse_features = ['user_id', 'item_id', 'i_fund_type', 'i_management', 'i_custodian', 'i_invest_type', 'u_gender',
                       'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN',
                       'u_OCCU', 'u_IS_VAIID_INVST']
    dense_features = ['i_buy_counts_30d', 'i_amount_sum_30d', 'i_amount_avg_30d', 'i_amount_min_30d',
                      'i_amount_max_30d', 'u_buy_counts_30d',
                      'u_amount_sum_30d', 'u_amount_avg_30d', 'u_amount_min_30d', 'u_amount_max_30d', 'u_buy_days_30d',
                      'u_buy_avg_days_30d', 'u_last_buy_days_30d']
    sequence_features = ['u_buy_list']

    # 修改数据类型
    for col in columns:
        if data[col].dtype not in [np.dtype('float64'), np.dtype('int64')]:
            data[col] = data[col].astype('str')

    for feat in sparse_features:
        lbe = LabelEncoder()
        lbe.fit(data[feat])
        data[feat] = lbe.transform(data[feat])
        # TODO：完善hdfs_helper后存储encoder至hdfs
        # 保存encode
        with open(hdfs_path + '{}_encoder.pkl'.format(str(feat)), 'wb') as file:
            pickle.dump(lbe, file)

    mms = MinMaxScaler(feature_range=(0, 1))
    mms.fit(data[dense_features])
    data[dense_features] = mms.transform(data[dense_features])
    # TODO：完善hdfs_helper后存储scaler至hdfs
    with open(hdfs_path + 'scaler.pkl', 'wb') as file:
        pickle.dump(mms, file)

    tree = lambda: collections.defaultdict(tree)
    sequence_feature_dic = tree()
    for v in sequence_features:
        v_tmp = '_'.join(v.split('_')[0:2])
        sequence_feature_dic[v][v_tmp + '_key2index'], sequence_feature_dic[v]['train_' + v_tmp], \
        sequence_feature_dic[v][v_tmp + '_maxlen'] = get_var_feature(data, v)[0]
    with open(hdfs_path + 'sequence_feature_dic.pkl', 'wb') as file:
        pickle.dump(sequence_features, file)

    return data, sequence_feature_dic


def user_data_preprocessing(user_data, col_user, encoder_hdfs_path, sequence_feature_dic):
    user_sparse_features, user_dense_features = ['user_id', 'u_gender', 'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN',
                                                 'u_OCCU', 'u_IS_VAIID_INVST'], ['u_buy_counts_30d',
                                                                                 'u_amount_sum_30d', 'u_amount_avg_30d',
                                                                                 'u_amount_min_30d', 'u_amount_max_30d',
                                                                                 'u_buy_days_30d',
                                                                                 'u_buy_avg_days_30d',
                                                                                 'u_last_buy_days_30d']
    user_sequence_features = ['u_buy_list']

    # 修改数据类型
    for col in col_user:
        if user_data[col].dtype not in [np.dtype('float64'), np.dtype('int64')]:
            user_data[col] = user_data[col].astype('str')


    for feat in user_sparse_features:
        # 读取encode
        with open(encoder_hdfs_path + '{}_encoder.pkl'.format(str(feat)), 'rb') as file:
            lbe = pickle.load(file)
        user_data = user_data[feat].map(lambda s: 'unknown' if s not in lbe.classes_ else s)
        lbe.classes_ = np.append(lbe.classes_, "unknown")
        user_data[feat] = lbe.transform(user_data[feat])

    # TODO：完善hdfs_helper后存储scaler至hdfs
    with open(encoder_hdfs_path + 'scaler.pkl', 'rb') as file:
        mms = pickle.load(file)
    user_data[user_dense_features] = mms.transform(user_data[user_dense_features])

    sequence_feature_dic_test = {}
    for v in user_sequence_features:
        v_tmp = '_'.join(v.split('_')[0:2])
        sequence_feature_dic_test[v] = get_test_var_feature(user_data, v, sequence_feature_dic[v][v_tmp + '_key2index'])

    with open(encoder_hdfs_path + 'sequence_feature_dic_test.pkl', 'wb') as file:
        pickle.dump(sequence_feature_dic_test, file)

    return user_data



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
    var_feature_length = np.array(list(map(len, var_feature)))
    max_len = np.max(var_feature_length)
    var_feature = pad_sequence([torch.from_numpy(np.array(x)) for x in var_feature], batch_first=True).numpy()

    return key2index, var_feature, max_len


def get_test_var_feature(data, col, key2index):
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