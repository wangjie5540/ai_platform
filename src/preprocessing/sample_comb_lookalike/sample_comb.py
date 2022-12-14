#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.spark_helper as spark_helper
import numpy as np
import torch
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from torch.nn.utils.rnn import pad_sequence
import pickle
import collections
from pyspark.ml.feature import StringIndexer, StringIndexerModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import MinMaxScaler as MinMaxScalerSpark, VectorAssembler


def sample_comb(sample_table_name, sample_columns,
                user_feature_table_name, user_columns,
                item_feature_table_name, item_columns):
    spark_client = spark_helper.SparkClient()
    sample = spark_client.get_session().sql(f"""select {",".join(sample_columns)} from {sample_table_name}""")
    user_feature = spark_client.get_session().sql(f"""select {",".join(user_columns)} from {user_feature_table_name}""")
    item_feature = spark_client.get_session().sql(f"""select {",".join(item_columns)} from {item_feature_table_name}""")

    user_id_sample = sample_columns[0]
    item_id_sample = sample_columns[1]

    user_id = user_columns[0]

    item_id = item_columns[0]

    user_feature = user_feature.withColumnRenamed(user_id, user_id_sample)
    item_feature = item_feature.withColumnRenamed(item_id, item_id_sample)

    data = sample.join(item_feature, item_id_sample)
    data = data.join(user_feature, user_id_sample, "left")
    # TODO：后续改为在spark上进行预处理
    data = data.toPandas()
    user_data = user_feature.toPandas()

    # TODO：后续完善hdfs_helper组件
    hdfs_dir = "/data/pycharm_project_950/src/preprocessing/sample_comb_lookalike/dir/"
    columns = data.columns
    data, sequence_feature_dic = train_data_preprocessing(data, columns, hdfs_dir)

    data = data.sample(frac=1).reset_index(drop=True)
    train_data = data.iloc[:int(len(data) * 0.8), :].reset_index(drop=True)
    test_data = data.iloc[int(len(data) * 0.8):, :].reset_index(drop=True)

    train_data_table_name = "algorithm.tmp_aip_train_data"
    train_data = spark_client.get_session().createDataFrame(train_data)
    train_data.write.format("hive").mode("overwrite").saveAsTable(train_data_table_name)

    test_data_table_name = "algorithm.tmp_aip_test_data"
    test_data = spark_client.get_session().createDataFrame(test_data)
    test_data.write.format("hive").mode("overwrite").saveAsTable(test_data_table_name)

    user_data = user_data_preprocessing(user_data, user_columns, hdfs_dir, sequence_feature_dic)

    user_data_table_name = "algorithm.tmp_aip_user_data"
    user_data = spark_client.get_session().createDataFrame(user_data)
    user_data.write.format("hive").mode("overwrite").saveAsTable(user_data_table_name)

    return train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir


def train_data_preprocessing(data, columns, hdfs_path):
    sparse_features = ['user_id', 'item_id', 'i_fund_type', 'i_management', 'i_custodian', 'i_invest_type', 'u_gender',
                       'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN',
                       'u_OCCU', 'u_IS_VAIID_INVST']
    sequence_features = ['u_buy_list']
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
    mms.fit(data[user_dense_features])
    data[user_dense_features] = mms.transform(data[user_dense_features])
    # TODO：完善hdfs_helper后存储scaler至hdfs
    with open(hdfs_path + 'user_scaler.pkl', 'wb') as file:
        pickle.dump(mms, file)

    mms = MinMaxScaler(feature_range=(0, 1))
    mms.fit(data[item_dense_features])
    data[item_dense_features] = mms.transform(data[item_dense_features])
    # TODO：完善hdfs_helper后存储scaler至hdfs
    with open(hdfs_path + 'item_scaler.pkl', 'wb') as file:
        pickle.dump(mms, file)

    sequence_feature_dic = collections.defaultdict(collections.defaultdict)
    for v in sequence_features:
        v_tmp = '_'.join(v.split('_')[0:2])
        data, sequence_feature_dic[v][v_tmp + '_key2index'], sequence_feature_dic[v][v_tmp + '_maxlen'] = \
            get_var_feature(data, v)
    with open(hdfs_path + 'sequence_feature_dic.pkl', 'wb') as file:
        pickle.dump(sequence_feature_dic, file)

    return data, sequence_feature_dic


def user_data_preprocessing(user_data, col_user, hdfs_path, sequence_feature_dic):
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
        with open(hdfs_path + '{}_encoder.pkl'.format(str(feat)), 'rb') as file:
            lbe = pickle.load(file)
        user_data[feat] = user_data[feat].map(lambda s: 'unknown' if s not in lbe.classes_ else s)
        lbe.classes_ = np.append(lbe.classes_, "unknown")
        user_data[feat] = lbe.transform(user_data[feat])

    # TODO：完善hdfs_helper后存储scaler至hdfs
    with open(hdfs_path + 'user_scaler.pkl', 'rb') as file:
        mms = pickle.load(file)
    user_data[user_dense_features] = mms.transform(user_data[user_dense_features])

    sequence_feature_dic_test = collections.defaultdict(collections.defaultdict)
    for v in user_sequence_features:
        v_tmp = '_'.join(v.split('_')[0:2])
        user_data, sequence_feature_dic_test[v][v_tmp + '_key2index'], sequence_feature_dic_test[v][
            v_tmp + '_maxlen'] = get_var_feature(user_data, v, sequence_feature_dic[v][v_tmp + '_key2index'])

    with open(hdfs_path + 'sequence_feature_dic_test.pkl', 'wb') as file:
        pickle.dump(sequence_feature_dic_test, file)

    return user_data


def get_var_feature(data, col, key2index=None):
    if not key2index:
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

    data[col] = data[col].map(split)
    max_len = np.max(list(map(len, data[col])))
    data[col] = list(map(lambda x: "|".join(map(str, x)),
                         pad_sequence([torch.from_numpy(np.array(x)) for x in data[col]], batch_first=True).numpy()))

    return data, key2index, max_len

# TODO：标签编码和归一化修改为spark方式
def labelEncodeDF(df, dfColumn, inputColumn, outputColumn, savePath, flag=True):
    '''
    label编码
    :param df: 数据框
    :param inputColumn: 待转换列名
    :param outputColumn: 编码后列名
    :param savePath: 编码器保存路径
    :param flag: 是否保存
    :return:
    '''
    stringIndexer = StringIndexer(inputCol=inputColumn, outputCol=outputColumn).setHandleInvalid("keep")
    label_model = stringIndexer.fit(df)
    df = label_model.transform(df)
    index = dfColumn.index(inputColumn)
    dfColumn[index] = outputColumn
    df = df.select(dfColumn).withColumnRenamed(outputColumn, inputColumn)
    dfColumn[index] = inputColumn
    if flag:
        label_model.write().overwrite().save(savePath)

    return df
