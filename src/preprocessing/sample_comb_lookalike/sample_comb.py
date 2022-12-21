#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.spark_helper as spark_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import pickle
import collections
import random
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType

hdfs_client = hdfs_helper.HdfsClient()


def sample_comb(sample_table_name, sample_columns,
                user_feature_table_name, user_columns,
                item_feature_table_name, item_columns):
    spark_client = spark_helper.SparkClient()
    print("read data")
    sample = spark_client.get_session().sql(f"""select {",".join(sample_columns)} from {sample_table_name}""")
    user_feature = spark_client.get_session().sql(f"""select {",".join(user_columns)} from {user_feature_table_name}""")
    item_feature = spark_client.get_session().sql(f"""select {",".join(item_columns)} from {item_feature_table_name}""")

    user_id_sample = sample_columns[0]
    item_id_sample = sample_columns[1]
    user_id = user_columns[0]
    item_id = item_columns[0]

    user_feature = user_feature.withColumnRenamed(user_id, user_id_sample)
    item_feature = item_feature.withColumnRenamed(item_id, item_id_sample)

    print("data join")
    data = sample.join(item_feature, item_id_sample, "left")
    data = data.join(user_feature, user_id_sample, "left")
    columns = data.columns

    print("train data preprocessing")
    # TODO：后续完善hdfs_helper组件
    hdfs_dir = '/user/ai/aip/lookalike/'
    data = train_data_preprocessing(data, hdfs_dir)

    print("train test split")
    data = data.rdd.map(lambda x: (x, random.random()))
    train_test_threshold = 0.8
    train_data = data.filter(lambda x: x[1] < train_test_threshold).map(lambda x: x[0]).toDF(columns)
    test_data = data.filter(lambda x: x[1] >= train_test_threshold).map(lambda x: x[0]).toDF(columns)

    print("user data preprocessing")
    user_data = user_data_preprocessing(user_feature, hdfs_dir)

    data_table_columns = train_data.columns
    user_data_table_columns = user_data.columns

    print("train data saveAsTable")
    train_data_table_name = "algorithm.tmp_aip_train_data"
    train_data.write.format("hive").mode("overwrite").saveAsTable(train_data_table_name)

    print("test data saveAsTable")
    test_data_table_name = "algorithm.tmp_aip_test_data"
    test_data.write.format("hive").mode("overwrite").saveAsTable(test_data_table_name)

    print("user data saveAsTable")
    user_data_table_name = "algorithm.tmp_aip_user_data"
    user_data.write.format("hive").mode("overwrite").saveAsTable(user_data_table_name)

    print("Mission Finished")

    return train_data_table_name, test_data_table_name, user_data_table_name, data_table_columns, user_data_table_columns, hdfs_dir


def train_data_preprocessing(data, hdfs_path):
    sparse_features = ['fund_type', 'management', 'custodian', 'invest_type', 'gender',
                       'EDU', 'RSK_ENDR_CPY', 'NATN',
                       'OCCU', 'IS_VAIID_INVST']
    dense_features = ['i_buy_counts_30d', 'i_amount_sum_30d', 'i_amount_avg_30d', 'i_amount_min_30d',
                      'i_amount_max_30d', 'u_buy_counts_30d',
                      'u_amount_sum_30d', 'u_amount_avg_30d', 'u_amount_min_30d', 'u_amount_max_30d', 'u_buy_days_30d',
                      'u_buy_avg_days_30d', 'u_last_buy_days_30d']
    id_features = [['user_id'], ['item_id', 'u_buy_list']]

    fill_dic = collections.defaultdict()
    for feat in sparse_features:
        fill_dic[feat] = "unknown"
    for feat in dense_features:
        fill_dic[feat] = 0
        if feat == "u_last_buy_days_30d":
            fill_dic[feat] = 90
    fill_dic["u_buy_list"] = ""
    data = data.fillna(fill_dic)

    data = feat_label_encoder(data, sparse_features, hdfs_path + f"sparse_features_dict.pkl", True, True)
    data = feat_minmax_scaler(data, dense_features, hdfs_path + f"dense_features_dict.pkl", True, True)
    data = id_label_encoder(data, id_features, hdfs_path + f"id_features_dict.pkl", True, True, False)

    return data


def user_data_preprocessing(user_data, hdfs_path):
    user_sparse_features, user_dense_features = ['gender', 'EDU', 'RSK_ENDR_CPY', 'NATN',
                                                 'OCCU', 'IS_VAIID_INVST'], ['u_buy_counts_30d',
                                                                             'u_amount_sum_30d', 'u_amount_avg_30d',
                                                                             'u_amount_min_30d', 'u_amount_max_30d',
                                                                             'u_buy_days_30d',
                                                                             'u_buy_avg_days_30d',
                                                                             'u_last_buy_days_30d']
    id_features = [['user_id'], ['item_id', 'u_buy_list']]

    fill_dic = collections.defaultdict()
    for feat in user_sparse_features:
        fill_dic[feat] = "unknown"
    for feat in user_dense_features:
        fill_dic[feat] = 0
        if feat == "u_last_buy_days_30d":
            fill_dic[feat] = 90
    fill_dic["u_buy_list"] = ""
    user_data = user_data.fillna(fill_dic)

    user_data = feat_label_encoder(user_data, user_sparse_features, hdfs_path + f"sparse_features_dict.pkl", False,
                                   True)
    user_data = feat_minmax_scaler(user_data, user_dense_features, hdfs_path + f"dense_features_dict.pkl", False, True)
    user_data = id_label_encoder(user_data, id_features, hdfs_path + f"id_features_dict.pkl", False, True, True)

    return user_data


def feat_label_encoder(data, cols, save_path, is_fit=True, flag=True):
    dir = "./sparse_features_dict.pkl"
    if is_fit:
        sparse_features_dict = collections.defaultdict()
        for col in cols:
            feats_list = data.select(col).rdd.distinct().map(lambda x: x[0]).collect()
            feats_map = collections.defaultdict()
            index = 1
            for feat in feats_list:
                feats_map[feat] = index
                index += 1
            feats_map["unknown"] = 0
            sparse_features_dict[col] = feats_map
    else:
        hdfs_client.copy_to_local(save_path, dir)
        with open(dir, "rb") as file:
            sparse_features_dict = pickle.load(file)

    for col in cols:
        def somefunc(value):
            if value not in sparse_features_dict[col].keys():
                value = "unknown"
            return sparse_features_dict[col][value]

        udfsomefunc = F.udf(somefunc, IntegerType())
        data = data.withColumn(col, udfsomefunc(col))

    if flag:
        with open(dir, "wb") as file:
            pickle.dump(sparse_features_dict, file)
        if hdfs_client.exists(save_path):
            hdfs_client.delete(save_path)
        hdfs_client.copy_from_local(dir, save_path)

    return data


def feat_minmax_scaler(data, cols, save_path, is_fit=True, flag=True):
    dir = "./dense_features_dict.pkl"
    if is_fit:
        dense_features_dict = collections.defaultdict()
        for col in cols:
            feats_list = data.select(F.min(col), F.max(col)).rdd.map(lambda x: (x[0], x[1])).collect()
            feats_min = feats_list[0][0]
            feats_max = feats_list[0][1]
            feats_scaler = {'column': col,
                            'min_value': feats_min,
                            'max_value': feats_max}
            dense_features_dict[col] = feats_scaler
    else:
        hdfs_client.copy_to_local(save_path, dir)
        with open(dir, "rb") as file:
            dense_features_dict = pickle.load(file)

    for col in cols:
        feats_min = dense_features_dict.get(col).get('min_value')
        feats_max = dense_features_dict.get(col).get('max_value')

        def min_max_scaler(value):
            value = (value - feats_min) / (feats_max - feats_min)
            return value

        udfsomefunc = F.udf(min_max_scaler, FloatType())
        data = data.withColumn(col, udfsomefunc(col))

    if flag:
        with open(dir, "wb") as file:
            pickle.dump(dense_features_dict, file)
        if hdfs_client.exists(save_path):
            hdfs_client.delete(save_path)
        hdfs_client.copy_from_local(dir, save_path)

    return data


def id_label_encoder(data, cols, save_path, is_fit=True, flag=True, only_user=False):
    dir = "./id_features_dict.pkl"
    if is_fit:
        id_features_dict = collections.defaultdict()
        for col in cols:
            if len(col) == 1:
                feats_list = data.select(col[0]).rdd.distinct().map(lambda x: x[0]).collect()
            if len(col) == 2:
                feats_list = data.select([col[0], col[1]]).rdd \
                    .map(lambda x: set([x[0]]).union(set(x[1].split("|")))) \
                    .reduce(lambda a, b: set(a).union(set(b)))
            feats_map = collections.defaultdict()
            index = 1
            for feat in feats_list:
                feats_map[feat] = index
                index += 1
            feats_map["unknown"] = 0
            id_features_dict[col[0]] = feats_map
    else:
        hdfs_client.copy_to_local(save_path, dir)
        with open(dir, "rb") as file:
            id_features_dict = pickle.load(file)

    for col in cols:
        def label_encoder(feat):
            if feat not in id_features_dict[col[0]].keys():
                feat = "unknown"
            return id_features_dict[col[0]][feat]

        def list_label_encoder(feat):
            value_list = feat.split("|")
            new_list = []
            for v in value_list:
                if v not in id_features_dict[col[0]].keys():
                    v = "unknown"
                new_list.append(str(id_features_dict[col[0]][v]))
            return "|".join(new_list)

        def pad_sequence(value_list):
            value_list = value_list.split("|")
            if len(value_list) != 5:
                fill_count = 5 - len(value_list)
                value_list = value_list + ["0"] * fill_count
            return "|".join(value_list)

        udf_id_func = F.udf(label_encoder, IntegerType())
        udf_list_func = F.udf(list_label_encoder, StringType())
        udf_padding_func = F.udf(pad_sequence, StringType())

        if len(col) == 1:
            data = data.withColumn(col[0], udf_id_func(col[0]))
        if len(col) == 2 and only_user:
            data = data.withColumn(col[1], udf_list_func(col[1]))
            data = data.withColumn(col[1], udf_padding_func(col[1]))
        if len(col) == 2 and not only_user:
            data = data.withColumn(col[0], udf_id_func(col[0]))
            data = data.withColumn(col[1], udf_list_func(col[1]))
            data = data.withColumn(col[1], udf_padding_func(col[1]))

    if flag:
        with open(dir, "wb") as file:
            pickle.dump(id_features_dict, file)
        if hdfs_client.exists(save_path):
            hdfs_client.delete(save_path)
        hdfs_client.copy_from_local(dir, save_path)
    return data
