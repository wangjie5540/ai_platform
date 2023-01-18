#!/usr/bin/env python3
# encoding: utf-8
import time
from digitforce.aip.common.utils.hive_helper import hive_client

import numpy as np
import torch
import torch.optim.adam as adam
from sklearn.metrics import log_loss, roc_auc_score

from digitforce.aip.common.aip_feature.zq_feature import *
from model.dssm import DSSM
from preprocessing.inputs import SparseFeat, DenseFeat, VarLenSparseFeat

dnn_hidden_units = (256, 128, 64)
DEVICE = 'cpu'
use_cuda = True
if use_cuda and torch.cuda.is_available():
    logging.info('cuda ready...')
    DEVICE = 'cuda:0'


def train(train_data_table_name, test_data_table_name,
          dnn_dropout=0.2,
          batch_size=256, lr=0.01,
          is_automl=False,
          model_user_feature_table_name=None,
          user_vec_table_name=None):
    if not is_automl and model_user_feature_table_name is None:
        raise ValueError(
            f"train model on TRAIN the model_user_feature_table_name must be set... {model_user_feature_table_name}")
    print(f"begin key args "
          f"train_data_table_name:{train_data_table_name}, "
          f"test_data_table_name:{test_data_table_name}, "
          f"is_automl:{is_automl}, "
          f"model_user_feature_table_name:{model_user_feature_table_name}")
    print("read train dataset")
    train_data = hive_client.query_to_df(
        f"""select * from {train_data_table_name}""")
    train_data.columns = [_.split(".")[-1] for _ in train_data.columns]
    print("read test dataset")
    test_data = hive_client.query_to_df(
        f"""select * from {test_data_table_name}""")
    test_data.columns = [_.split(".")[-1] for _ in test_data.columns]

    print("show user_feature_encoder and item_feature_encoder")
    show_all_encoder()

    user_sequence_feature_and_max_len_map = {"u_buy_list": 5}
    print(f"build DSSM model_feature...")
    user_feature_columns, item_feature_columns = get_dssm_feature_columns(user_sequence_feature_and_max_len_map)
    feature_columns = user_feature_columns + item_feature_columns
    feature_names = [_.name for _ in feature_columns]
    print(f"feature names:{feature_names}")
    print(f"dataset to model input...")
    train_model_input = dataset_to_dssm_model_input(train_data, feature_names, user_sequence_feature_and_max_len_map)
    test_model_input = dataset_to_dssm_model_input(test_data, feature_names, user_sequence_feature_and_max_len_map)

    print("build DSSM model with feature and super params")
    model = build_model(user_feature_columns, item_feature_columns, dnn_dropout, lr)

    print("begin fit model...")
    start = time.time()
    model.fit(train_model_input, train_data["label"].values, batch_size=batch_size,
              epochs=3, verbose=2, validation_split=0.2)
    end = time.time()
    print("model training takes {} seconds".format(end - start))

    print("begin  evaluate model...")
    # 评估
    state_dict = torch.load("./model_zoo/model.pth")
    model.load_state_dict(state_dict['model_state_dict'], strict=False)

    pred_ts = model.predict(test_model_input, batch_size=batch_size)
    print("test-logloss={:.4f}, test-auc={:.4f}".format(log_loss(test_data["label"].values, pred_ts),
                                                        roc_auc_score(test_data["label"].values, pred_ts)))

    if not is_automl:
        # 获取单塔 user tower
        dict_trained = model.state_dict()
        user_tower_model = DSSM(user_feature_columns, [], dnn_hidden_units=dnn_hidden_units,
                                dnn_dropout=dnn_dropout, task='binary', device=DEVICE)
        dict_user = user_tower_model.state_dict()
        # todo 读model_user_feature 表
        print(f"begin predict all user in {model_user_feature_table_name}")
        print(f"begin read model_user_feature_table ")
        model_user_feature_dataset = hive_client.query_to_df(
            f"""select * from {model_user_feature_table_name}""")
        model_user_feature_dataset.columns = [_.split(".")[-1] for _ in model_user_feature_dataset.columns]
        all_user_model_input = dataset_to_dssm_model_input(model_user_feature_dataset,
                                                           [_.name for _ in user_feature_columns],
                                                           user_sequence_feature_and_max_len_map)

        for key in dict_user:
            dict_user[key] = dict_trained[key]
        user_tower_model.load_state_dict(dict_user, strict=False)  # load trained model parameters of user tower

        user_embedding = user_tower_model.predict(all_user_model_input, batch_size=batch_size)
        user_vec_df = model_user_feature_dataset[["user_id_raw"]]
        user_vec_df["user_vec"] = [_.tolist() for _ in list(user_embedding)]
        from digitforce.aip.common.utils.spark_helper import spark_client
        print("upload user_vec to hive")
        # todo 测试一下 pandasDF -> 保存成csv-> 传到hdfs-> 转成sparkDataframe-> 存表
        user_vec_dataframe = spark_client.get_session().createDataFrame(user_vec_df)
        if user_vec_table_name is None:
            user_vec_table_name = "algorithm.lookalike_user_vec_table"
        user_vec_dataframe.write.format("hive").mode("overwrite").saveAsTable(user_vec_table_name)
        model_hdfs_path = "/user/aip/aip/lookalike" + "model.pth"

        if hdfs_client.exists(model_hdfs_path):
            hdfs_client.delete(model_hdfs_path)
        hdfs_client.copy_from_local("./model_zoo/model.pth", model_hdfs_path)


def build_model(user_feature_columns, item_feature_columns, dnn_dropout, lr):
    logging.info("model building")
    model = DSSM(user_feature_columns, item_feature_columns, dnn_hidden_units=dnn_hidden_units,
                 dnn_dropout=dnn_dropout, task='binary', device=DEVICE)
    optim = adam.Adam(model.parameters(), lr=lr)
    model.compile(optim, "binary_crossentropy", metrics=['auc'])
    return model


# 获取训练集、测试集输入
def __sequence_to_fixed_len_sequence(x, length):
    res = [int(i) for i in x.split("|")]
    if len(res) < length:
        for _ in range(length - len(res)):
            res.append(0)
    return res


def get_dssm_feature_columns(user_sequence_feature_and_max_len_map):
    # user dense and sparse feature
    user_model_feature_names = user_feature_factory.get_encoder_names()
    user_parse_feature_names = []
    user_dense_feature_names = []
    for user_feature_name in user_model_feature_names:
        encoder = user_feature_factory.get_encoder(user_feature_name)
        if isinstance(encoder, CategoryFeatureEncoder):
            user_parse_feature_names.append(user_feature_name)
        elif isinstance(encoder, NumberFeatureEncoder):
            user_dense_feature_names.append(user_feature_name)

    user_feature_columns = []
    for feat in user_parse_feature_names:
        pf = SparseFeat(feat, len(user_feature_factory.get_encoder(feat).vocabulary) + 1, embedding_dim=4)
        user_feature_columns.append(pf)
    user_feature_columns += [DenseFeat(feat, 1, ) for feat in
                             user_dense_feature_names]

    # item dense and sparse feature todo 代码重复
    item_model_feature_names = item_feature_factory.get_encoder_names()
    item_parse_feature_names = []
    item_dense_feature_names = []
    for feature_name in item_model_feature_names:
        encoder = item_feature_factory.get_encoder(feature_name)
        if isinstance(encoder, CategoryFeatureEncoder):
            item_parse_feature_names.append(feature_name)
        elif isinstance(encoder, NumberFeatureEncoder):
            item_dense_feature_names.append(feature_name)

    item_feature_columns = []
    for feat in item_parse_feature_names:
        pf = SparseFeat(feat, len(item_feature_factory.get_encoder(feat).vocabulary) + 1, embedding_dim=4)
        item_feature_columns.append(pf)
    item_feature_columns += \
        [DenseFeat(feat, 1, ) for feat in
         item_dense_feature_names]
    ## sequence feature
    # user sequence feature

    for user_v, maxlen in user_sequence_feature_and_max_len_map.items():  # [u_buy_list]
        user_varlen_feature = \
            VarLenSparseFeat(SparseFeat(user_v, len(user_feature_factory.get_encoder("user_id").vocabulary)),
                             maxlen=maxlen, combiner='mean', length_name=None)
        user_feature_columns.append(user_varlen_feature)

    return user_feature_columns, item_feature_columns


def dataset_to_dssm_model_input(dataset, feature_names, user_sequence_feature_and_max_len_map):
    # build dssm model input
    model_input = {name: dataset[name] for name in feature_names}
    for user_v, max_len in user_sequence_feature_and_max_len_map.items():
        model_input[user_v] = np.array(
            list(dataset[user_v].map(lambda x: __sequence_to_fixed_len_sequence(x, max_len)).values))
    return model_input


def main():
    # !/usr/bin/env python3
    # encoding: utf-8

    train_data_table_name = "algorithm.tmp_aip_train_data"
    test_data_table_name = "algorithm.tmp_aip_test_data"
    model_user_feature_table_name = "algorithm.tmp_model_user_feature_table_name"
    user_vec_table_name = "algorithm.tmp_user_vec_table_name"
    dnn_hidden_units = (256, 128, 64)
    dnn_dropout = 0.2
    batch_size = 256
    lr = 0.01
    train(train_data_table_name, test_data_table_name,
          dnn_dropout=dnn_dropout,
          batch_size=batch_size, lr=lr,
          is_automl=False,
          model_user_feature_table_name=model_user_feature_table_name,
          user_vec_table_name=user_vec_table_name
          )


if __name__ == '__main__':
    main()

