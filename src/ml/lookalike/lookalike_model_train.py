#!/usr/bin/env python3
# encoding: utf-8
import time
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.hive_helper import hive_client
import numpy as np
import torch
import torch.optim.adam as adam
from sklearn.metrics import log_loss, roc_auc_score

from digitforce.aip.common.aip_feature.zq_feature import *

from model.dssm import DSSM
from preprocessing.inputs import SparseFeat, DenseFeat, VarLenSparseFeat

dnn_hidden_units = (256, 128, 64)


def start_model_train(train_data_table_name, test_data_table_name,
                      dnn_dropout=0.2,
                      batch_size=256, lr=0.01,
                      is_train=True):
    print("start model train")
    print("read train data and test data")
    train_data = hive_client.query_to_df(
        f"""select * from {train_data_table_name}""")
    train_data.columns = [_.split(".")[-1] for _ in train_data.columns]
    test_data = hive_client.query_to_df(
        f"""select * from {test_data_table_name}""")
    test_data.columns = [_.split(".")[-1] for _ in test_data.columns]
    print("get train test input")
    show_all_encoder()

    user_sequence_feature_and_max_len_map = {"u_buy_list": 5}
    user_feature_columns, item_feature_columns = get_dssm_feature_columns(user_sequence_feature_and_max_len_map)
    feature_columns = user_feature_columns + item_feature_columns
    feature_names = [_.name for _ in feature_columns]
    train_model_input = dataset_to_dssm_model_input(train_data, feature_names, user_sequence_feature_and_max_len_map)
    test_model_input = dataset_to_dssm_model_input(test_data, feature_names, user_sequence_feature_and_max_len_map)

    model = build_model(user_feature_columns, item_feature_columns, dnn_dropout, lr)

    print("model fit")
    start = time.time()
    model.fit(train_model_input, train_data["label"].values, batch_size=batch_size,
              epochs=3, verbose=2, validation_split=0.2)
    end = time.time()
    print("model training takes {} seconds".format(end - start))

    print("model evaluate")
    # 评估
    state_dict = torch.load("./model_zoo/model.pth")
    model.load_state_dict(state_dict['model_state_dict'], strict=False)

    pred_ts = model.predict(test_model_input, batch_size=batch_size)
    print("test-logloss={:.4f}, test-auc={:.4f}".format(log_loss(test_data["label"].values, pred_ts),
                                                        roc_auc_score(test_data["label"].values, pred_ts)))

    if is_train:
        model_hdfs_path = "/user/aip/aip/lookalike" + "model.pth"
        if hdfs_client.exists(model_hdfs_path):
            hdfs_client.delete(model_hdfs_path)
        hdfs_client.copy_from_local("./model_zoo/model.pth", model_hdfs_path)
        # todo save user embeding to hive


def build_model(user_feature_columns, item_feature_columns, dnn_dropout, lr):
    logging.info("model building")
    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        logging.info('cuda ready...')
        device = 'cuda:0'
    model = DSSM(user_feature_columns, item_feature_columns, dnn_hidden_units=dnn_hidden_units,
                 dnn_dropout=dnn_dropout, task='binary', device=device)
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

    for user_v , maxlen in user_sequence_feature_and_max_len_map.items():  # [u_buy_list]
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
    from lookalike_model_train import start_model_train

    train_data_table_name = "algorithm.tmp_aip_train_data"
    test_data_table_name = "algorithm.tmp_aip_test_data"

    dnn_hidden_units = (256, 128, 64)
    dnn_dropout = 0.2
    batch_size = 256
    lr = 0.01
    is_train = True
    start_model_train(train_data_table_name, test_data_table_name,
                      dnn_dropout=dnn_dropout,
                      batch_size=batch_size, lr=lr,
                      is_train=is_train
                      )


if __name__ == '__main__':
    main()
