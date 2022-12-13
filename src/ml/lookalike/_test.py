#!/usr/bin/env python3
# encoding: utf-8
from src.ml.lookalike.lookalike_model_train import start_model_train

train_data_table_name = "algorithm.tmp_aip_train_data"
test_data_table_name = "algorithm.tmp_aip_test_data"
user_data_table_name = "algorithm.tmp_aip_user_data"
hdfs_path = "/data/pycharm_project_950/src/preprocessing/sample_comb_lookalike/dir/"
dnn_hidden_units = (256, 128, 64)
dnn_dropout = 0.2
batch_size = 256
lr = 0.01
start_model_train(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_path,
                  dnn_hidden_units=dnn_hidden_units, dnn_dropout=dnn_dropout,
                  batch_size=batch_size, lr=lr
                  )
