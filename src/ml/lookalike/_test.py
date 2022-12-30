#!/usr/bin/env python3
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
