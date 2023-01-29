#!/usr/bin/env python3
# encoding: utf-8

train_data_table_name = "algorithm.tmp_aip_train_data"
test_data_table_name = "algorithm.tmp_aip_test_data"

dnn_hidden_units = (256, 128, 64)
dnn_dropout = 0.2
batch_size = 256
lr = 0.01
is_train = True
# train(train_data_table_name, test_data_table_name,
#       dnn_dropout=dnn_dropout,
#       batch_size=batch_size, lr=lr,
#       is_automl=is_train
#       )


import json
import os

os.environ["global_params"] = json.dumps(
    {"op_name": {
        "raw_sample_table_name": "algorithm.tmp_aip_sample",
        "model_sample_table_name": "algorithm.tmp_aip_model_sample",
    }})
os.environ["name"] = "op_name"

os.environ["train_dataset_table_name"] = "algorithm.train_dataset_table_name"
os.environ["test_dataset_table_name"] = "algorithm.test_dataset_table_name"
from main import run

run()
