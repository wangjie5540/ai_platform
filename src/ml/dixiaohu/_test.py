#!/usr/bin/env python3
# encoding: utf-8

from model_train import start_model_train

train_table_name = "algorithm.aip_zq_liushi_custom_feature_train"
test_table_name = "algorithm.aip_zq_liushi_custom_feature_test"
learning_rate = 0.05
n_estimators = 200
max_depth = 5
scale_pos_weight = 0.5
# start_model_train(train_table_name, test_table_name,
#                   learning_rate=learning_rate, n_estimators=n_estimators, max_depth=max_depth,
#                   scale_pos_weight=scale_pos_weight,
#                   is_automl=False)

# for test
import os
import json

train_table_name = "algorithm.aip_zq_liushi_custom_feature_train"
test_table_name = "algorithm.aip_zq_liushi_custom_feature_test"
learning_rate = 0.05
n_estimators = 200
max_depth = 5
scale_pos_weight = 0.5
os.environ["global_params"] = json.dumps(
    {"op_name": {
        "train_data": train_table_name,
        "learning_rate": learning_rate,
        "test_data": test_table_name,
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "scale_pos_weight": scale_pos_weight,
        "is_automl": False,
        "model_and_metrics_data_hdfs_path": "/user/ai/aip/tmp"
    }})
os.environ["name"] = "op_name"
from main import run

run()
