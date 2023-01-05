#!/usr/bin/env python3
# encoding: utf-8

from model_predict import start_model_predict

predict_table_name = "algorithm.aip_zq_liushi_custom_feature_predict"
model_hdfs_path = "/user/ai/aip/zq/liushi/model/lasted.model"
start_model_predict(predict_table_name, model_hdfs_path)