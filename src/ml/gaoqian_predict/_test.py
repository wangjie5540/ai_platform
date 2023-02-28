#!/usr/bin/env python3
# encoding: utf-8

from model_predict import start_model_predict

predict_table_name = "algorithm.tmp_aip_user_feature_gaoqian_predict"
model_hdfs_path = "/user/ai/aip/zq/gaoqian/model/lasted.model"
output_file_name = "result.csv"
start_model_predict(predict_table_name, model_hdfs_path, output_file_name)