#!/usr/bin/env python3
# encoding: utf-8

from model_predict import start_model_predict

predict_table_name = "algorithm.aip_zq_liushi_custom_feature_predict"
model_hdfs_path = "/user/ai/aip/zq/liushi/model/lasted.model"
output_file_name = "result.csv"
instance_id = 112233
predict_score_table_name = "score_112233"
shapley_table_name = "shapley_112233"
start_model_predict(predict_table_name, model_hdfs_path, output_file_name,
                    instance_id, predict_score_table_name, shapley_table_name,
)