# encoding: utf-8

from model_predict import start_model_predict

predict_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_predict_standarddata"
model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest.model"
output_file_name = "result.csv"
start_model_predict(predict_table_name, model_hdfs_path, output_file_name)

