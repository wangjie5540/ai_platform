# encoding: utf-8

from model_predict import start_model_predict

predict_feature_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_predict_standarddata"
# model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest_model.pk"
# model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/2023-04-10.model"
model_hdfs_path = "/user/ai/aip/zq/dixiaohu/model/latest_model.pickle.dat"
# print("model_hdfs_path:", model_hdfs_path)
output_file_name = "result.csv"
instance_id = 1646809101662289921
predict_table_name = "aip.score_254"
shapley_table_name = "aip.shapley_254"
start_model_predict(predict_feature_table_name=predict_feature_table_name
                    , model_hdfs_path=model_hdfs_path
                    , output_file_name=output_file_name
                    , instance_id=instance_id
                    , predict_table_name=predict_table_name
                    , shapley_table_name=shapley_table_name)
