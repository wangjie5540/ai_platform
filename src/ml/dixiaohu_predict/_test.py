# encoding: utf-8

from model_predict import start_model_predict

predict_feature_table_name = "algorithm.aip_zq_dixiaohu_custom_feature_predict_standarddata"
model_hdfs_path = "/user/ai/aip/model/222/model.pk"
output_file_name = "result.csv"
instance_id = 3
predict_table_name = "score_241"
start_model_predict(predict_feature_table_name=predict_feature_table_name
                    , model_hdfs_path=model_hdfs_path
                    , output_file_name=output_file_name
                    , instance_id=instance_id
                    , predict_table_name=predict_table_name)

