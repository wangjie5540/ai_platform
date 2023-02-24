#!/usr/bin/env python3
# encoding: utf-8
from lookalike_model_train import train
train_data_table_name = "algorithm.train_dataset_table_name"
test_data_table_name = "algorithm.test_dataset_table_name"
#
dnn_hidden_units = (256, 128, 64)
dnn_dropout = 0.2
batch_size = 256
lr = 0.01
is_automl = False

model_user_feature_table_name = "algorithm.tmp_model_user_feature_table_name"
user_vec_table_name = "algorithm.tmp_user_vec_table_name"
model_and_metrics_data_hdfs_path = "/user/ai/aip/model/112233"
train(train_data_table_name, test_data_table_name,
      dnn_dropout=dnn_dropout,
      batch_size=batch_size, lr=lr,
      is_automl=is_automl,
      model_user_feature_table_name=model_user_feature_table_name,
      user_vec_table_name=user_vec_table_name,
      model_and_metrics_data_hdfs_path=model_and_metrics_data_hdfs_path
)
print(123)
# from digitforce.aip.common.utils.spark_helper import SparkClient
# spark_client = SparkClient.get()
# df = spark_client.get_session().sql(f"""select * from {train_data_table_name}""")
# print(df)
# # zq_standard