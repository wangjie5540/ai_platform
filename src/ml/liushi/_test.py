#!/usr/bin/env python3
# encoding: utf-8
# from pyspark.sql import SparkSession

from digitforce.aip.common.utils import component_helper
from model_train import start_model_train

train_table_name = "algorithm.aip_zq_liushi_custom_feature_train"
test_table_name = "algorithm.aip_zq_liushi_custom_feature_test"
model_and_metrics_data_hdfs_path = "/user/ai/aip/model/112233"
learning_rate = 0.05
n_estimators = 200
max_depth = 5
scale_pos_weight = 0.5
start_model_train(train_table_name, test_table_name,
                  learning_rate=learning_rate, n_estimators=n_estimators, max_depth=max_depth,
                  scale_pos_weight=scale_pos_weight,
                  is_automl=False,
                  model_and_metrics_data_hdfs_path= model_and_metrics_data_hdfs_path)

# for test
# import os
# import json
#
# train_table_name = "algorithm.aip_zq_liushi_custom_train_1"
# test_table_name = "algorithm.aip_zq_liushi_custom_test_1"
# learning_rate = 0.05
# n_estimators = 200
# max_depth = 5
# scale_pos_weight = 0.5
# os.environ["global_params"] = json.dumps(
#     {"op_name": {
#         "train_data": train_table_name,
#         "learning_rate": learning_rate,
#         "test_data": test_table_name,
#         "n_estimators": n_estimators,
#         "max_depth": max_depth,
#         "scale_pos_weight": scale_pos_weight,
#         "is_automl": False,
#         "model_and_metrics_data_hdfs_path": "/user/ai/aip/tmp"
#     }})
# os.environ["name"] = "op_name"
# from main import run
# from digitforce.aip.common.utils.spark_helper import *
# client_host = ip_helper.get_local_ip()
# spark_client._session =  SparkSession.builder.appName(component_helper.get_component_app_name()) \
#             .master(spark_config['master_uri']) \
#             .config("spark.driver.host", client_host) \
#             .config("spark.kubernetes.container.image", spark_config['kubernetes_runtime_image']) \
#             .config("spark.kubernetes.container.image.pullPolicy", "Always") \
#             .config("spark.sql.autoBroadcastJoinThreshold", -1) \
#             .config("spark.executor.instances", "2") \
#             .config("spark.debug.maxToStringFields", 100) \
#             .config("spark.executor.cores", "1") \
#             .config("spark.executor.memory", "16g") \
#             .config("spark.rpc.message.maxSize", 1000) \
#             .config("spark.driver.memory", "32g") \
#             .config(" spark.driver.maxResultSize", "16g") \
#             .config("spark.driver.cores", "1") \
#             .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") \
#             .config("hive.metastore.uris", spark_config['hive_uris']) \
#             .config("spark.submit.pyFiles", SUBMIT_ZIP_PATH) \
#             .enableHiveSupport().getOrCreate()
# run()
