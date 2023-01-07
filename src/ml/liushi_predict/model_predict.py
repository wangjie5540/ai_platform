#!/usr/bin/env python3
# encoding: utf-8
import datetime

import pandas as pd
import joblib
from digitforce.aip.common.utils.spark_helper import spark_client
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper

DATE_FORMAT = "%Y%m%d"
today = datetime.datetime.today().strftime(DATE_FORMAT)
hdfs_client = hdfs_helper.HdfsClient()


def start_model_predict(predict_table_name, model_hdfs_path):
    dt = "20230103" #todo read latest partition
    df_predict = spark_client.get_session().sql(
        "select * from {} where dt = {}".format(predict_table_name, dt)).toPandas()

    for col in df_predict.columns:
        if df_predict[col].dtypes == "object":
            df_predict[col] = df_predict[col].astype(float)

    custom_list = df_predict['custom_id'].values
    x_predict = df_predict.drop(columns=['custom_id', 'label', 'dt'], axis=1)

    # 模型加载
    local_file_path = "./model"
    read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)
    model = joblib.load(local_file_path)

    # 预测打分
    y_pred_score = [x[1] for x in model.predict_proba(x_predict)]
    result = pd.DataFrame({'custom_id': custom_list, 'score': y_pred_score})
    result.sort_values(by="score", inplace=True, ascending=False)

    # 结果存储
    result_local_path = "result.csv"
    result_hdfs_path = "/user/ai/aip/zq/liushi/result/{}_liushi_result.csv".format(today)
    result.to_csv(result_local_path, index=False)
    write_hdfs_path(result_local_path, result_hdfs_path, hdfs_client)

# 读hdfs
def read_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.copy_to_local(hdfs_path, local_path)

# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)
