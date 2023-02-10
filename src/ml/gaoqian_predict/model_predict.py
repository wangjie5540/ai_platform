#!/usr/bin/env python3
# encoding: utf-8
import datetime

import pandas as pd
import joblib
from digitforce.aip.common.utils.spark_helper import spark_client
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
from digitforce.aip.common.utils import cos_helper

DATE_FORMAT = "%Y%m%d"
today = datetime.datetime.today().strftime(DATE_FORMAT)
hdfs_client = hdfs_helper.HdfsClient()


def start_model_predict(predict_table_name, model_hdfs_path, output_file_name):
    # dt = spark_client.get_session().sql(
    #     f"show partitions {predict_table_name}").collect()[-1][0][3:]
    # print(dt)
    df_predict = spark_client.get_session().sql(
        "select * from {} ".format(predict_table_name)).toPandas()
    df_predict.dropna(axis=0, subset=['user_id'], inplace=True)

    for col in df_predict.columns:
        if df_predict[col].dtypes == "object":
            df_predict[col] = df_predict[col].astype(float)


    custom_list = df_predict['user_id'].values
    x_predict = df_predict.drop(columns=['user_id', 'label'], axis=1)
    if type(custom_list[0]) is not str:
        custom_list = [str(int(custom_id)) for custom_id in custom_list]

    # 模型加载
    local_file_path = "./model"
    read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)
    model = joblib.load(local_file_path)

    # 预测打分
    y_pred_score = [x[1] for x in model.predict_proba(x_predict)]
    result = pd.DataFrame({'custom_id': custom_list, 'score': y_pred_score})
    result = result.drop_duplicates('custom_id')
    result.sort_values(by="score", inplace=True, ascending=False)
    print(result.head(5))

    # 结果存储
    result_local_path = "result.csv"
    # result_hdfs_path = "/user/ai/aip/zq/gaoqian/result/{}_gaoqian_result.csv".format(today)
    result.to_csv(result_local_path, index=False,header=False)
    # write_hdfs_path(result_local_path, result_hdfs_path, hdfs_client)
    output_file_path = cos_helper.upload_file("result.csv", output_file_name)

# 读hdfs
def read_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.copy_to_local(hdfs_path, local_path)

# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)
