# encoding: utf-8
from digitforce.aip.common.utils import cos_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import datetime

import pandas as pd
import joblib
from digitforce.aip.common.utils.spark_helper import SparkClient
import os
# os.environ['SPARK_HOME'] = '/opt/spark-2.4.8-bin-hadoop2.7'
import findspark
findspark.init()

DATE_FORMAT = "%Y%m%d"
now = datetime.datetime.today()
today = now.strftime(DATE_FORMAT)
predict_day = (now - datetime.timedelta(days=5)
               ).strftime(DATE_FORMAT)  # 预测的日期为5天前，防止没有数据
hdfs_client = hdfs_helper.HdfsClient()


def start_model_predict(predict_table_name, model_hdfs_path, output_file_name):
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    df_predict = (
        spark
        .sql("select * from {} where dt = {}".format(predict_table_name, predict_day))
        .toPandas()
    )

    # 连续特征，离散特征，丢弃特征等的处理
    drop_features = []
    categorical_features = [
        "sex",
        "city_name",
        "province_name",
        "educational_degree",
        "is_login",
    ]
    float_features = [
        "age",
        "transfer_out_amt",
        "transfer_in_amt",
        "transfer_out_cnt",
        "transfer_in_cnt",
        "total_tran_cnt",
        "total_tran_amt",
        "gp_tran_cnt",
        "gp_tran_amt",
        "jj_tran_cnt",
        "jj_tran_amt",
        "zq_tran_cnt",
        "zq_tran_amt",
        "total_ast",
        "net_ast",
        "total_liab",
        "unmoney_fnd_val",
        'stock_ast',
        'cash_bal',
        'total_prd_ast',
    ]
    df_predict = featuretype_process(
        data_init=df_predict,
        drop_labels=drop_features,
        categorical_feature=categorical_features,
        float_feature=float_features,
    )

    custom_list = df_predict["cust_code"].values
    x_predict = df_predict.drop(columns=["cust_code", "label", "dt"], axis=1)

    # 模型加载
    local_file_path = "./model"
    read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)
    model = joblib.load(local_file_path)

    # 预测打分
    y_pred_score = [x[1] for x in model.predict_proba(x_predict)]
    result = pd.DataFrame({"cust_code": custom_list, "score": y_pred_score})
    result.sort_values(by="score", inplace=True, ascending=False)

    # 结果存储
    result_local_path = "result.csv"
    result_hdfs_path = "/user/ai/aip/zq/dixiaohu/result/{}_dixiaohu_result.csv".format(
        predict_day
    )
    result.to_csv(result_local_path, index=False, header=False)
    write_hdfs_path(result_local_path, result_hdfs_path, hdfs_client)
    output_file_path = cos_helper.upload_file("result.csv", output_file_name)
    print("-----*****", output_file_path)


# 读hdfs
def read_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.copy_to_local(hdfs_path, local_path)


# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)


# 处理分类特征，数值特征，需要丢掉的特征
def featuretype_process(data_init, drop_labels, categorical_feature, float_feature):
    """TODO:自动识别连续特征和离散特征"""
    # Process Feature 1
    data_process = data_init.drop(
        labels=drop_labels, axis=1, errors="ignore"
    )
    data_process[categorical_feature] = (
        data_process[categorical_feature].astype("str").astype("category")
    )
    # data_process[float_feature] = data_process[float_feature].astype('float')
    data_process[float_feature] = data_process[float_feature].apply(
        lambda col: pd.to_numeric(col, errors="coerce"), axis=0
    )
    return data_process
