# encoding: utf-8
from digitforce.aip.common.utils import cos_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
import datetime

import pandas as pd
import joblib

from digitforce.aip.common.utils.time_helper import DATE_FORMAT
from digitforce.aip.common.utils.spark_helper import SparkClient

now = datetime.datetime.today()
today = now.strftime(DATE_FORMAT)
predict_day = (now - datetime.timedelta(days=5)).strftime(
    DATE_FORMAT
)  # 预测的日期为5天前，防止没有数据
hdfs_client = hdfs_helper.HdfsClient()


def start_model_predict(
        predict_table_name: str, model_hdfs_path: str, output_file_name: str
):
    """
    预测过程
    Args:
        predict_table_name:
        model_hdfs_path:
        output_file_name:
    """
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    print("spark init-----------------")
    df_predict = spark.sql(
        f"""
        select * from {predict_table_name} where dt = '{predict_day}'
        """
    ).toPandas()
    print("predict_day---------", predict_day)
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
        "stock_ast",
        "cash_bal",
        "total_prd_ast",
    ]
    df_predict = featuretype_process(
        data_init=df_predict,
        drop_labels=drop_features,
        categorical_feature=categorical_features,
        float_feature=float_features,
    )
    print("预测数据规模", len(df_predict))

    # 模型加载
    local_file_path = "./model"
    read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)
    model = joblib.load(local_file_path)

    # 预测打分
    custom_list = df_predict["cust_code"].values
    x_predict = df_predict[model.feature_name_]
    pred_proba = model.predict_proba(x_predict)

    y_pred_score = pred_proba[:, 1]
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
    print("output_file_path-----*****", output_file_path)

    from digitforce.aip.common.utils.explain_helper import explain_main  # shap和spark存在兼容性冲突，须放在spark_client后使用
    # 计算ale值和shap值并存储
    ale_df, shap_df = explain_main(
        df_predict.drop(columns=["label", "dt"]), model, categorical_features
    )
    # 存储ale
    ale_local_path = "ale.csv"
    ale_hdfs_path = "/user/ai/aip/zq/dixiaohu/explain/{}_ale.csv".format(predict_day)
    ale_df.to_csv(ale_local_path, index=False, header=False)
    write_hdfs_path(ale_local_path, ale_hdfs_path, hdfs_client)
    print("ale_ 计算存储完成-----*****", ale_hdfs_path)

    # 存储shap
    shap_local_path = "shap.csv"
    shap_hdfs_path = "/user/ai/aip/zq/dixiaohu/explain/{}_shap.csv".format(predict_day)
    shap_df.to_csv(shap_local_path, index=False, header=False)
    write_hdfs_path(shap_local_path, shap_hdfs_path, hdfs_client)
    print("shap 计算存储完成-----*****", shap_hdfs_path)


# 读hdfs
def read_hdfs_path(local_path, hdfs_path, client):
    if client.exists(hdfs_path):
        client.copy_to_local(hdfs_path, local_path)


# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, client):
    if client.exists(hdfs_path):
        client.delete(hdfs_path)
    client.copy_from_local(local_path, hdfs_path)


# 处理分类特征，数值特征，需要丢掉的特征
def featuretype_process(data_init, drop_labels, categorical_feature, float_feature):
    """TODO:自动识别连续特征和离散特征"""
    # Process Feature 1
    data_process = data_init.drop(labels=drop_labels, axis=1, errors="ignore")
    data_process[categorical_feature] = (
        data_process[categorical_feature].astype("str").astype("category")
    )
    # data_process[float_feature] = data_process[float_feature].astype('float')
    data_process[float_feature] = data_process[float_feature].apply(
        lambda col: pd.to_numeric(col, errors="coerce"), axis=0
    )
    return data_process
