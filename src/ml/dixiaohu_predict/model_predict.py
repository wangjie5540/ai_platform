# encoding: utf-8
import findspark

findspark.init()
from digitforce.aip.common.utils import cos_helper
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper

import pandas as pd
import numpy as np
import joblib

from digitforce.aip.common.utils.spark_helper import SparkClient

hdfs_client = hdfs_helper.HdfsClient()

from digitforce.aip.common.utils.starrocks_helper import write_score
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, StringType, FloatType
import json
from datetime import datetime


def start_model_predict(
        predict_feature_table_name: str, model_hdfs_path: str, output_file_name: str,
        instance_id: str, predict_table_name: str, shapley_table_name: str,
):
    """
    预测过程
    Args:
        shapley_table_name: shap分数存储表名
        instance_id: 预测任务id，需要接收的
        predict_table_name: 预测分数存储表名
        predict_feature_table_name: 预测特征表名
        model_hdfs_path: 模型存储hdfs地址
        output_file_name: 输出文件名称，需要输出的
    """
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    print("spark init-----------------")
    final_cols_list = [
        "cust_code",
        "label",
        "dt",
        "age",
        "sex",
        "city_name",
        "province_name",
        "educational_degree",
        "is_login",
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
        "instance_id",
    ]
    final_cols_str = str(final_cols_list).replace("[", "").replace("]", "").replace("'", "")
    df_predict = spark.sql(
        f"""
        select {final_cols_str} 
        from {predict_feature_table_name} 
        where instance_id = {instance_id}
        """
    ).toPandas()
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
    print("预测数据", df_predict.head())

    # 模型加载
    local_file_path = "model.pickle.dat"
    print("local_file_path--------------", local_file_path)
    read_hdfs_path(local_file_path, model_hdfs_path, hdfs_client)  # hdfs上的模型复制到本地
    print("model_hdfs_path--------------", model_hdfs_path)
    model = joblib.load(local_file_path)

    # 预测打分
    customer_list = df_predict["cust_code"].values
    x_predict = df_predict[model.feature_name_]
    pred_proba = model.predict_proba(x_predict)
    print("x_predict-----*****\n", x_predict)

    y_pred_score = pred_proba[:, 1]
    result = pd.DataFrame({"cust_code": customer_list, "score": y_pred_score})
    result.sort_values(by="score", inplace=True, ascending=False)

    # 结果存储
    result_local_path = "result.csv"
    result.to_csv(result_local_path, index=False, header=False)
    output_file_path = cos_helper.upload_file("result.csv", output_file_name)
    print("output_file_path-----*****\n", output_file_path)

    # 统计和可解释性部分
    result["instance_id"] = instance_id
    result = result.rename(columns={"cust_code": "user_id"})  # 重命名
    result = result[["instance_id", "user_id", "score"]]  # 调整顺序
    print("result-----*****\n", result)
    result_spark_df = (
        spark.createDataFrame(result)
        .select(F.col("instance_id").cast(LongType()),
                F.col("user_id").cast(StringType()),
                F.col("score").cast(FloatType()))
        .distinct()
    )  # 格式化数据类型
    print("distinct ------------------")
    # 存储到starrocks,列名[['instance_id','user_id','score']]
    result_spark_df.show()
    print("result_spark_df.schema----", result_spark_df.schema)
    write_score(result_spark_df, predict_table_name)
    print("预测分数存储完成-----*****")

    from digitforce.aip.common.utils.explain_helper import get_explain_result
    # shap和spark存在兼容性冲突，须放在spark_client后使用
    feature_cname_dict = {
        "cust_code": "客户编号",
        "label": "标签",
        "age": "年龄",
        "sex": "性别",
        "city_name": "城市",
        "province_name": "省份",
        "educational_degree": "学历",
        "is_login": "是否登录",
        "transfer_out_amt": "转出金额",
        "transfer_in_amt": "转入金额",
        "transfer_out_cnt": "转出次数",
        "transfer_in_cnt": "转入次数",
        "total_tran_cnt": "总交易次数",
        "total_tran_amt": "总交易金额",
        "gp_tran_cnt": "股票交易次数",
        "gp_tran_amt": "股票交易金额",
        "jj_tran_cnt": "基金交易次数",
        "jj_tran_amt": "基金交易金额",
        "zq_tran_cnt": "债券交易次数",
        "zq_tran_amt": "债券交易金额",
        "total_ast": "总资产",
        "net_ast": "净资产",
        "total_liab": "总负债",
        "unmoney_fnd_val": "货币基金净值",
        "stock_ast": "股票资产",
        "cash_bal": "现金余额",
        "total_prd_ast": "产品资产",
    }
    # 计算ale值和shap值并存储
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "-----------ale——shap计算开始---------")
    ale_json, shap_df = get_explain_result(
        df_predict.drop(columns=["label", "dt"]), model, categorical_features, feature_cname_dict
    )
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "-----------ale——shap计算结束---------")
    # 存储ale
    ale_local_path = "ale.json"
    ale_hdfs_path = f"/user/ai/aip/predict/{instance_id}/ale.json"
    with open(ale_local_path, "w") as f:
        json.dump(ale_json, f, indent=4)
    write_hdfs_path(ale_local_path, ale_hdfs_path, hdfs_client)
    print("ale_ 计算存储完成-----*****", ale_hdfs_path)

    # 存储shap
    shap_df["instance_id"] = instance_id
    shap_df = shap_df.rename(columns={"cust_code": "user_id"})  # 重命名
    shap_df = shap_df[["instance_id", "user_id", "shapley"]]  # 调整顺序
    print("shap_df-----***** \n", shap_df)
    shap_spark_df = (
        spark.createDataFrame(shap_df)
        .select(F.col("instance_id").cast(LongType()),
                F.col("user_id").cast(StringType()),
                F.col("shapley").cast(StringType()))
    )  # 格式化数据类型
    shap_spark_df.show()
    print("shap_spark_df.schema---- \n", shap_spark_df.schema)
    write_score(shap_spark_df, shapley_table_name)
    print("compute shapley value and store to starrocks success")
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "-----------ale——shap存储结束---------")


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
    data_process = data_init.drop(labels=drop_labels, axis=1, errors="ignore")  # 丢掉不需要的特征
    data_process[categorical_feature] = (
        data_process[categorical_feature]
        .replace([np.nan, pd.NA], None)  # 替换缺失值
        .astype("str").astype("category")  # 转换为类别特征
    )
    values = (
        data_process[categorical_feature]
        .mode(axis=0, dropna=True)
        .to_dict(orient='records')[0]
    )  # 获取众数
    print("values------------------", values)
    data_process[categorical_feature] = data_process[categorical_feature].fillna(values)  # 填充缺失值

    data_process[float_feature] = (
        data_process[float_feature]
        .fillna(0)  # 填充缺失值
        .apply(lambda col: pd.to_numeric(col, errors="coerce"), axis=0)
    )
    return data_process
