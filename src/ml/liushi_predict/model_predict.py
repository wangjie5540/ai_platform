#!/usr/bin/env python3
# encoding: utf-8
import findspark
findspark.init()
import datetime
import json
import pandas as pd
import joblib
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper
from digitforce.aip.common.utils import cos_helper
from digitforce.aip.common.utils.starrocks_helper import write_score
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, StringType, FloatType

DATE_FORMAT = "%Y%m%d"
today = datetime.datetime.today().strftime(DATE_FORMAT)
hdfs_client = hdfs_helper.HdfsClient()

def start_model_predict(predict_table_name, model_hdfs_path, output_file_name,
                        instance_id: int, predict_score_table_name: str, shapley_table_name: str,):

    spark_client = SparkClient.get()
    spark = spark_client.get_session()

    dt = spark_client.get_session().sql(
        f"show partitions {predict_table_name}").collect()[-1][0][3:]
    print(dt)
    df_predict = spark_client.get_session().sql(
        "select * from {} where dt = {}".format(predict_table_name, dt)).toPandas()

    for col in df_predict.columns:
        if df_predict[col].dtypes == "object":
            df_predict[col] = df_predict[col].astype(float)

    custom_list = df_predict['custom_id'].values
    x_predict = df_predict.drop(columns=['custom_id', 'label', 'dt'], axis=1)
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

    # 结果存储
    result_local_path = "result.csv"
    result_hdfs_path = "/user/ai/aip/zq/liushi/result/{}_liushi_result.csv".format(today)
    result.to_csv(result_local_path, index=False,header=False)
    # write_hdfs_path(result_local_path, result_hdfs_path, hdfs_client)
    output_file_path = cos_helper.upload_file("result.csv", output_file_name)
    print("output_file_path-----*****/n", output_file_path)

    # 统计和可解释性部分
    result["instance_id"] = instance_id
    result = result.rename(columns={"custom_id": "user_id"})  # 重命名
    result = result[["instance_id", "user_id", "score"]]  # 调整顺序
    print("result-----*****/n", result)
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
    write_score(result_spark_df, predict_score_table_name)
    print("预测分数存储完成-----*****")


    from digitforce.aip.common.utils.explain_helper import get_explain_result
    # shap和spark存在兼容性冲突，须放在spark_client后使用
    feature_cname_dict = {
        'custom_code': '客户编号',
         'label': '标签',
         'last_jy_days': '上次交易距今天数',
         'last_jy_money': '上次交易金额',
         '3_jy_cnt': '近3天交易次数',
         '3_jy_money': '近3天交易金额',
         '3_jy_gp_cnt': '近3天股票交易次数',
         '3_jy_gp_money': '近3天股票交易金额',
         '3_jy_jj_cnt': '近3天基金交易次数',
         '3_jy_jj_money': '近3天基金交易金额',
         '7_jy_cnt': '近7天交易次数',
         '7_jy_money': '近7天交易金额',
         '7_jy_gp_cnt': '近7天股票交易次数',
         '7_jy_gp_money': '近7天股票交易金额',
         '7_jy_jj_cnt': '近7天基金交易次数',
         '7_jy_jj_money': '近7天基金交易金额',
         '15_jy_cnt': '近15天交易次数',
         '15_jy_money': '近15天交易金额',
         '15_jy_gp_cnt': '近15天股票交易次数',
         '15_jy_gp_money': '近15天股票交易金额',
         '15_jy_jj_cnt': '近15天基金交易次数',
         '15_jy_jj_money': '近15天基金交易金额',
         '30_jy_cnt': '近30天交易次数',
         '30_jy_money': '近30天交易金额',
         '30_jy_gp_cnt': '近30天股票交易次数',
         '30_jy_gp_money': '近30天股票交易金额',
         '30_jy_jj_cnt': '近30天基金交易次数',
         '30_jy_jj_money': '近30天基金交易金额',
         'last_zj_days': '上次资金变动距今天数',
         'last_zj_money': '上次资金变动金额',
         '3_zj_zc_money': '近3天资金转出金额',
         '3_zj_zr_money': '近3天资金转入金额',
         '3_zj_zc_cnt': '近3天资金转出次数',
         '3_zj_zr_cnt': '近3天资金转入次数',
         '7_zj_zc_money': '近7天资金转出金额',
         '7_zj_zr_money': '近7天资金转入金额',
         '7_zj_zc_cnt': '近7天资金转出次数',
         '7_zj_zr_cnt': '近7天资金转入次数',
         '15_zj_zc_money': '近15天资金转出金额',
         '15_zj_zr_money': '近15天资金转入金额',
         '15_zj_zc_cnt': '近15天资金转出次数',
         '15_zj_zr_cnt': '近15天资金转入次数',
         '30_zj_zc_money': '近30天资金转出金额',
         '30_zj_zr_money': '近30天资金转入金额',
         '30_zj_zc_cnt': '近30天资金转出次数',
         '30_zj_zr_cnt': '近30天资金转入次数',
         '3_login_cnt': '近3天app登录天数',
         '7_login_cnt': '近7天app登录天数',
         '15_login_cnt': '近15天app登录天数',
         '30_login_cnt': '近30天app登录天数',
         'age': '年龄',
         'sex': '性别',
         'city': '城市',
         'province': '省份',
         'edu': '学历',
         'now_zc': '总资产',
         'now_fuzhai': '总负债',
         'now_zc_jj': '基金资产',
         'now_zc_gp': '股票资产',
         'now_zj': '资金余额',
         'now_zc_cp': '产品资产',
    }
    # 计算ale值和shap值并存储
    categorical_features = []
    df_predict = df_predict.rename(columns={"custom_id": "custom_code"})
    ale_json, shap_df = get_explain_result(
        df_predict.drop(columns=["label", "dt"]), model, categorical_features, feature_cname_dict
    )
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

# 读hdfs
def read_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.copy_to_local(hdfs_path, local_path)

# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)
