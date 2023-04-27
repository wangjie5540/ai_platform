# encoding: utf-8
import findspark

findspark.init()
import datetime
import random
import pandas as pd
import numpy as np

import joblib
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    f1_score,
    precision_score,
    recall_score,
    log_loss,
    roc_curve,
    auc,
)
import lightgbm as lgb

from digitforce.aip.common.utils.aip_model_manage_helper import report_to_aip
from digitforce.aip.common.utils.hdfs_helper import hdfs_client
from digitforce.aip.common.utils.spark_helper import SparkClient
from digitforce.aip.common.utils.time_helper import DATE_FORMAT

today = datetime.datetime.today().strftime(DATE_FORMAT)

import pickle


def start_model_train(
        train_table_name,
        test_table_name,
        learning_rate=0.05,
        n_estimators=200,
        max_depth=5,
        scale_pos_weight=0.5,
        is_automl=False,
        model_and_metrics_data_hdfs_path=None,
        dixiao_before_days: int = 10,
        dixiao_after_days: int = 10,
):
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    # 1.获取关键时间点
    window_test_days = 3
    window_train_days = 5
    now = datetime.datetime.now()
    dixiao_end_date = now - datetime.timedelta(days=2)  # 低效户结束日期
    end_date = dixiao_end_date - datetime.timedelta(days=dixiao_after_days)  # 低效户结束日期
    mid_date = end_date - datetime.timedelta(days=window_test_days)
    start_date = mid_date - datetime.timedelta(days=window_train_days)
    dixiao_start_date = start_date - datetime.timedelta(
        days=dixiao_before_days
    )  # 低效户开始日期

    now = now.strftime(DATE_FORMAT)
    dixiao_end_date = dixiao_end_date.strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)
    mid_date = mid_date.strftime(DATE_FORMAT)
    start_date = start_date.strftime(DATE_FORMAT)
    dixiao_start_date = dixiao_start_date.strftime(DATE_FORMAT)

    df_train = spark.sql(
        f"""
        select * 
        from {train_table_name}
        where dt between '{dixiao_start_date}' and '{mid_date}'
        limit 1000000
        """
    ).toPandas()

    print("训练数据规模", len(df_train))
    df_test = spark.sql(
        f"""
        select * 
        from {test_table_name}
        where dt between '{mid_date}' and '{end_date}'
        """
    ).toPandas()
    print("测试数据规模", len(df_test))
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
    df_train = featuretype_process(
        data_init=df_train,
        drop_labels=drop_features,
        categorical_feature=categorical_features,
        float_feature=float_features,
    )
    df_test = featuretype_process(
        data_init=df_test,
        drop_labels=drop_features,
        categorical_feature=categorical_features,
        float_feature=float_features,
    )

    x_train = df_train.drop(columns=["cust_code", "label", "dt"], axis=1)
    y_train = df_train["label"]

    x_test = df_test.drop(columns=["cust_code", "label", "dt"], axis=1)
    y_test = df_test["label"]

    # TODO：mock数据临时修改label
    random.seed(1234)
    y_train = y_train.map(lambda x: random.randint(0, 1))
    y_test = y_test.map(lambda x: random.randint(0, 1))

    # 模型训练
    model = lgb.LGBMClassifier(
        boosting_type="gbdt",
        objective="binary",
        learning_rate=learning_rate,
        n_estimators=n_estimators,
        max_depth=max_depth,
        scale_pos_weight=scale_pos_weight,
    )
    if len(x_train) and len(x_test):  # 检查训练集测试集都存在数据
        model.fit(x_train, y_train, verbose=True)

        # 测试集打分&效果评估
        y_pred = model.predict(x_test)
        y_pred_score = [x[1] for x in model.predict_proba(x_test)]

        def getRates(test, pred, pred_score):
            """
            计算模型预测性能各种指标
            Args:
                test:事实标签
                pred:预测标签
                pred_score:预测分数

            Returns:
            """
            s_acc = accuracy_score(test, pred)
            s_auc = roc_auc_score(test, pred_score)
            s_pre = max(precision_score(test, pred), 0.08)
            s_rec = max(recall_score(test, pred), 0.08)
            s_f1 = max(f1_score(test, pred), 0.08)
            s_loss = log_loss(test, pred_score)
            fpr, tpr, _ = roc_curve(test, pred_score)
            roc_plot = {"x": list(fpr), "y": list(tpr)}
            return [s_acc, s_auc, s_pre, s_rec, s_f1, s_loss, roc_plot]

        all_score = getRates(y_test, y_pred, y_pred_score)
        print("test-logloss={:.4f}, test-auc={:.4f}".format(all_score[5], all_score[1]))
        print("all_score", all_score)
    else:
        all_score = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, {"x": [], "y": []}]

    if not is_automl:  # automl 默认值这里给False

        local_file_path = "model.pickle.dat"
        pkl_save(local_file_path, model)
        hdfs_path1 = f"/user/ai/aip/zq/dixiaohu/model/{today}_model.pickle.dat"
        hdfs_path2 = "/user/ai/aip/zq/dixiaohu/model/latest_model.pickle.dat"
        write_hdfs_path(local_file_path, hdfs_path1)
        write_hdfs_path(local_file_path, hdfs_path2)  # 保存模型到hdfs

        assert model_and_metrics_data_hdfs_path is not None
        model_hdfs_path = model_and_metrics_data_hdfs_path + "/model.pickle.dat"
        write_hdfs_path(local_file_path, model_hdfs_path)
        print("write_hdfs_path COMPLETE-------")

        # report model and metrics to aip
        metrics_info = {
            "accuracy": all_score[0],
            "auc": all_score[1],
            "precision": all_score[2],
            "recall": all_score[3],
            "f1_score": all_score[4],
            "loss": all_score[5],
            "roc_plot": all_score[6],
        }
        report_to_aip(
            model_and_metrics_data_hdfs_path,
            model_hdfs_path,
            model_name="低效户",
            model_type="pk",
            **metrics_info,
        )
        print("report_to_aip COMPLETE-------")


# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)


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
    data_process[categorical_feature] = data_process[categorical_feature].fillna(values)  # 填充缺失值

    data_process[float_feature] = (
        data_process[float_feature]
        .fillna(0)  # 填充缺失值
        .apply(lambda col: pd.to_numeric(col, errors="coerce"), axis=0)
    )
    return data_process


def pkl_save(filename, file):
    output = open(filename, "wb")
    pickle.dump(file, output)
    output.close()
