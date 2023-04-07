# encoding: utf-8
import findspark

findspark.init()
import datetime
import random
import pandas as pd

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


def start_model_train(
        train_table_name,
        test_table_name,
        learning_rate=0.05,
        n_estimators=200,
        max_depth=5,
        scale_pos_weight=0.5,
        is_automl=False,
        model_and_metrics_data_hdfs_path=None,
):
    spark_client = SparkClient.get()
    spark = spark_client.get_session()
    df_train = spark.sql(
        f"""
            select * from {train_table_name} limit 1000000
            """
    ).toPandas()

    print("训练数据规模", len(df_train))
    df_test = spark.sql(
        f"""
            select * from {test_table_name}
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
        eval_metric="logloss",
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
            s_pre = precision_score(test, pred)
            s_rec = recall_score(test, pred)
            s_f1 = f1_score(test, pred)
            s_loss = log_loss(test, pred_score)
            fpr, tpr, _ = roc_curve(test, pred_score)
            roc_plot = {"x": list(fpr), "y": list(tpr)}
            return [s_acc, s_auc, s_pre, s_rec, s_f1, s_loss, roc_plot]

        all_score = getRates(y_test, y_pred, y_pred_score)
        print("test-logloss={:.4f}, test-auc={:.4f}".format(all_score[5], all_score[1]))
    else:
        all_score = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, {"x": [], "y": []}]

    if not is_automl:  # automl 默认值这里给False
        local_file_path = "{}_aip_zq_dixiaohu.model".format(today)
        joblib.dump(model, local_file_path)
        hdfs_path1 = "/user/ai/aip/zq/dixiaohu/model/{}.model".format(today)
        hdfs_path2 = "/user/ai/aip/zq/dixiaohu/model/latest.model"
        write_hdfs_path(local_file_path, hdfs_path1)
        write_hdfs_path(local_file_path, hdfs_path2)
        assert model_and_metrics_data_hdfs_path is not None
        model_hdfs_path = model_and_metrics_data_hdfs_path + "/model.pk"
        write_hdfs_path(local_file_path, model_hdfs_path)

        # report model and metrics to aip
        metrics_info = {
            "accuracy": all_score[0],
            "auc": all_score[1],
            "precision": all_score[2],
            "recall": all_score[3],
            "f1_score": all_score[4],
            "loss": all_score[5],
            # "roc_plot": all_score[6],
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
def featuretype_process(data_init: pd.DataFrame, drop_labels: list, categorical_feature: list,
                        float_feature: list):
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
