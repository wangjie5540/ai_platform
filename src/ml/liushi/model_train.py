#!/usr/bin/env python3
# encoding: utf-8
import datetime

import random
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score, precision_score, recall_score, log_loss
import joblib
from digitforce.aip.common.utils.spark_helper import spark_client
import digitforce.aip.common.utils.hdfs_helper as hdfs_helper

DATE_FORMAT = "%Y%m%d"
today = datetime.datetime.today().strftime(DATE_FORMAT)
hdfs_client = hdfs_helper.HdfsClient()


def start_model_train(train_table_name, test_table_name,
                learning_rate=0.05, n_estimators=200, max_depth=5, scale_pos_weight=0.5,
                is_train=True):
    df_train = spark_client.get_session().sql(
        "select * from {} where dt = {}".format(train_table_name, today)).toPandas()
    df_test = spark_client.get_session().sql(
        "select * from {} where dt = {}".format(test_table_name, today)).toPandas()

    for col in df_train.columns:
        if df_train[col].dtypes == "object":
            df_train[col] = df_train[col].astype(float)
        if df_test[col].dtypes == "object":
            df_test[col] = df_test[col].astype(float)

    x_train = df_train.drop(columns=['label', 'dt'], axis=1)
    y_train = df_train['label']

    x_test = df_test.drop(columns=['label', 'dt'], axis=1)
    y_test = df_test['label']

    # TODO：mock数据临时修改label
    random.seed(1234)
    y_train = y_train.map(lambda x: random.randint(0,1))
    y_test = y_test.map(lambda x: random.randint(0,1))


    # 模型训练
    model = XGBClassifier(learning_rate=learning_rate, n_estimators=n_estimators,
                          max_depth=max_depth, scale_pos_weight=scale_pos_weight,
                          eval_metric="logloss")

    model.fit(x_train, y_train, verbose=True)

    # 测试集打分&效果评估
    y_pred = model.predict(x_test)
    y_pred_score = [x[1] for x in model.predict_proba(x_test)]

    def getRates(y_test, y_pred, y_pred_score):
        s_acc = accuracy_score(y_test, y_pred)
        s_auc = roc_auc_score(y_test, y_pred_score)
        #     s_auc = 0
        s_pre = precision_score(y_test, y_pred)
        s_rec = recall_score(y_test, y_pred)
        s_f1 = f1_score(y_test, y_pred)
        s_loss = log_loss(y_test, y_pred_score)
        return [s_acc, s_auc, s_pre, s_rec, s_f1, s_loss]

    all_score = getRates(y_test, y_pred, y_pred_score)
    print("test-logloss={:.4f}, test-auc={:.4f}".format(all_score[5], all_score[1]))

    if is_train:
        local_file_path = "{}_aip_zq_liushi.model".format(today)
        joblib.dump(model, local_file_path)
        hdfs_path1 = "/user/ai/aip/zq/liushi/model/{}.model".format(today)
        hdfs_path2 = "/user/ai/aip/zq/liushi/model/lasted.model"
        write_hdfs_path(local_file_path, hdfs_path1, hdfs_client)
        write_hdfs_path(local_file_path, hdfs_path2, hdfs_client)


# 写hdfs，覆盖写！
def write_hdfs_path(local_path, hdfs_path, hdfs_client):
    if hdfs_client.exists(hdfs_path):
        hdfs_client.delete(hdfs_path)
    hdfs_client.copy_from_local(local_path, hdfs_path)
