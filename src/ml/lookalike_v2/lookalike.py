# encoding: utf-8
import os
import time
import json

import torch
import pandas as pd
import logging
import argparse

from src.ml.lookalike_v2.model.dssm import DSSM
from src.ml.lookalike_v2.preprocessing.utils import size_format
from src.ml.lookalike_v2.model_train import filter_features, get_train_test_input
from digitforce.aip.common.utils import spark_helper

def start_model_train(sample_table_name, sample_feature_columns, user_table_name, user_feature_columns):
    spark_client = spark_helper.SparkClient()
    data = spark_client.get_session().sql(f"select * from {sample_table_name}").toPandas()

    print("筛选构建模型所需特征")
    sparse_features, dense_features, sequence_features, \
    target, user_sparse_features, user_dense_features, \
    item_sparse_features, item_dense_features, \
    user_sequence_features, item_sequence_features = filter_features(
        sample_feature_columns)

    print("构建模型所需数据、特征及相应输入")
    train, test, data, user_feature_columns, item_feature_columns, train_model_input, test_model_input = \
        get_train_test_input(sample, user_list, sparse_features, dense_features, sequence_features,
                             user_sparse_features, user_dense_features,
                             item_sparse_features, item_dense_features,
                             user_sequence_features, item_sequence_features,
                             encoder_path, scaler_path)

    print("定义模型，训练、预测、评估")
    device = 'cpu'
    use_cuda = True
    if use_cuda and torch.cuda.is_available():
        print('cuda ready...')
        device = 'cuda:0'
    model = DSSM(user_feature_columns, item_feature_columns, l2_reg_dnn=1e-5, l2_reg_embedding=1e-5,
                 dnn_dropout=0.2, task='binary', device=device)

    model.compile("adam", "binary_crossentropy", metrics=['auc', 'accuracy', 'precision', 'recall', 'f1_score'])

    print("开始训练：")
    start = time.time()
    os.makedirs('model_zoo', exist_ok=True)
    model.fit(train_model_input, train[target].values, epochs=3, verbose=2, validation_split=0.2)
    end = time.time()
    print("训练结束，共用时：{}秒".format(end - start))

    # 评估
    eval_tr = model.evaluate(train_model_input, train[target].values)
    torch.save({'model_state_dict': model.state_dict(),
                'optimizer_state_dict': model.optim.state_dict()}, model_path)
    model_size = size_format(os.path.getsize(model_path))
    auc = eval_tr.get('auc')
    auc = "%.2f%%" % (auc * 100)
    now = int(time.time())
    # 转换为其他日期格式,如:"%Y-%m-%d %H:%M:%S"
    timeArray = time.localtime(now)
    generateTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    res = {'taskId': taskid, 'auc': auc, "fileSize": model_size, "generateTime": generateTime}

    # 4、计算用户向量
    # 训练过程保存了最佳模型，需要先加载模型
    print("计算用户向量")
    state_dict = torch.load("./model_zoo/model.pth")

    model = DSSM(user_feature_columns, item_feature_columns, task='binary', device=device)
    model.load_state_dict(state_dict['model_state_dict'], strict=False)
    dict_trained = model.state_dict()  # trained model
    trained_lst = list(dict_trained.keys())

    # 获取单塔 user tower
    model_user = DSSM(user_feature_columns, [], task='binary', device=device)
    dict_user = model_user.state_dict()
    for key in dict_user:
        dict_user[key] = dict_trained[key]
    model_user.load_state_dict(dict_user)  # load trained model parameters of user tower
    user_feature_name = user_sparse_features + user_dense_features
    user_model_input = {name: test[name] for name in user_feature_name}
    for user_seq in user_sequence_features:
        user_model_input[user_seq] = test_model_input[user_seq]
    user_embedding = model_user.predict(user_model_input, batch_size=256)

    #     test = pd.read_csv(test_path, sep='\t')
    user_embedding_dic = {}
    for i in range(len(user_embedding)):
        k = user_id_list[i]
        user_embedding_dic[str(k)] = str(user_embedding[i].tolist())
    print("得到用户向量")
    user_embedding_df = pd.DataFrame.from_dict(user_embedding_dic, orient='index', columns=['embedding'])
    user_embedding_df = user_embedding_df.reset_index().rename(columns={'index': 'user_id'})
    print("上传用户向量")
    spark = SparkEnv("lookalike_train").spark
    upload_user_embedding(spark, taskid, user_embedding_df)
    return res, state_dict


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--solution_id', type=str, default='', help='solution id')
    parser.add_argument('--instance_id', type=str, default='', help='instance id')
    args = parser.parse_args()
    data_dict = {
        "userData": {
            "city": "",
            "sex": "sex_id",
            "consume_level": "",
            "yuliu_id": "",
            "tableName": "labelx.push_rpt_member_labels",
            "dt": "dt",
            "recent_view_day": "",
            "membership_level": "",
            "province": "",
            "user_id": "vip_id",
            "online_signup_time": "",
            "life_stage": "",
            "age": "age"},
        "trafficData": {
            "cart_remove": "",
            "cart_add": "",
            "click": "CLICK",
            "tableName": "labelx.push_event_vip_traffic",
            "dt": "",
            "search": "",
            "exposure": "EXPOSURE",
            "card_add": "BROWSE",
            "user_id": "vip_id",
            "event_code": "event_code",
            "sku": "sku",
            "collect": "BROWSE",
            "event_time": "event_time",
            "browse": ""},
        "orderData": {
            "user_id": "vip_id",
            "order_time": "order_time",
            "sku": "sku",
            "sale_quantity": "sale_quantity",
            "order_id": "order_id",
            "sale_amount": "sale_amount",
            "tableName": "labelx.push_event_vip_order"},
        "goodsData": {
            "dt": "dt",
            "cate": "cate",
            "sku": "sku",
            "brand": "brand",
            "tags": "",
            "tableName": "labelx.push_goods"},
        "eventCode": {
            "event_code": {
                "search": "",
                "cart_remove": "",
                "exposure": "EXPOSURE",
                "cart_add": "",
                "click": "CLICK",
                "collect": "BROWSE",
                "browse": ""}},
        'taskId': args.solution_id}
    print("启动训练")
    res, model = start_model_train(data_dict)
    # res: res = {'solution_id': solution_id, 'auc': auc, "fileSize": model_size, "generateTime": generateTime}
    # model: 模型文件.pth
    print("返回结果")
