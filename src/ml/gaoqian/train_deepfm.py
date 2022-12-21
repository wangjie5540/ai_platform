import os
from typing import Dict
import pickle
import datetime
import logging
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data as Data
import time, json, datetime
from tqdm import tqdm
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from deepfm import DeepFM
import warnings
import digitforce.aip.common.utils.spark_helper as spark_helper
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)




# 打印模型参数
def get_parameter_number(model):
    total_num = sum(p.numel() for p in model.parameters())
    trainable_num = sum(p.numel() for p in model.parameters() if p.requires_grad)
    return {'Total': total_num, 'Trainable': trainable_num}

def filter_feature(data_columns):
    sparse_features = ['gender',
                       'EDU', 'RSK_ENDR_CPY', 'NATN',
                       'OCCU', 'IS_VAIID_INVST']
    dense_features = ['u_event1_counts_30d', 'u_event1_amount_sum_30d', 'u_event1_amount_avg_30d',
                      'u_event1_amount_min_30d',
                      'u_event1_amount_max_30d', 'u_event1_days_30d',
                      'u_event1_avg_days_30d', 'u_last_event1_days_30d']
    label = ['label', 'label1', 'label2']
    sparse_features = list(np.intersect1d(data_columns, sparse_features))
    dense_features = list(np.intersect1d(data_columns, sparse_features))
    label = list(np.intersect1d(data_columns, label))
    return sparse_features, dense_features, label

def process_dataset(train_data, valid_data, hdfs_path):
    data_columns = train_data.columns
    dense_features, sparse_features, label = filter_feature(data_columns)
    if len(label) == 1:
        tag_name = 'label'
    else:
        tag_name = ['label1', 'label2']

    with open(hdfs_path + "sparse_features_dict.pkl", "rb") as file:
        sparse_features_dict = pickle.load(file)

    cate_fea_nuniqs = [len(sparse_features_dict[feat]) for feat in sparse_features]
    nume_fea_size = len(dense_features)

    print(train_data.shape, valid_data.shape)

    train_dataset = Data.TensorDataset(torch.LongTensor(train_data[sparse_features].values),
                                       torch.FloatTensor(train_data[dense_features].values),
                                       torch.FloatTensor(train_data[tag_name].values),
                                       )

    train_loader = Data.DataLoader(dataset=train_dataset, batch_size=2048, shuffle=True)

    valid_dataset = Data.TensorDataset(torch.LongTensor(valid_data[sparse_features].values),
                                       torch.FloatTensor(valid_data[dense_features].values),
                                       torch.FloatTensor(valid_data[tag_name].values),
                                        )
    valid_loader = Data.DataLoader(dataset=valid_dataset, batch_size=4096, shuffle=False)



    return train_loader, valid_loader, cate_fea_nuniqs, nume_fea_size


def train_and_eval(model, train_loader, valid_loader, epochs, device, optimizer, loss_fcn, scheduler, model_path):
    best_auc = 0.0
    for _ in range(epochs):
        """训练部分"""
        model.train()
        print("Current lr : {}".format(optimizer.state_dict()['param_groups'][0]['lr']))
        print('Epoch: {}'.format(_ + 1))
        train_loss_sum = 0.0
        start_time = time.time()
        for idx, x in enumerate(train_loader):
            cate_fea, nume_fea, label= x[0], x[1], x[2]
            cate_fea, nume_fea, label= cate_fea.to(device), nume_fea.to(device), label.float().to(
                device)
            pred1 = model(cate_fea, nume_fea).view(-1)
            loss = loss_fcn(pred1, label)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            train_loss_sum += loss.cpu().item()
            if (idx + 1) % 500 == 0 or (idx + 1) == len(train_loader):
                print("Epoch {:04d} | Step {:04d} / {} | Loss {:.4f} | Time {:.4f}".format(
                    _ + 1, idx + 1, len(train_loader), train_loss_sum / (idx + 1), time.time() - start_time))
        scheduler.step()
        """推断部分"""
        model.eval()
        with torch.no_grad():
            valid_labels1, valid_preds1 = [], []
            for idx, x in tqdm(enumerate(valid_loader)):
                cate_fea, nume_fea, label1= x[0], x[1], x[2]
                cate_fea, nume_fea = cate_fea.to(device), nume_fea.to(device)
                pred1 = model(cate_fea, nume_fea).reshape(-1).data.cpu().numpy().tolist()
                valid_preds1.extend(pred1)
                valid_labels1.extend(label1.cpu().numpy().tolist())

        cur_auc = roc_auc_score(valid_labels1, valid_preds1)
        if cur_auc > best_auc:
            best_auc = cur_auc
            torch.save(model.state_dict(), model_path)
        print('Current AUC: %.6f, Best AUC: %.6f \n' % (cur_auc, best_auc))
    return best_auc


def start_train(train_data_table_name, train_data_columns, test_data_table_name, hdfs_path,
          lr=0.005, weight_decay=0.001):
    spark_client = spark_helper.SparkClient()
    train_dataset = spark_client.get_session().sql(f"""select {",".join(train_data_columns)} from {train_data_table_name}""").toPandas()
    valid_dataset = spark_client.get_session().sql(f"""select {",".join(train_data_columns)} from {test_data_table_name}""").toPandas()
    hdfs_path = "/tmp/pycharm_project_19/src/preprocessing/sample_comb_gaoqian/"
    train_loader, valid_loader, cate_fea_nuniqs, nume_fea_size = process_dataset(train_dataset, valid_dataset, hdfs_path)

    device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')

    model = DeepFM(cate_fea_nuniqs, nume_fea_size=nume_fea_size)
    model.to(device)
    loss_fcn = nn.BCELoss()  # Loss函数
    loss_fcn = loss_fcn.to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.8)
    epoches = 30

    # todo: upload to hdfs
    hdfs_path = "/tmp/pycharm_project_19/src/ml/gaoqian"
    model_path = os.path.join(hdfs_path, '/deepfm.pth')

    auc = train_and_eval(model, train_loader, valid_loader, epoches, device, optimizer, loss_fcn, scheduler, model_path)




