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

def process_dataset(data, tag_name):
    dense_features = [f for f in data.columns.tolist() if f[0] == "I"]
    sparse_features = [f for f in data.columns.tolist() if f[0] == "C"]

    data[sparse_features] = data[sparse_features].fillna('-1000', )
    data[dense_features] = data[dense_features].fillna(0, )

    ## 类别特征labelencoder
    for feat in tqdm(sparse_features):
        lbe = LabelEncoder()
        data[feat] = lbe.fit_transform(data[feat])

    ## 数值特征标准化
    dense_feature_info_dict = {}
    for feat in tqdm(dense_features):
        mean = data[feat].mean()
        std = data[feat].std()
        data[feat] = (data[feat] - mean) / (std + 1e-12)  # 防止除零
        dense_feature_info_dict[feat] = {'mean': mean, 'std': std}

    train_data, valid_data = train_test_split(data, test_size=0.2, random_state=2020)
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

    cate_fea_nuniqs = [data[f].nunique() for f in sparse_features]
    nume_fea_size = len(dense_features)
    return train_loader, valid_loader, cate_fea_nuniqs, nume_fea_size, dense_feature_info_dict


def train_and_eval(model, train_loader, valid_loader, epochs, device, optimizer, loss_fcn, scheduler, model_path):
    best_auc = 0.0
    for _ in range(epochs):
        """训练部分"""
        model.train()
        print("Current lr : {}".format(optimizer.state_dict()['param_groups'][0]['lr']))
        logging.info('Epoch: {}'.format(_ + 1))
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
                logging.info("Epoch {:04d} | Step {:04d} / {} | Loss {:.4f} | Time {:.4f}".format(
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
        logging.info('Current AUC: %.6f, Best AUC: %.6f \n' % (cur_auc, best_auc))
    return best_auc


def train(train_data_table_name, hdfs_path, train_data_columns):
    spark_client = spark_helper.SparkClient()
    dataset = spark_client.get_session().sql(f"select * from {train_data_table_name}").toPandas()
    train_loader, valid_loader, cate_fea_nuniqs, nume_fea_size, dense_feature_info = process_dataset(dataset, 'label')
    input_params['dense_feature_info'] = dense_feature_info
    device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')

    model = DeepFM(cate_fea_nuniqs, nume_fea_size=nume_fea_size)
    model.to(device)
    loss_fcn = nn.BCELoss()  # Loss函数
    loss_fcn = loss_fcn.to(device)
    optimizer = optim.Adam(model.parameters(), lr=0.005, weight_decay=0.001)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.8)
    epoches = 30

    # store the model
    # outputpath = os.path.join(path,"model")
    model_local_path=str(input_params['taskid'])+r".pth"
    # model_local_path = os.path.join(outputpath,fname)

    auc = train_and_eval(model, train_loader, valid_loader, epoches, device, optimizer, loss_fcn, scheduler, model_local_path)

    # end = time.time()
    # cur_time = datetime.datetime.now()
    # statinfo = os.stat(filepath)

    # upload model and parameters
    model_target_file_path = os.path.join(hdfs_model_path, str(input_params['taskid'])+'.pth')
    params_target_file_path = os.path.join(hdfs_model_path, str(input_params['taskid']) + '.txt')
    params = json.dumps(input_params, ensure_ascii=False).encode('utf-8')
    params_local_path = str(input_params['taskid']) + r'.txt'
    with open(params_local_path, 'w') as f:
        f.write(params)
    upload_flag1 = upload_hdfs(model_local_path, model_target_file_path)
    upload_flag2 = upload_hdfs(params_local_path, params_target_file_path)
    if upload_flag1 and upload_flag2:
        model_file_url = upload_flag1
        os.remove(model_local_path)
        os.remove(params_local_path)
    else:
        model_file_url = '-1'
    print(f'model_file_path: {model_file_url}')

