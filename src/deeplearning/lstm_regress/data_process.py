# -*- coding:utf-8 -*-

import os
import random

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def setup_seed(seed):
    os.environ['PYTHONHASHSEED'] = str(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True


def load_data(path):
    """
    :return: normalized dataframe
    """
    # path = os.path.dirname(os.path.realpath(__file__)) + '/data/data.csv'
    df = pd.read_csv(path, encoding='gbk')
    columns = df.columns
    df.fillna(df.mean(), inplace=True)
    MAX = np.max(df[columns[1]])
    MIN = np.min(df[columns[1]])
    df[columns[1]] = (df[columns[1]] - MIN) / (MAX - MIN)

    return df, MAX, MIN


class MyDataset(Dataset):
    def __init__(self, data):
        self.data = data

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)

def nn_seq(B, input_file):
    print('data processing...')
    data_feature = []
    data_label = []
    # feature_len = 0
    data = []
    with open(input_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            x = []
            # y = []
            for term in parts[0].split(","):
                x.append(float(term))
            # y.append(float(parts[1]))
            y = float(parts[1])

            x = torch.FloatTensor(x)
            y = torch.FloatTensor([y])
            # data_feature.append(x)
            # data_label.append(y)
            # feature_len = len(x)
            data.append((x, y))

    # zheng_len = int(len(data) / B) * B
    # mydata = MyDataset(data[:zheng_len])
    mydata = MyDataset(data)
    data_loader = DataLoader(dataset=mydata, batch_size=B, shuffle=False, drop_last=True)
    # data_loader = DataLoader(data_feature, data_label, feature_len, batch_size=B, shuffle=False)
    return data_loader


def get_mape(x, y):
    """
    :param x: true value
    :param y: pred value
    :return: mape
    """
    return np.mean(np.abs((x - y) / x))
