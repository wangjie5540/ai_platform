# -*- coding:utf-8 -*-

import os
import random
import logging

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


class MyDataset(Dataset):
    def __init__(self, data):
        self.data = data

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)


def nn_seq_int(B, input_file):
    logging.info('data processing...')
    data = []
    with open(input_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            x = []
            for term in parts[0].split(","):
                x.append(int(term))
            y = int(parts[1])
            x = torch.IntTensor(x)
            data.append((x, y))
    mydata = MyDataset(data)
    data_loader = DataLoader(dataset=mydata, batch_size=B, shuffle=False, drop_last=True)
    return data_loader


def nn_seq_float(B, input_file):
    logging.info('data processing...')
    data = []
    with open(input_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue
            x = []
            for term in parts[0].split(","):
                x.append(float(term))
            y = float(parts[1])
            x = torch.FloatTensor(x)
            y = torch.FloatTensor([y])
            data.append((x, y))
    mydata = MyDataset(data)
    data_loader = DataLoader(dataset=mydata, batch_size=B, shuffle=False, drop_last=True)
    return data_loader


def nn_seq(B, input_file, task_type):
    if task_type == "classify":
        return nn_seq_int(B, input_file)
    else:
        return nn_seq_float(B, input_file)


def get_mape(x, y):
    """
    :param x: true value
    :param y: pred value
    :return: mape
    """
    return np.mean(np.abs((x - y) / x))
