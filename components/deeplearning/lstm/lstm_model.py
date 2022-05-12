# -*- coding:utf-8 -*-
"""
@Time：2022/04/15 16:06
@Author：KI
@File：util.py
@Motto：Hungry And Humble
"""
from itertools import chain

import torch
from scipy.interpolate import make_interp_spline
from torch import nn
import numpy as np
import matplotlib.pyplot as plt
from models import LSTM, BiLSTM
from data_process import nn_seq, device, get_mape, setup_seed
from tqdm import tqdm

setup_seed(20)


def train(args):
    Dtr = nn_seq(args.batch_size, args.input_file_train)

    input_size, hidden_size, num_layers = args.input_size, args.hidden_size, args.num_layers
    output_size = args.output_size
    if args.bidirectional:
        model = BiLSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)
    else:
        model = LSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)

    loss_function = nn.MSELoss().to(args.device)
    if args.optimizer == 'adam':
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr,
                                     weight_decay=args.weight_decay)
    else:
        optimizer = torch.optim.SGD(model.parameters(), lr=args.lr,
                                    momentum=0.9, weight_decay=args.weight_decay)
    # training
    loss = 0
    for i in tqdm(range(args.epochs)):
        cnt = 0
        for (seq, label) in Dtr:
            cnt += 1
            seq = seq.to(args.device)
            label = label.to(args.device)
            y_pred = model(seq)
            loss = loss_function(y_pred, label)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            # if cnt % 100 == 0:
            #     print('epoch', i, ':', cnt - 100, '~', cnt, loss.item())
        print('epoch', i, ':', loss.item())

    state = {'model': model.state_dict(), 'optimizer': optimizer.state_dict()}
    torch.save(state, args.output_model)


def predict(args):
    Dte = nn_seq(args.batch_size, args.input_file_predict)

    pred = []
    y = []
    print('loading model...')
    input_size, hidden_size, num_layers = args.input_size, args.hidden_size, args.num_layers
    output_size = args.output_size
    if args.bidirectional:
        model = BiLSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)
    else:
        model = LSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)
    model.load_state_dict(torch.load(args.output_model)['model'])
    model.eval()
    print('predicting...')
    for (seq, target) in tqdm(Dte):
        target = list(chain.from_iterable(target.data.tolist()))
        y.extend(target)
        seq = seq.to(args.device)
        with torch.no_grad():
            y_pred = model(seq)
            y_pred = list(chain.from_iterable(y_pred.data.tolist()))
            pred.extend(y_pred)

    with open(args.output_predict_file, "w") as f:
        for index in range(len(pred)):
            f.write("%s\t%s\n" % (y[index], pred[index]))

    # y, pred = np.array(y), np.array(pred)
    # y = (m - n) * y + n
    # pred = (m - n) * pred + n
    # print('mape:', get_mape(y, pred))
    # # plot
    # x = [i for i in range(1, 151)]
    # x_smooth = np.linspace(np.min(x), np.max(x), 900)
    # y_smooth = make_interp_spline(x, y[150:300])(x_smooth)
    # plt.plot(x_smooth, y_smooth, c='green', marker='*', ms=1, alpha=0.75, label='true')
    #
    # y_smooth = make_interp_spline(x, pred[150:300])(x_smooth)
    # plt.plot(x_smooth, y_smooth, c='red', marker='o', ms=1, alpha=0.75, label='pred')
    # plt.grid(axis='y')
    # plt.legend()
    # plt.show()
