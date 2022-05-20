
from itertools import chain
import logging
import torch
from torch import nn

from models import LSTM, BiLSTM
from data_process import nn_seq, setup_seed
from tqdm import tqdm

setup_seed(20)


def train(args):
    Dtr = nn_seq(args.batch_size, args.input_file_train)
    if args.valid:
        Dva = nn_seq(args.batch_size, args.input_file_valid)

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
    for i in tqdm(range(args.epochs)):
        model.train()
        total_loss = 0
        count = 0
        for (seq, label) in Dtr:
            seq = seq.to(args.device)
            label = label.to(args.device)
            y_pred = model(seq)
            loss = loss_function(y_pred, label)
            total_loss += loss.data
            count += 1
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        logging.info(f"train-epoch:{i}, loss:{total_loss.item() / count}")

        if args.valid:
            total_loss = 0
            count = 0
            model.eval()
            for (seq, label) in Dva:
                seq = seq.to(args.device)
                label = label.to(args.device)
                y_pred = model(seq)
                loss = loss_function(y_pred, label)
                total_loss += loss.data
                count += 1
            logging.info(f"valid-epoch:{i}, loss:{total_loss.item() / count}")

    state = {'model': model.state_dict(), 'optimizer': optimizer.state_dict()}
    torch.save(state, args.output_model)


def predict(args):
    args.batch_size = 1
    Dte = nn_seq(args.batch_size, args.input_file_predict)

    pred = []
    y = []
    logging.info('loading model...')
    input_size, hidden_size, num_layers = args.input_size, args.hidden_size, args.num_layers
    output_size = args.output_size
    if args.bidirectional:
        model = BiLSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)
    else:
        model = LSTM(input_size, hidden_size, num_layers, output_size, batch_size=args.batch_size).to(args.device)
    model.load_state_dict(torch.load(args.output_model)['model'])
    model.eval()
    logging.info('predicting...')
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
    logging.info("predict finished.")

