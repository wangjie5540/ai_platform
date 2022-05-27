import logging
import torch
from torch import nn
from models import LSTMClassify, LSTMRegress
from data_process import nn_seq, setup_seed
from tqdm import tqdm
from itertools import chain

setup_seed(20)

def trainEpoch(epochnum, data, model, args, loss_function, optimizer, is_train, task_type):
    if is_train:
        model.train()
    else:
        model.eval()
    total_loss = 0
    count = 0
    corrects = 0
    for (seq, label) in data:
        seq = seq.to(args.device)
        label = label.to(args.device)
        y_pred = model(seq)
        loss = loss_function(y_pred, label)
        total_loss += loss.data
        count += 1
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        if task_type == "classify":
            correct = (torch.max(y_pred, 1)[1].view(label.size()).data == label.data).sum()
            corrects += correct
    if task_type == "classify":
        acc = corrects.item() / (count * len(label))

    if is_train:
        logging.info(f"train-epoch:{epochnum}, loss:{total_loss.item() / count}")
    else:
        if task_type == "classify":
            logging.info(f"valid-epoch: {epochnum} | loss: {total_loss.item() / count} | acc: {acc} | corrects: {corrects}")
        else:
            logging.info(f"valid-epoch:{epochnum}, loss:{total_loss.item() / count}")

def train(args):
    Dtr = nn_seq(args.batch_size, args.input_file_train, args.task_type)
    if args.valid:
        Dva = nn_seq(args.batch_size, args.input_file_valid, args.task_type)

    if args.task_type == "classify":
        model = LSTMClassify(batch_size=args.batch_size,
                                  output_size=args.output_size,
                                  hidden_size=args.hidden_size,
                                  vocab_size=args.vocab_size,
                                  embed_dim=args.embed_dim,
                                  bidirectional=args.bidirectional,
                                  dropout=args.dropout,
                                  attention_size=args.attention_size,
                                  sequence_length=args.sequence_length)
        loss_function = nn.CrossEntropyLoss()
    elif args.task_type == "regress":
        model = LSTMRegress(args.input_size, args.hidden_size,
                            args.num_layers, args.output_size,
                            batch_size=args.batch_size, bidirectional=args.bidirectional).to(args.device)
        loss_function = nn.MSELoss().to(args.device)
    else:
        logging.error("args.task_type required classify or regress, but input:"+args.task_type)
        return

    if args.optimizer == 'adam':
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr,
                                     weight_decay=args.weight_decay)
    else:
        optimizer = torch.optim.SGD(model.parameters(), lr=args.lr,
                                    momentum=0.9, weight_decay=args.weight_decay)

    # training
    for i in tqdm(range(args.epochs)):
        trainEpoch(i, Dtr, model, args, loss_function, optimizer, True, args.task_type)
        if args.valid:
            trainEpoch(i, Dva, model, args, loss_function, optimizer, False, args.task_type)

    state = {'model': model.state_dict(), 'optimizer': optimizer.state_dict()}
    torch.save(state, args.output_model)


def predict(args):
    args.batch_size = 1
    Dte = nn_seq(args.batch_size, args.input_file_predict, args.task_type)

    pred = []
    y = []
    logging.info('loading model...')
    if args.task_type == "classify":
        model = LSTMClassify(batch_size=args.batch_size,
                 output_size=args.output_size,
                 hidden_size=args.hidden_size,
                 vocab_size=args.vocab_size,
                 embed_dim=args.embed_dim,
                 bidirectional=args.bidirectional,
                 dropout=args.dropout,
                 attention_size=args.attention_size,
                 sequence_length=args.sequence_length)
    elif args.task_type == "regress":
        model = LSTMRegress(args.input_size, args.hidden_size,
                            args.num_layers, args.output_size,
                            batch_size=args.batch_size, bidirectional=args.bidirectional).to(args.device)
    else:
        logging.error("args.task_type required classify or regress, but input:" + args.task_type)
        return

    model.load_state_dict(torch.load(args.output_model)['model'])
    model.eval()
    logging.info('predicting...')

    for (seq, target) in tqdm(Dte):
        y.append(target.item())
        seq = seq.to(args.device)
        with torch.no_grad():
            y_pred = model(seq)
            if args.task_type == "classify":
                pred.append(torch.max(y_pred, 1)[1].item())
            else:
                pred.append(y_pred.item())
    with open(args.output_predict_file, "w") as f:
        for index in range(len(pred)):
            f.write("%s\t%s\n" % (y[index], pred[index]))
    logging.info("predict finished.")