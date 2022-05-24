
import torch
from torch import nn
from models import LSTM
from data_process import nn_seq, setup_seed
from tqdm import tqdm

setup_seed(20)


def train(args):
    Dtr = nn_seq(args.batch_size, args.input_file_train)
    if args.valid:
        Dva = nn_seq(args.batch_size, args.input_file_valid)

    model = LSTM(batch_size=args.batch_size,
                                  output_size=args.output_size,
                                  hidden_size=args.hidden_size,
                                  vocab_size=args.vocab_size,
                                  embed_dim=args.embed_dim,
                                  bidirectional=args.bidirectional,
                                  dropout=args.dropout,
                                  attention_size=args.attention_size,
                                  sequence_length=args.sequence_length)

    loss_function = nn.CrossEntropyLoss()
    if args.optimizer == 'adam':
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr,
                                     weight_decay=args.weight_decay)
    else:
        optimizer = torch.optim.SGD(model.parameters(), lr=args.lr,
                                    momentum=0.9, weight_decay=args.weight_decay)
    # training
    loss = 0
    for i in tqdm(range(args.epochs)):
        model.train()
        total_loss = 0
        count = 0
        for (seq, label) in Dtr:
            seq = seq.to(args.device)
            label = label.to(args.device)
            y_pred = model(seq)
            loss = loss_function(y_pred, label)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total_loss += loss.data
            count += 1
        # print('train-epoch', i, ':', loss.item())
        print('train-epoch', i, ':', total_loss.item()/count)

        if args.valid:
            model.eval()
            count = 0
            total_loss = 0
            corrects = 0
            for (seq, label) in Dva:
                seq = seq.to(args.device)
                label = label.to(args.device)
                y_pred = model(seq)
                loss = loss_function(y_pred, label)
                count += 1
                total_loss += loss.data
                correct = (torch.max(y_pred, 1)[1].view(label.size()).data == label.data).sum()
                corrects += correct
            acc = corrects.item()/(count * len(label))
            print("valid: epoch {} | loss: {} | acc: {} | corrects: {}".format(i, total_loss.item()/count, acc, corrects))

    state = {'model': model.state_dict(), 'optimizer': optimizer.state_dict()}
    torch.save(state, args.output_model)


def predict(args):
    args.batch_size = 1
    Dte = nn_seq(args.batch_size, args.input_file_predict)

    pred = []
    y = []
    print('loading model...')
    model = LSTM(batch_size=args.batch_size,
                 output_size=args.output_size,
                 hidden_size=args.hidden_size,
                 vocab_size=args.vocab_size,
                 embed_dim=args.embed_dim,
                 bidirectional=args.bidirectional,
                 dropout=args.dropout,
                 attention_size=args.attention_size,
                 sequence_length=args.sequence_length)

    model.load_state_dict(torch.load(args.output_model)['model'])
    model.eval()
    print('predicting...')
    for (seq, target) in tqdm(Dte):
        y.append(target.item())
        seq = seq.to(args.device)
        with torch.no_grad():
            y_pred = model(seq)
            y_pred_class = torch.max(y_pred, 1)[1].item()
            pred.append(y_pred_class)

    with open(args.output_predict_file, "w") as f:
        for index in range(len(pred)):
            f.write("%s\t%s\n" % (y[index], pred[index]))