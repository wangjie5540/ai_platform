# coding: utf-8
import torch
import lstm_model
import argparse
from facade.components.ml.uplift import *


def run():
    print("uplift component running")
    # 解析输入参数
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_file_train', type=str, default="train.txt", help='input filename for train')
    parser.add_argument('--input_file_predict', type=str, default="test.txt", help='input filename for test')
    parser.add_argument('--train', type=bool, default=True, help='train or not')
    parser.add_argument('--predict', type=bool, default=True, help='predict or not')
    parser.add_argument('--output_model', type=str, default="output.model", help='output model')
    parser.add_argument('--output_predict_file', type=str, default="output.predict", help='output predict file name')

    parser.add_argument('--epochs', type=int, default=3, help='input dimension')
    parser.add_argument('--input_size', type=int, default=1, help='input dimension')
    parser.add_argument('--output_size', type=int, default=1, help='output dimension')
    parser.add_argument('--hidden_size', type=int, default=32, help='hidden size')
    parser.add_argument('--num_layers', type=int, default=2, help='num layers')
    parser.add_argument('--lr', type=float, default=0.001, help='learning rate')
    parser.add_argument('--batch_size', type=int, default=30, help='batch size')
    parser.add_argument('--optimizer', type=str, default='adam', help='type of optimizer')
    parser.add_argument('--device', default=torch.device("cuda" if torch.cuda.is_available() else "cpu"))
    parser.add_argument('--weight_decay', type=float, default=1e-4, help='weight decay')
    parser.add_argument('--bidirectional', type=bool, default=False, help='LSTM direction')

    args = parser.parse_args()
    print(f"参数解析完毕. args={args}]")

    # 训练
    if args.train:
        lstm_model.train(args)

    # 预测
    if args.predict:
        lstm_model.predict(args)

    # 向下游传递参数
    # TODO 后续将进行封装
    # component_helper.pass_output({'out_1': out_1})


if __name__ == '__main__':
    run()
