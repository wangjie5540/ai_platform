
import torch
import lstm_model
import argparse
from digitforce.aip.common.logging_config import setup_console_log
import logging

def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()

    parser.add_argument('--task_type', type=str, default="regress", help='classify or regress')

    parser.add_argument('--input_file_train', type=str, default="train.txt.full", help='input filename for train')
    parser.add_argument('--input_file_valid', type=str, default="test.txt.full", help='input filename for valid')
    parser.add_argument('--input_file_predict', type=str, default="test.txt.full", help='input filename for test')

    parser.add_argument('--train', type=bool, default=True, help='train or not')
    parser.add_argument('--valid', type=bool, default=True, help='valid or not')
    parser.add_argument('--predict', type=bool, default=True, help='predict or not')

    parser.add_argument('--output_model', type=str, default="output.model", help='output model')
    parser.add_argument('--output_predict_file', type=str, default="output.predict", help='output predict file name')

    parser.add_argument('--epochs', type=int, default=3, help='input dimension')
    parser.add_argument('--input_size', type=int, default=1, help='input dimension')
    parser.add_argument('--output_size', type=int, default=1, help='output dimension')
    parser.add_argument('--vocab_size', type=int, default=9277, help='vocab_size')
    parser.add_argument('--embed_dim', type=int, default=70, help='embed_dim')
    parser.add_argument('--dropout', type=float, default=0.5, help='dropout')
    parser.add_argument('--hidden_size', type=int, default=32, help='hidden size')
    parser.add_argument('--num_layers', type=int, default=1, help='num layers')
    parser.add_argument('--attention_size', type=int, default=16, help='attention_size')
    parser.add_argument('--sequence_length', type=int, default=16, help='sequence_length')
    parser.add_argument('--lr', type=float, default=0.001, help='learning rate')
    parser.add_argument('--batch_size', type=int, default=17, help='batch size')
    parser.add_argument('--optimizer', type=str, default='adam', help='type of optimizer')
    parser.add_argument('--device', default=torch.device("cuda" if torch.cuda.is_available() else "cpu"))
    parser.add_argument('--weight_decay', type=float, default=1e-4, help='weight decay')
    parser.add_argument('--bidirectional', type=bool, default=False, help='LSTM direction')

    args = parser.parse_args()
    logging.info(f"参数解析完毕. args={args}")

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
