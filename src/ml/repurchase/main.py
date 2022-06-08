import argparse
import repurchase
from digitforce.aip.common.logging_config import setup_logging
import logging
import json

def run():
    setup_logging("info.log", "error.log")
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_train_params', type=str,  help='params file path while training')
    parser.add_argument('--train', type=bool, default=True, help='train or not')
    parser.add_argument('--predict', type=bool, default=True, help='predict or not')
    parser.add_argument('--output_model_path', type=str, default='hdfs:///usr/algorithm/cd/fugou/model', help='head of model upload path while training')
    parser.add_argument('--input_predict_params', type=str, help='params file path while predicting')
    parser.add_argument('--output_res_path', type=str, default='hdfs:///usr/algorithm/cd/fugou/result', help='head of result upload path while predicting')

    args = parser.parse_args()
    logging.info(f'Argument Parameters: args={args}')

    if args.train:
        if not args.input_train_params:
            logging.info('Please input parameters file for training')
        elif not args.output_model_path:
            logging.info('Please input output model path for training')
        else:
            with open(args.input_train_params, 'r') as file:
                data = file.readline()
            logging.info(f'Task parameters: {data}')
            params_dict = json.loads(data)
            repurchase.train(params_dict, args.output_model_path)

    if args.predict:
        if not args.input_predict_params:
            logging.info('Please input parameters file for predicting')
        elif not args.output_res_path:
            logging.info('Please input result file path for predicting')
        else:
            with open(args.input_predict_params, 'r') as file:
                data = file.readline()
            logging.info(f'Crowd parameters: {data}')
            params_dict = json.loads(data)
            repurchase.predict(params_dict, args.output_res_path)

if __name__ == '__main__':
    run()