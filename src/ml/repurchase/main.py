import argparse
import repurchase
# from digitforce.aip.common.logging_config import setup_logging
import logging
import json
from param_test import params_dict


def run():
    # setup_logging("info.log", "error.log")
    parser = argparse.ArgumentParser()

    # parser.add_argument('--input_train_params', type=str,  help='params file path while training')
    # parser.add_argument('--train', type=bool, default=True, help='train or not')
    # parser.add_argument('--predict', type=bool, default=True, help='predict or not')
    # parser.add_argument('--output_model_filename', type=str, default='fugou_model',
    #                     help='head of model upload path while training')
    # parser.add_argument('--input_predict_params', type=str, help='params file path while predicting')
    # parser.add_argument('--output_res_path', type=str, default='hdfs:///usr/algorithm/cd/fugou/result', help='head of result upload path while predicting')
    parser.add_argument('--solution_id', type=str, default='', help='solution id')
    parser.add_argument('--instance_id', type=str, default='', help='instance id')

    args = parser.parse_args()
    print(f'Argument Parameters: args={args}')

    repurchase.train(params_dict, args.solution_id)
    # if args.train:
    #     # if not args.input_train_params:
    #     #     logging.info('Please input parameters file for training')
    #     if not args.output_model_filename:
    #         logging.info('Please input output model path for training')
    #     else:
    #         # with open(args.input_train_params, 'r') as file:
    #         #     data = file.readline()
    #         # logging.info(f'Task parameters: {data}')
    #         # params_dict = json.loads(data)

    #
    # if args.predict:
    #     if not args.input_predict_params:
    #         logging.info('Please input parameters file for predicting')
    #     elif not args.output_res_path:
    #         logging.info('Please input result file path for predicting')
    #     else:
    #         with open(args.input_predict_params, 'r') as file:
    #             data = file.readline()
    #         logging.info(f'Crowd parameters: {data}')
    #         params_dict = json.loads(data)
    #         repurchase.predict(params_dict, args.output_res_path)


if __name__ == '__main__':
    run()
