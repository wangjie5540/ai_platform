#!/usr/bin/env python3
# encoding: utf-8
import argparse
import json

from model_train import start_model_train


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--train_data", type=str, required=True, help="训练数据")
    parser.add_argument("--test_data", type=str, required=True, help="测试数据")
    parser.add_argument("--learning_rate", type=float, required=False, help="learning_rate")
    parser.add_argument("--n_estimators", type=int, required=False, help="n_estimators")
    parser.add_argument("--max_depth", type=int, required=False, help="max_depth")
    parser.add_argument("--scale_pos_weight", type=float, required=False, help="scale_pos_weight")
    parser.add_argument("--is_train", type=str, default="True", required=False, help="训练标识")

    args = parser.parse_args()
    is_train = args.is_train

    if is_train == 'True':
        global_params = json.loads(args.global_params)
        component_params = global_params[args.name]
        learning_rate = component_params["learning_rate"]
        n_estimators = component_params["n_estimators"]
        max_depth = component_params["max_depth"]
        scale_pos_weight = component_params["scale_pos_weight"]
    else:
        learning_rate = args.learning_rate
        n_estimators = args.n_estimators
        max_depth = args.max_depth
        scale_pos_weight = args.scale_pos_weight

    start_model_train(args.train_data, args.test_data,
                      learning_rate=learning_rate, n_estimators=n_estimators, max_depth=max_depth,
                      scale_pos_weight=scale_pos_weight,
                      is_train=is_train)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
