#!/usr/bin/env python3
# encoding: utf-8

import os

from digitforce.aip.common.utils import component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from lookalike_model_train import train


def run():
    # 参数解析
    # 参数解析
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {
    #         "raw_sample_table_name": "algorithm.tmp_aip_sample",
    #         "model_sample_table_name": "algorithm.tmp_aip_model_sample",
    #     }})
    # os.environ["name"] = "op_name"

    # todo tmp
    os.environ["train_dataset_table_name"] = "algorithm.train_dataset_table_name"
    os.environ["test_dataset_table_name"] = "algorithm.test_dataset_table_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--train_dataset_table_name", type=str, required=False, help="训练集")
    df_argument_helper.add_argument("--test_dataset_table_name", type=str, required=False, help="测试集")

    df_argument_helper.add_argument("--batch_size", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--lr", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--dnn_dropout", type=str, required=False, help="样本数据")

    train_dataset_table_name = df_argument_helper.get_argument("train_dataset_table_name")
    test_dataset_table_name = df_argument_helper.get_argument("test_dataset_table_name")

    batch_size = int(df_argument_helper.get_argument("batch_size"))
    lr = float(df_argument_helper.get_argument("lr"))
    dnn_dropout = float(df_argument_helper.get_argument("dnn_dropout"))
    is_automl = df_argument_helper.get_argument("is_automl")
    is_automl = str(is_automl).lower() not in ["", "none", "false"]
    train(train_data_table_name=train_dataset_table_name,
          test_data_table_name=test_dataset_table_name,
          batch_size=batch_size, lr=lr,
          dnn_dropout=dnn_dropout,

          is_automl=is_automl,
          model_user_feature_table_name=None,
          user_vec_table_name=None
          )

    if is_train == 'True':
        global_params = json.loads(args.global_params)
        component_params = global_params[args.name]
        dnn_dropout = component_params["dnn_dropout"]
        batch_size = component_params["batch_size"]
        lr = component_params["lr"]
    else:
        dnn_dropout = args.dnn_dropout
        batch_size = args.batch_size
        lr = args.lr

    component_helper.write_output("train_dataset_table_name", train_dataset_table_name)
    component_helper.write_output("test_dataset_table_name", test_dataset_table_name)
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    # parser.add_argument("--name", type=str, required=True, help="名称")
    # parser.add_argument("--train_data", type=str, required=True, help="训练数据")
    # parser.add_argument("--test_data", type=str, required=True, help="测试数据")
    # parser.add_argument("--user_data", type=str, required=True, help="用户数据")
    # parser.add_argument("--other_data", type=str, required=True, help="其他数据")
    #
    # parser.add_argument("--dnn_dropout", type=float, required=False, help="dnn_dropout")
    # parser.add_argument("--batch_size", type=int, required=False, help="batch_size")
    # parser.add_argument("--lr", type=float, required=False, help="lr")
    # parser.add_argument("--is_train", type=str, default="True", required=False, help="训练标识")


if __name__ == '__main__':
    run()
