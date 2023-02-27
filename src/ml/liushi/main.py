#!/usr/bin/env python3
# encoding: utf-8
import argparse
import json

from model_train import start_model_train
from digitforce.aip.common.utils.argument_helper import df_argument_helper
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 初始化组件
    component_helper.init_config()
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument("--train_data", type=str, required=False, help="训练数据")
    df_argument_helper.add_argument("--test_data", type=str, required=False, help="测试数据")
    df_argument_helper.add_argument("--learning_rate", type=str, required=False, help="learning_rate")
    df_argument_helper.add_argument("--n_estimators", type=str, required=False, help="n_estimators")
    df_argument_helper.add_argument("--max_depth", type=str, required=False, help="max_depth")
    df_argument_helper.add_argument("--scale_pos_weight", type=str, required=False, help="scale_pos_weight")
    df_argument_helper.add_argument("--is_automl", type=str, default=False, required=False, help="训练标识")
    df_argument_helper.add_argument("--model_and_metrics_data_hdfs_path", type=str,
                                    required=False, help="模型管理")

    is_automl = df_argument_helper.get_argument("is_automl")
    is_automl = str(is_automl).lower() == "true"

    learning_rate = float(df_argument_helper.get_argument("learning_rate"))
    n_estimators = int(df_argument_helper.get_argument("n_estimators"))
    max_depth = int(df_argument_helper.get_argument("max_depth"))
    scale_pos_weight = float(df_argument_helper.get_argument("scale_pos_weight"))

    train_data = df_argument_helper.get_argument("train_data")
    test_data = df_argument_helper.get_argument("test_data")

    model_and_metrics_data_hdfs_path = df_argument_helper.get_argument("model_and_metrics_data_hdfs_path")
    start_model_train(train_data, test_data,
                      learning_rate=learning_rate, n_estimators=n_estimators, max_depth=max_depth,
                      scale_pos_weight=scale_pos_weight,
                      is_automl=is_automl,
                      model_and_metrics_data_hdfs_path=model_and_metrics_data_hdfs_path)


if __name__ == '__main__':
    run()
