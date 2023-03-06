
# encoding: utf-8
import argparse
import json

from model_train import start_model_train
from digitforce.aip.common.utils.argument_helper import df_argument_helper
import digitforce.aip.common.utils.component_helper as component_helper
component_helper.init_config()


def run():
    # 参数解析
    df_argument_helper.add_argument(
        "--global_params", type=str, required=False, help="全局参数"
    )
    df_argument_helper.add_argument(
        "--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument(
        "--train_table_name", type=str, required=False, help="训练表名称"
    )
    df_argument_helper.add_argument(
        "--test_table_name", type=str, required=False, help="测试表名称"
    )
    df_argument_helper.add_argument(
        "--learning_rate", type=str, required=False, help="learning_rate"
    )
    df_argument_helper.add_argument(
        "--n_estimators", type=str, required=False, help="n_estimators"
    )
    df_argument_helper.add_argument(
        "--max_depth", type=str, required=False, help="max_depth"
    )
    df_argument_helper.add_argument(
        "--scale_pos_weight", type=str, required=False, help="scale_pos_weight"
    )
    df_argument_helper.add_argument(
        "--is_automl", type=str, default=False, required=False, help="训练标识"
    )
    df_argument_helper.add_argument(
        "--model_and_metrics_data_hdfs_path", type=str, required=False, help="模型管理"
    )

    is_automl = df_argument_helper.get_argument("is_automl")
    is_automl = str(is_automl).lower() == "true"

    learning_rate = float(df_argument_helper.get_argument("learning_rate"))
    n_estimators = int(df_argument_helper.get_argument("n_estimators"))
    max_depth = int(df_argument_helper.get_argument("max_depth"))
    scale_pos_weight = float(
        df_argument_helper.get_argument("scale_pos_weight"))

    train_table_name = df_argument_helper.get_argument("train_table_name")
    test_table_name = df_argument_helper.get_argument("test_table_name")
    model_and_metrics_data_hdfs_path = df_argument_helper.get_argument(
        "model_and_metrics_data_hdfs_path"
    )
    start_model_train(
        train_table_name=train_table_name,
        test_table_name=test_table_name,
        learning_rate=learning_rate,
        n_estimators=n_estimators,
        max_depth=max_depth,
        scale_pos_weight=scale_pos_weight,
        is_automl=is_automl,
        model_and_metrics_data_hdfs_path=model_and_metrics_data_hdfs_path,
    )

    outputs = {}
    # component_helper.write_output(outputs)


if __name__ == "__main__":
    run()
