# coding: utf-8
import uplift_model
import argparse
from facade.components.ml.uplift import *
import os


def run():
    print("uplift component running")
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_1', type=str, required=True, help='样本数据')
    args = parser.parse_args()
    print(f"参数解析完毕. [input_1={args.input_1}]")
    # 调用组件功能
    model_path = os.path.join(global_constant.MOUNT_NFS_DIR, 'uplift.model')
    uplift_model.uplift_train(args.input_1, model_path)

    # 向下游传递参数
    # TODO 后续将进行封装
    component_helper.pass_output(model_path, 1)


if __name__ == '__main__':
    run()
