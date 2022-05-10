# coding: utf-8
import uplift_model
import argparse
from facade.components.ml.uplift import *

if __name__ == '__main__':
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_1', type=str, required=True, help='样本数据')
    args = parser.parse_args()

    # 调用组件功能
    up = uplift_model.uplift_train(args.intput_1, out_1)

    # 向下游传递参数
    # TODO 后续将进行封装
    component_helper.pass_output({'out_1': out_1})
