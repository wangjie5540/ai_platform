# coding: utf-8
import my_print
import argparse
import json


def run():
    # 解析全局参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, help='your component name')
    parser.add_argument('--global_params', type=str, help='your global params')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)

    # 获取组件参数
    component_params = global_params[args.name]
    message = component_params['message']
    my_print.do_print(message)


if __name__ == '__main__':
    run()
