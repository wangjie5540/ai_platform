# coding:utf-8

import argparse
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--component_name', type=str, required=True, help='组件名称')
    args = parser.parse_args()
    component_path = os.path.join('src', *args.component_name.split('-'))
    print(component_path)
    os.system(
        f"docker build -t digit-force-docker.pkg.coding.net/ai-platform/ai-components/{args.component_name} {component_path}")


if __name__ == '__main__':
    main()
