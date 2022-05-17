# coding: utf-8
from clickhouse_driver import Client
import argparse


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, required=True, help='clickhouse host')
    parser.add_argument('--port', type=int, required=True, help='clickhouse port')
    parser.add_argument('-f', '--file_path', type=str, required=True, help='输入文件路径')
    return parser.parse_args()


def fun():
    pass


if __name__ == '__main__':
    args = parse_arguments()
