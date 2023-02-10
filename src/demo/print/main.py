# coding: utf-8
import my_print
import argparse


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--param', type=str, help='param')
    args = parser.parse_args()
    my_print.do_print(args.param)


if __name__ == '__main__':
    run()
