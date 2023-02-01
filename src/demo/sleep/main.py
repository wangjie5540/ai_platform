# coding: utf-8
import my_sleep
import argparse


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--minutes', type=str, help='sleep的分钟数')
    args = parser.parse_args()
    my_sleep.do_sleep(args.param)


if __name__ == '__main__':
    run()
