# coding: utf-8
import argparse
import digitforce.aip.common.utils.component_helper as component_helper
import my_sleep


def run():
    component_helper.init_config()
    parser = argparse.ArgumentParser()
    parser.add_argument('--minutes', type=int, help='sleep的分钟数')
    args = parser.parse_args()
    my_sleep.do_sleep(args.minutes)


if __name__ == '__main__':
    run()
