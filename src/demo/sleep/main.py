# coding: utf-8
import subprocess

cmd = '''
cp /mnt/config/aip_config.yaml /usr/local/etc
cp /mnt/config/kube_config /root/.kube/config
cp /mnt/config/hdfs-site.xml $SPARK_HOME/conf
'''
subprocess.check_call(cmd, shell=True)

import argparse
import digitforce.aip.common.utils.spark_helper as spark_helper


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--minutes', type=int, help='sleep的分钟数')
    args = parser.parse_args()
    # my_sleep.do_sleep(args.minutes)
    df = spark_helper.spark_client.get_starrocks_table_df("algorithm.sample_jcbq_zxr")
    df.show()


if __name__ == '__main__':
    run()
