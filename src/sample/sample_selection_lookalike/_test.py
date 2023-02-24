#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/7 17:49
@desc:
'''
# from sample_select import start_sample_selection
#
# event_code = '申购'
# table, col = start_sample_selection(event_code, pos_sample_proportion=0.5,
#                                     pos_sample_num=10000)
# print(table, col)


from digitforce.aip.common.utils.spark_helper import SparkClient
spark_client = SparkClient.get()

print(11)