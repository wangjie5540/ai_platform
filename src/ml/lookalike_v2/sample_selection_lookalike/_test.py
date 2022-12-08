#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/7 17:49
@desc:
'''
from src.ml.lookalike_v2.sample_selection_lookalike.sample_select import start_sample_selection

data_table_name = 'aip.read_table_4698160228885073921'
columns = ['custom_id', 'fund_code', 'trade_type']
event_code = {'buy': 'fund_buy'}
table, col = start_sample_selection(data_table_name, columns, event_code, pos_sample_proportion=0.5, pos_sample_num=200000)
print(table,col)