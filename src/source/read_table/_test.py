# coding: utf-8
import read_table

read_table.read_table_to_hive('aip.item', column_list=['item_type', 'item_id'], filter_condition=None, start_dt=None, end_dt=None)