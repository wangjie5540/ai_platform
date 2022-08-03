# -*- coding: utf-8 -*-
# @Time : 2022/06/29
# @Author : Arvin
from forecast.common.mysql import get_data_from_mysql
from forecast.common.reference_package import *
from digitforce.aip.common.spark_helper import *


def data_prepare(spark, params_data_prepare):
    method = params_data_prepare['method']
    sales_table = params_data_prepare['sales_table']
    col_key = params_data_prepare['col_key']
    config_columns = params_data_prepare['config_columns']
    col_sku_category = params_data_prepare['col_sku_category']
    shops = params_data_prepare['shop_list']
    w_tail = params_data_prepare['w_tail']
    w_ls = params_data_prepare['w_ls']
    col_time = params_data_prepare['col_time']
    col_qty = params_data_prepare['col_qty']
    threshold_count = params_data_prepare['threshold_count']
    edate = params_data_prepare['edate']
    threshold_proportion = params_data_prepare['threshold_proportion']
    threshold_sales = params_data_prepare['threshold_sales']
    # data_prepare_table = params_data_prepare['data_prepare_table']
    sparkdf_sales = read_table(spark, sales_table, sdt='N', partition_list=shops)
    config_tables = params_data_prepare['config_tables']
    task_id = params_data_prepare['task_id']
    config_table_list = []

    # 销量分层
    # if 'sales_classfify' in method:
    #     sparkdf_classfiy = read_table()
    #     config_table_list.append(sparkdf_classfiy)
    #
    # # 配置信息
    # if 'item_config' in method:
    #     sparkdf_item = get_item_config_info(spark, sparkdf_sales, config_columns, col_key, shops)
    #     config_table_list.append(sparkdf_item)
    #
    # if '' in method:
    #     pass

    table_names = locals()
    for table_name in config_tables:
        table_names[table_name] = read_table(spark, table_name)
        config_table_list.append(table_names[table_name])

    merge_info = reduce(lambda l, r: l.join(r, col_key, 'left'), config_table_list)
    merge_info.select(config_columns + col_sku_category).filter("task_id='{0}'".format(task_id))
    # save_table(spark, merge_info, data_prepare_table, partition=["shop_id"])
    print("数据准备已经完成！")
    return merge_info
