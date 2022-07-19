# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：对外提供的接口：回测
"""
import os
try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import sys
import json
import traceback
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../../'))
sys.path.append(file_path)
import argparse
from time_series.sp.backup_test_for_time_series_sp import back_test_sp
from common.log import get_logger

def time_series_back_test(param,spark=None):
    """
    预测模型回测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """
    logger_info=get_logger()
    mode_type='sp'#先给个默认值
    status=False
    if 'mode_type' in param.keys():
        mode_type=param['mode_type']
    try:
        if mode_type=='sp':#spark版本
            status=back_test_sp(param,spark)
        else:#pandas版本
            pass
        logger_info.info(str(param))
    except Exception as e:
        logger_info.info(traceback.format_exc())
    return status

#为了开发测试用，正式环境记得删除
def param_default():
    param = {
        'ts_model_list': ['holt-winter'],
        'y_type_list': ['c'],
        'mode_type': 'sp',
        'forcast_start_date': '20211212',
        'predict_len': 14,
        'key_list': ['shop_id', 'goods_id', 'y_type', 'apply_model'],
        'apply_model_index': 2,
        'step_len': 1,
        'mode_type': 'sp',
        'purpose': 'back_test',
        'time_col': 'dt',
        'col_qty': 'th_y',
        'time_type': 'day',
        'cols_feat_y': ['shop_id', 'goods_id', 'th_y', 'dt'],
        'sdate': '20210101',
        'edate': '20211211',
        'apply_model': 'apply_model',
        'partitions': ['shop_id', 'apply_model'],
        'key_cols': ['shop_id', 'goods_id', 'apply_model'],
        'feat_y': 'ai_dm_dev.no_sales_adjust_0620',
        'table_sku_group': 'ai_dm_dev.model_selection_grouping_table_0620',
        'output_table': 'ai_dm_dev.model_backup_test_result',
        'prepare_data_table': 'ai_dm_dev.prepare_data_result',
        'method_param_all': {
            'holt-winter': {
                'param': {
                    "trend": None,
                    "damped_trend": False,
                    "seasonal": None,
                    "seasonal_periods": None,
                    "initialization_method": "estimated",
                    "initial_level": None,
                    "initial_trend": None,
                    "initial_seasonal": None,
                    "use_boxcox": False,
                    "bounds": None,
                    "freq": None,
                    "missing": "missing",
                    "dates": None
                }
            }
        }
    }
    return param

def parse_arguments():
    """
    解析参数
    :return:
    """
    param=param_default()#开发测试用
    parser=argparse.ArgumentParser(description='time series predict')
    parser.add_argument('--param',default=param,help='arguments')
    parser.add_argument('--spark',default=None,help='spark')
    args=parser.parse_args()
    return args

def run():
    """
    跑接口
    :return:
    """
    args=parse_arguments()
    param=args.param
    spark=args.spark
    if isinstance(param,str):
        param=json.loads(param)
    time_series_back_test(param,spark)

if __name__ == "__main__":
    run()