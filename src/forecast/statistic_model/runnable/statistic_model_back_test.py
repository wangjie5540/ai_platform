# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    统计模型：对外提供的接口：回测
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
from statistic_model.sp.back_test_sp import back_test_sp
from common.log import get_logger

def statistic_model_back_test(param,spark=None):
    """
    统计模型回测
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
    param={
        'ts_model_list':['ES'],
        'y_type_list':['c'],
        'mode_type': 'sp',
        'forcast_start_date':'20211009',
        'predict_len':14,
        'key_list':['shop_id','goods_id','y_type','apply_model'],
        'apply_model_index':3,
        'step_len':5,
        'mode_type':'sp',
        'purpose':'back_test'
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
    statistic_model_back_test(param,spark)

if __name__ == "__main__":
    run()