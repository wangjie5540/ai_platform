# -*- coding: utf-8 -*-
# @Time : 2022/05/26
# @Author : Arvin
"""
无销量还原天级别销量还原
"""
from forecast.data_processing.sp.sp_data_adjust import no_sales_adjust

try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import argparse
import traceback
from forecast.common.log import get_logger
from forecast.common.toml_helper import TomlOperation


def load_params():
    """运行run方法时"""
    param_cur = {
        'mode_type': 'sp',
        'sdate': '20210101',
        'edate': '20220101',
        'col_openinv': 'opening_inv',
        'col_endinv': 'ending_inv',
        'col_key': ['shop_id', 'goods_id'],
        'col_category': 'category4_code',
        'col_time': 'dt',
        'w':2,
        'col_qty':'fill_sum_qty',
        'join_key':['shop_id','goods_id','dt']
    }
    f = TomlOperation(os.getcwd()+"/forecast/data_processing/config/param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def parse_arguments():
    """
    #开发测试用
    :return:
    """
    params = load_params()
    parser = argparse.ArgumentParser(description='no_sales_adjust')
    parser.add_argument('--param', default=params, help='arguments')
    parser.add_argument('--spark', default=spark, help='spark')
    args = parser.parse_args(args=[])
    return args


def run():
    """
    跑接口
    :return:
    """
    logger_info = get_logger()
    logger_info.info("LOADING···")
    args = parse_arguments()
    param = args.param
    spark = args.spark

    logger_info.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logger_info.info("RUNNING···")
            no_sales_adjust(spark, param)
        else:
            # pandas版本
            pass
        status = "SUCCESS"
        logger_info.info("SUCCESS")
    except Exception as e:
        status = "ERROR"
        logger_info.info(traceback.format_exc())
    return status

if __name__ == "__main__":
    run()