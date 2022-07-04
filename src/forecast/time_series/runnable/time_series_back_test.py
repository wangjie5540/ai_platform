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
    param={
        'ts_model_list':['arima','arimax','ar','ma','sarima','sarimax','arx','croston','crostontsb','holt','holt-winter','ses','stl','theta','stlf'],
        'y_type_list':['c'],
        'mode_type': 'sp',
        'forcast_start_date':'20211009',
        'predict_len':14,
        'key_list':['shop_id','goods_id','y_type','apply_model'],
        'apply_model_index':3,
        'step_len':5,
        'mode_type':'sp',
        'purpose':'back_test',
        'time_col': 'dt',
        'time_type': 'day',
        'method_param_all': {
            'arima': {
                'param':
                    {
                        "exog": None,
                        "order": (2, 1, 1),
                        "seasonal_order": (0, 0, 0, 0),
                        "trend": None,
                        "enforce_stationarity": True,
                        "enforce_invertibility": True,
                        "concentrate_scale": False,
                        "trend_offset": 1,
                        "dates": None,
                        "freq": None,
                        "missing": 'none',
                        "validate_specification": True
                    },
                'param_fit':
                    {
                        "start_params": None,
                        "transformed": True,
                        "includes_fixed": False,
                        "method": None,
                        "method_kwargs": None,
                        "gls": None,
                        "gls_kwargs": None,
                        "cov_type": None,
                        "cov_kwds": None,
                        "return_params": False,
                        "low_memory": False
                    }
            },
            'arimax': {
                'param': {
                    "exog": None,
                    "order": (1, 1, 1),
                    "seasonal_order": (0, 0, 0, 0),
                    "trend": None,
                    "enforce_stationarity": True,
                    "enforce_invertibility": True,
                    "concentrate_scale": False,
                    "trend_offset": 1,
                    "dates": None,
                    "freq": None,
                    "missing": 'none',
                    "validate_specification": True
                },
                'param_fit': {
                    "start_params": None,
                    "transformed": True,
                    "includes_fixed": False,
                    "method": None,
                    "method_kwargs": None,
                    "gls": None,
                    "gls_kwargs": None,
                    "cov_type": None,
                    "cov_kwds": None,
                    "return_params": False,
                    "low_memory": False
                }
            },
            'ar': {
                'param': {
                    "trend": 'c',
                    'lags': None,
                    "seasonal": False,
                    "exog": None,
                    "hold_back": None,
                    "period": None,
                    "missing": 'none',
                    "deterministic": None,
                    "old_names": False
                },
                'param_fit': {
                    "cov_type": 'nonrobust',
                    "cov_kwds": None,
                    "use_t": False
                }
            },
            'ma': {
                'param': {
                    "exog": None,
                    "order": (0, 0, 1),
                    "seasonal_order": (0, 0, 0, 0),
                    "trend": None,
                    "enforce_stationarity": True,
                    "enforce_invertibility": True,
                    "concentrate_scale": False,
                    "trend_offset": 1,
                    "dates": None,
                    "freq": None,
                    "missing": 'none',
                    "validate_specification": True
                }, 'param_fit': {
                    "start_params": None,
                    "transformed": True,
                    "includes_fixed": False,
                    "method": None,
                    "method_kwargs": None,
                    "gls": None,
                    "gls_kwargs": None,
                    "cov_type": None,
                    "cov_kwds": None,
                    "return_params": False,
                    "low_memory": False
                }
            },
            'sarima': {
                'param': {
                    "exog": None,
                    "order": (1, 0, 0),
                    "seasonal_order": (0, 0, 0, 0),
                    "trend": None,
                    "measurement_error": False,
                    "time_varying_regression": False,
                    "mle_regression": True,
                    "simple_differencing": False,
                    "enforce_stationarity": True,
                    "enforce_invertibility": True,
                    "hamilton_representation": False,
                    "concentrate_scale": False,
                    "trend_offset": 1,
                    "use_exact_diffuse": False,
                    "dates": None,
                    "freq": None,
                    "missing": 'none',
                    "validate_specification": True
                }, 'param_fit': {
                    "start_params": None,
                    "transformed": True,
                    "includes_fixed": False,
                    "cov_type": None,
                    "cov_kwds": None,
                    "method": 'lbfgs',
                    "maxiter": 50,
                    "full_output": 1,
                    "disp": 5,
                    "callback": None,
                    "return_params": False,
                    "optim_score": None,
                    "optim_complex_step": None,
                    "optim_hessian": None,
                    "flags": None,
                    "low_memory": False
                }
            },
            'sarimax': {
                'param': {
                    "exog": None,
                    "order": (1, 0, 0),
                    "seasonal_order": (0, 0, 0, 0),
                    "trend": None,
                    "measurement_error": False,
                    "time_varying_regression": False,
                    "mle_regression": True,
                    "simple_differencing": False,
                    "enforce_stationarity": True,
                    "enforce_invertibility": True,
                    "hamilton_representation": False,
                    "concentrate_scale": False,
                    "trend_offset": 1,
                    "use_exact_diffuse": False,
                    "dates": None,
                    "freq": None,
                    "missing": 'none',
                    "validate_specification": True
                }, 'param_fit': {
                    "start_params": None,
                    "transformed": True,
                    "includes_fixed": False,
                    "cov_type": None,
                    "cov_kwds": None,
                    "method": 'lbfgs',
                    "maxiter": 50,
                    "full_output": 1,
                    "disp": 5,
                    "callback": None,
                    "return_params": False,
                    "optim_score": None,
                    "optim_complex_step": None,
                    "optim_hessian": None,
                    "flags": None,
                    "low_memory": False
                }
            },
            'arx': {
                'param': {
                    "trend": 'c',
                    "lags": None,
                    "seasonal": False,
                    "exog": None,
                    "hold_back": None,
                    "period": None,
                    "missing": 'none',
                    "deterministic": None,
                    "old_names": False
                }, 'param_fit': {
                    "cov_type": 'nonrobust',
                    "cov_kwds": None,
                    "use_t": False
                }
            },
            'croston': {
                'param': {
                    "curDate": '20211201',
                    "extra_periods": 4,
                    "alpha": 0,
                    "a": None,
                    "p": None,
                    "f": None,
                    "q": None,
                    "cols": None
                }
            },
            'crostontsb': {
                'param': {
                    "curDate": '20211201',
                    "extra_periods": 4,
                    "alpha": 0,
                    "beta": 0,
                    "a": None,
                    "p": None,
                    "f": None,
                    "cols": None
                }
            },
            'holt': {
                'param': {
                    "exponential": False,
                    "damped_trend": False,
                    "initialization_method": None,
                    "initial_level": None,
                    "initial_trend": None
                },
                'param_fit': {
                    "smoothing_level": None,
                    "smoothing_trend": None,
                    "damping_trend": None,
                    "optimized": True,
                    "start_params": None,
                    "initial_level": None,
                    "initial_trend": None,
                    "use_brute": True,
                    "use_boxcox": None,
                    "remove_bias": False,
                    "method": None,
                    "minimize_kwargs": None
                }
            },
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
                    "missing": "none",
                    "dates": None
                },
                'param_fit': {
                    "smoothing_level": None,
                    "smoothing_trend": None,
                    "smoothing_seasonal": None,
                    "damping_trend": None,
                    "optimized": True,
                    "remove_bias": False,
                    "start_params": None,
                    "method": None,
                    "minimize_kwargs": None,
                    "use_brute": True,
                    "use_boxcox": None,
                    "use_basinhopping": None,
                    "initial_level": None,
                    "initial_trend": None
                }
            },
            'ses': {
                'param': {
                    "initialization_method": None,
                    "initial_level": None
                },
                'param_fit': {
                    "smoothing_level": None,
                    "optimized": True,
                    "start_params": None,
                    "initial_level": None,
                    "use_brute": True,
                    "use_boxcox": None,
                    "remove_bias": False,
                    "method": None,
                    "minimize_kwargs": None
                }
            },
            'stl': {
                'param': {
                    "period": None,
                    "seasonal": 7,
                    "trend": None,
                    "low_pass": None,
                    "seasonal_deg": 1,
                    "trend_deg": 1,
                    "low_pass_deg": 1,
                    "robust": False,
                    "seasonal_jump": 1,
                    "trend_jump": 1,
                    "low_pass_jump": 1
                },
                'param_fit': {
                    "inner_iter": None,
                    "outer_iter": None
                }
            },
            'theta': {
                'param': {
                    "period": 14,
                    "deseasonalize": True,
                    "use_test": True,
                    "method": 'auto',
                    "difference": False
                },
                'param_fit': {
                    "use_mle": False,
                    "disp": False
                }
            },
            'stlf': {
                'param': {
                    "model": "ARIMA",
                    "period": None,
                    "seasonal": 7,
                    "trend": None,
                    "low_pass": None,
                    "seasonal_deg": 1,
                    "trend_deg": 1,
                    "low_pass_deg": 1,
                    "robust": False,
                    "seasonal_jump": 1,
                    "trend_jump": 1,
                    "low_pass_jump": 1
                },
                'param_fit': {
                    "inner_iter": None,
                    "outer_iter": None
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