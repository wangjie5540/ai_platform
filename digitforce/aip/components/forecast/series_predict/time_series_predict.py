# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def time_series_predict(forecast_start_date,purpose,time_type, image_tag="latest"):
    '''
    time_series_predict：时序模型预测
    :param 预测模块需要的参数
    :param forecast_start_date: 预测开始时间
    :param purpose: 模型类型：预测or回测
    :param time_type:预测粒度
    :param predict_model_output: 模型输出文件
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ['spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster', '--conf',
                  'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/ibs/bin/python',
                  '--conf', 'spark.yarn.dist.archives=hdfs:///user/awg/ibs.zip#environment',
                  '--driver-memory', '8G', '--py-files',
                  './forecast.zip,digitforce.zip', 'forecast/time_series/runnable/submit_predict.py',
                  str(forecast_start_date),
                  str(purpose),
                  str(time_type)
                 ]
    return dsl.ContainerOp(name="time_series_predict",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command='/data/entrypoint.sh',
                           arguments=arguments)
