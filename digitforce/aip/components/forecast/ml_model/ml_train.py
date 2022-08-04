from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def ml_model_train(sdate, edate, image_tag="latest"):
    """
    大单过滤

    :param sdate: 开始时间
    :param edate: 结束时间
    :param image_tag: 组件版本
    :return: user_profile_calculator_op
    """
    return dsl.ContainerOp(name="ml_model_train",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command='bash',
                           arguments=['/data/entrypoint.sh','spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster', '--conf',
                                      'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/ibs/bin/python',
                                      '--conf', 'spark.yarn.dist.archives=hdfs:///user/awg/ibs.zip#environment',
                                      '--driver-memory', '8G', '--py-files', './forecast.zip,digitforce.zip',
                                      'forecast/ml_model/runnable/sales_ml_train.py', str(sdate),
                                      str(edate)])
