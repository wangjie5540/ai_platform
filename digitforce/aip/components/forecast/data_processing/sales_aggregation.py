from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def sales_aggregation(sdate, edate,input_table,output_table,agg_func,col_qty,agg_type, image_tag="latest"):
    """

    :param sdate: 开始时间
    :param edate: 结束时间
    :param image_tag: 组件版本
    :return: user_profile_calculator_op
    """
    return dsl.ContainerOp(name="sales_aggregation",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command='/data/entrypoint.sh',
                           arguments=['spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster', '--conf',
                                      'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/ibs/bin/python',
                                      '--conf', 'spark.yarn.dist.archives=hdfs:///user/awg/ibs.zip#environment',
                                      '--driver-memory', '8G', '--py-files', './forecast.zip,digitforce.zip',
                                      'forecast/data_processing/runnable/sales_aggregation.py', str(sdate),
                                      str(edate),input_table,output_table,agg_func,col_qty,agg_type])

