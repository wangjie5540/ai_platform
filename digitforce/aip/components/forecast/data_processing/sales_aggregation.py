from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def forecast_big_order_filter(sdate, edate, image_tag="latest"):
    """
    大单过滤

    :param sdate: 开始时间
    :param edate: 结束时间
    :param image_tag: 组件版本
    :return: user_profile_calculator_op
    """
    return dsl.ContainerOp(name="forecast_big_order_filter",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", sdate, edate])
