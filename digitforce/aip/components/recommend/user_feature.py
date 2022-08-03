from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO

from digitforce.aip.components.op_decorator import *

IMAGE_NAME_HEADER = "/src-recommend-user_feature"


@mount_data_pv
def user_action_seq_op(start_datetime, end_datetime, result_table, image_tag="latest"):
    """
    生成用户行为序列
    """

    return dsl.ContainerOp(name="cal_user_action_seq",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-agg_user_action_hive" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", start_datetime, end_datetime, result_table])


@mount_data_pv
def user_order_statis_op(start_datetime, end_datetime, result_table, image_tag="latest"):
    """
    统计用户购买行为
    """

    return dsl.ContainerOp(name="cal_user_order",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-user_order_statistic" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", start_datetime, end_datetime, result_table])
