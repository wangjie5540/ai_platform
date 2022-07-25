from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def forecast_big_order_filter(sdate, edate, image_tag="latest"):
    """
    根据用户的行为计算用户在不同兴趣上的得分
    **用户行为需要按照user_id排序**
    输入文件格式:
        user_id, item_id, profile_id, click_cnt, share_cnt, add_cnt, event_timestamp
    输出文件格式:
        user_id {"profile_1":score_1, ...} 分隔符为 \t

    :param input_file: 用户行为文件
    :param user_profile_file: 用户兴趣得分分布
    :param image_tag: 组件版本
    :return: user_profile_calculator_op
    """
    return dsl.ContainerOp(name="user_profile_calculator",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-recommend-user_profile" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, user_profile_file])
