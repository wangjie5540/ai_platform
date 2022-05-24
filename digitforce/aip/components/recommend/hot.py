from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def ctr_hot_op(user_show_and_action_table, output_file, image_tag="latest"):
    '''
    计算item的按照ctr排序的热门

    输入table必须包含字段:
        user_id, item_id,  click_cnt
    输出文件格式:
        item_id score 分隔符为 ,

    :param user_show_and_action_table: 行为表
    :param output_file: 热门排序文件
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="ctr_hot",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-recommend-hot-ctr_hot" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", user_show_and_action_table, output_file])


@mount_data_pv
def click_hot_op(user_show_and_action_table, output_file, image_tag="latest"):
    '''
    计算item的按照click_cnt排序的热门

    输入table必须包含字段:
        user_id, item_id,  click_cnt
    输出文件格式:
        item_id score 分隔符为 ,

    :param user_show_and_action_table: 行为表
    :param output_file: 热门排序文件
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="ctr_hot",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-recommend-hot-click_hot" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", user_show_and_action_table, output_file])
