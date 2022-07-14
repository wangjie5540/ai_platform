from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

@mount_data_pv
def chimerge_bincut_op(input_file, output_file, bins, vars, target,
                                 image_tag="latest"):
    """
    对连续值型变量进行卡方分箱，返回分箱完成后的数据
    :param input_file: 输入数据文件的位置
    :param output_file: 输出数据的位置
    :param bins: 变量对应的最大分箱数量
    :param vars: 需要分箱的变量名列表
    :param target: 目标列Y的列名
    :param image_tag:
    :return:
    """
    return dsl.ContainerOp(name="chimerge-bincut'",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-data_prepocess-bincut-chimerge_bincut"
                                 f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, bins, vars,
                                      target])
