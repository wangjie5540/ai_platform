from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

@mount_data_pv
def normalization_op(input_file, output_file, vars,
                                 image_tag="latest"):
    """
    对连续值型变量进行卡方分箱，返回分箱完成后的数据
    :param input_file: 输入数据文件的位置
    :param output_file: 输出数据的位置
    :param vars: 需要归一化的变量名列表,以‘，’分割， eg ‘age,quantities,times'
    :param image_tag:
    :return:
    """
    return dsl.ContainerOp(name="normalization",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-data_prepocess-normalization"
                                 f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, vars])
