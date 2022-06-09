# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def adtributor_op(control_data_file, experimental_data_file, feature_list, target_feature, target_is_rate, output1_file, output2_file, image_tag="latest"):
    '''
    基于adtributor算法的指标诊断
    :param control_data_file: 对照组数据文件
    :param experimental_data_file: 实验组数据文件
    :param feature_list: 待分析特征维度列表
    :param target_feature: 目标特征，率值目标传递[分子,分母]
    :param target_is_rate: 目标特征是否为率值
    :param output1_file: 维度集合输出文件
    :param output2_file: 细分元素输出文件
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["main.py",
                 "--control_data_file", control_data_file,
                 "--experimental_data_file", experimental_data_file,
                 "--feature_list", feature_list,
                 "--target_feature", target_feature,
                 "--output1_file", output1_file,
                 "--output2_file", --output2_file,
                 ]
    if not target_is_rate:
        arguments += ["--train", ""]
    return dsl.ContainerOp(name="adtributor",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-causal-adtributor" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
