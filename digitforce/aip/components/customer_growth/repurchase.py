# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def repurchase_op(input_train_params, train, predict, output_model_path, input_predict_params, output_res_path, image_tag="latest"):
    """
    复购模型组件
    :param input_train_params: 训练配置参数文件  json
    :param train: 是否训练 bool
    :param predict: 是否预测 bool
    :param output_model_path: 模型文件输出地址
    :param input_predict_params: 预测参数文件 json
    :param output_res_path: 人群文件输出地址
    :param image_tag:
    :return:
    """
    return dsl.ContainerOp(name="lookalike",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-ml-repurchase" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_train_params, train, predict, output_model_path, input_predict_params, output_res_path])
