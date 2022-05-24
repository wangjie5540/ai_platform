from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def lookalike_op(user_embedding_path, seed_file_path, crowd_file_path, output_file_path, image_tag="latest"):
    '''

    :param user_embedding_path: 用户向量文件路径
    :param seed_file_path: 种子用户文件路径
    :param crowd_file_path: 待拓展人群文件路径
    :param output_file_path: 人群结果存放路径
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="lookalike",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-ml-lookalike" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", user_embedding_path, seed_file_path, crowd_file_path, output_file_path])
