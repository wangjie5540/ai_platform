from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

@mount_data_pv
def cause_markov_op(input_file, output_file, image_tag="latest"):
    # 基于马尔科夫链的贡献归因

    # input_file输入样例：
    # path;total_conversions
    # Email;110
    # Email > Facebook;11
    # Email > Facebook > House > Ads;8
    # Facebook > House > Ads;72
    # Facebook > House > Ads > Instagram;24

    # output_file输出样例：
    # House   240.00000000000003
    # Instagram   147.58333333333334
    # Facebook    183.5
    arguments = ["main.py",
                 "--input_file", input_file,
                 "--output_file", output_file
                 ]
    return dsl.ContainerOp(name="cause_markov",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-causal-markov" + f":{image_tag}",
                           command="python",
                           arguments=arguments)

@mount_data_pv
def cause_shapley_op(input_file, output_file, model_type, image_tag="latest"):
    # 基于沙普利值的贡献归因

    # input_file输入样例：
    # path;total_conversions
    # Email;110
    # Email > Facebook;11
    # Email > Facebook > House > Ads;8
    # Facebook > House > Ads;72
    # Facebook > House > Ads > Instagram;24

    # output_file输出样例：
    # House   240.00000000000003
    # Instagram   147.58333333333334
    # Facebook    183.5

    # model_type有2个取值：simply 或 ordered
    # 模型1: simply，基于传统shapley的简化，原文：https://arxiv.org/abs/1804.05327，实现参考：https://github.com/ianchute/shapley-attribution-model-zhao-naive
    # 模型2: ordered，在simply基础上计算每个渠道在各个触点上的贡献度，从1维扩展到n维，实现参考同上
    arguments = ["main.py",
                 "--input_file", input_file,
                 "--output_file", output_file,
                 "--model_type", model_type
                 ]
    return dsl.ContainerOp(name="cause_shapley",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-causal-shapley" + f":{image_tag}",
                           command="python",
                           arguments=arguments)