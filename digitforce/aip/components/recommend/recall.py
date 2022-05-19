from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

IMAGE_NAME_HEADER = "/src-recommend-recall"


@mount_data_pv
def user_profile_recall_op(input_file, output_file, hot_csv_file, item_and_profile_map_file, image_tag="latest"):
    '''
    按照用户兴趣召回
    输入 input_file
            jsonl {user_id: xxx, user_profiles: {profile_id:score, ...}}
        hot_csv_file
            csv格式 无头 item_id, score
        item_and_profile_map_file
            csv格式 无头 profile_id, item_id
    输出
        jsonl {user_id:xxx, recall_item_ids:[item_id, ....]}
    :param input_file: 用户行为文件
    :param output_file: 召回结果
    :param hot_csv_file: 热门排序文件
    :param item_and_profile_map_file: item和profile的映射表
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="user_profile_recall",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-user_profile" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, hot_csv_file, item_and_profile_map_file])


@mount_data_pv
def deep_mf_op(input_file, item_embeding_file, user_embeding_file, image_tag="latest"):
    '''
    用深度学习框架训练user_emb矩阵和item_emb矩阵
    训练样本格式：
        user_id, item_id, score 分隔符为 ','
        user_id 为 int 类型
        item_id 为 int 类型
        score 为 int 类型
    训练结果会保存成文件
    用户向量结果保存格式：
        user_id, user_vec
    item向量结果保存格式：
        item_id, item_vec

    :param input_file: 训练样本路径
    :param item_embeding_file: item向量保存路径
    :param user_embeding_file: 用户向量保存路径
    :param image_tag: 组件版本
    :return: deep_mf_op
    '''
    return dsl.ContainerOp(name="deep_mf'",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-mf" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, item_embeding_file, user_embeding_file])


@mount_data_pv
def similarity_search_recall_op(user_vec_file, item_vec_file, output_file, topk, image_tag="latest"):
    '''
    基于最近邻的向量召回策略

    :param user_vec_file: 用户向量
    :param item_vec_file: item向量
    :param output_file: 召回结果
    :param topk: 召回数量
    :param image_tag: 组件版本
    :return: deep_mf_op
    '''
    return dsl.ContainerOp(name="deep_mf'",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-mf" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", user_vec_file, item_vec_file, output_file, topk])
