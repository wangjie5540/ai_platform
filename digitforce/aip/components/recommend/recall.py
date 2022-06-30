from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

IMAGE_NAME_HEADER = "/src-recommend-recall"


@mount_data_pv
def user_profile_recall_op(input_file, output_file, hot_csv_file, item_and_profile_map_file, image_tag="latest"):
    """
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
    """
    return dsl.ContainerOp(name="user_profile_recall",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-user_profile" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, hot_csv_file, item_and_profile_map_file])


@mount_data_pv
def deep_mf_op(input_file, item_embeding_file, user_embeding_file, image_tag="latest"):
    """
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
    """
    return dsl.ContainerOp(name="deep_mf",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-mf" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, item_embeding_file, user_embeding_file])


@mount_data_pv
def similarity_search_recall_op(user_vec_file, item_vec_file, output_file, topk,
                                user_and_id_map_file="",
                                item_and_id_map_file="", image_tag="latest"):
    """
    基于最近邻的向量召回策略
    输出文件
        jsonl {'user_id': xxx, 'recall_item_ids':[xxx, xxx]}
    :param user_vec_file: 用户向量
    :param item_vec_file: item向量
    :param output_file: 召回结果
    :param topk: 召回数量
    :param user_and_id_map_file: user_id和id 映射表
    :param item_and_id_map_file: item_id和id 映射表
    :param image_tag: 组件版本
    :return: deep_mf_op
    """
    return dsl.ContainerOp(name="similarity_search_recall",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-similarity_search" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", user_vec_file, item_vec_file, output_file, topk,
                                      user_and_id_map_file, item_and_id_map_file])


@mount_data_pv
def item2vec_op(input_file, output_file, skip_gram=0, vec_size=16, recall_result_file=None, image_tag="latest"):
    """
    基于最近邻的向量召回策略
    输入文件格式
        user_id, click_cnt, save_cnt, order_cnt
    输出文件格式
        jsonl {'item_id':xxx, 'item_vec':xxx}

    :param input_file: 输入文件
    :param output_file: 输出文件

    :param skip_gram: 模型参数
    :param vec_size: 输出向量维度
    :param image_tag: 组件版本
    :return: deep_mf_op
    """
    return dsl.ContainerOp(name="item2vec",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-item2vec" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, skip_gram, vec_size, recall_result_file])


@mount_data_pv
def association_rule_mining_op(input_file, output_file, min_sup=1, min_conf=0.01, image_tag="latest"):
    """
    关联规则挖掘 输出item之间的支持度和自信度
    文件输入
        jsonl {'user_id':xxx, 'items':xxx}
    输出文件
        jsonl {'item_id':[(xxx, confidence)]}
    :param input_file:
    :param output_file:
    :param min_sup:
    :param min_conf:
    :param image_tag: 组件版本
    :return:
    """

    return dsl.ContainerOp(name="association_rule_mining'",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-association_rule_mining" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, min_sup, min_conf])


@mount_data_pv
def upload_recall_result_op(recall_result_file, redis_key_header, image_tag="latest"):
    """
    将召回结果更新到redis
    文件输入
        jsonl {'user_id':xxx, 'recall_item_ids':xxx}
    :param recall_result_file: 召回结果
    :param redis_key_header: 召回结果保存的redis key的前缀
    :param image_tag: 组件版本
    """
    return dsl.ContainerOp(name="recall_result_to_redis",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"{IMAGE_NAME_HEADER}-recall_result_to_redis" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", recall_result_file, redis_key_header])
