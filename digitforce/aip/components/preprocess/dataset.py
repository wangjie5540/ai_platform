from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def generate_mf_train_dataset_op(input_file, output_file, user_and_id_map_file, item_and_id_map_file, names,
                                 image_tag="latest"):
    """
    生成mf训练数据
    input_file csv文件 必须包含 user_id, item_id, click_cnt, save_cnt, order_cnt
    output_file 输出mf训练样本, 训练样本中正负样本比例1:1 输出文件格式 user_id, item_id, score

    :param input_file: 用户行为文件
    :param output_file: 训练数据
    :param item_and_id_map_file: item_id映射表 将item_id 映射到 [1, n] n为 item_id个数
    :param user_and_id_map_file: user_id映射表 将user_id 映射到 [1, n] n为 user_id个数
    :param image_tag: 组件版本
    :return: op
    """
    return dsl.ContainerOp(name="mf-data_generator",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-data_prepocess-dataset-mf"
                                 f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, user_and_id_map_file, item_and_id_map_file,
                                      names])


@mount_data_pv
def load_user_action_op(show_and_action_table, output_file,
                        image_tag="latest"):
    """
    从hive中抽取有点击或收藏或购买的日志 并保存到本地csv文件
    show_and_action_table hive表 必须包含 user_id, item_id, click_cnt, save_cnt, order_cnt, eventtimestamp
    output_file csv文件 只保留用户user_id, item_id, click_cnt, save_cnt, order_cnt

    :param show_and_action_table: 展示和行为表
    :param output_file: 训练数据
    :param image_tag: 组件版本
    :return: op
    """

    return dsl.ContainerOp(name="load_user_action_from_hive",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-data_preprocess-dataset-user_action"
                                 f":{image_tag}",
                           command="python",
                           arguments=["main.py", show_and_action_table, output_file])
