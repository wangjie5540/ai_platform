from digitforce.aip.components.op_decorator import *


@mount_data_pv
def generate_mf_train_dataset_op(input_file, output_file, user_and_id_map_file, item_and_id_map_file,
                                 image_tag="latest"):
    '''
    生成mf训练数据
    input_file csv文件 必须包含 user_id, item_id, click_cnt, share_cnt, save_cnt
    output_file 输出mf训练样本, 训练样本中正负样本比例1:1 输出文件格式 user_id, item_id, score

    :param input_file: 用户行为文件
    :param output_file: 训练数据
    :param item_and_id_map_file: item_id映射表 将item_id 映射到 [1, n] n为 item_id个数
    :param user_and_id_map_file: user_id映射表 将user_id 映射到 [1, n] n为 user_id个数
    :param image_tag: 组件版本
    :return: deep_mf_op
    '''

    return dsl.ContainerOp(name="mf-data_generator'",
                           image="digit-force-docker.pkg.coding.net/ai-platform/ai-src/src-data_prepocess-dataset-mf-data_generator" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", input_file, output_file, user_and_id_map_file, item_and_id_map_file])
