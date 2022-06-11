from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def lr_classify_op(train_dataset_path, test_dataset_path,
                   model_save_dir,
                   label_col,
                   param="",
                   image_tag="latest"):
    '''
    LR 分类模型
    输入文件格式:
        train_dataset_path 训练数据集
        test_dataset_path  测试数据集
    输出文件格式:
        pickle文件

    :param test_dataset_path: 训练集
    :param train_dataset_path: 测试集
    :param model_save_dir: 模型文件保存路径
    :param label_col: 标签列
    :param image_tag: 组件版本
    :return: op
    '''

    return dsl.ContainerOp(name="lr_classify",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-ml-lr" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", train_dataset_path, test_dataset_path,
                                      model_save_dir,
                                      label_col,
                                      param])


@mount_data_pv
def lr_classify_op(train_dataset_path, test_dataset_path,
                   model_save_dir,
                   label_col,
                   categorical_feature="",
                   image_tag="latest"):
    '''
    LightGBM 分类模型
    输入文件格式:
        train_dataset_path 训练数据集
        test_dataset_path  测试数据集
    输出文件格式:
        pickle文件

    :param test_dataset_path: 训练集
    :param train_dataset_path: 测试集
    :param model_save_dir: 模型文件保存路径
    :param label_col: 标签列
    :param image_tag: 组件版本
    :return: op
    '''

    return dsl.ContainerOp(name="lightgbm_classify",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-ml-lightgbm" + f":{image_tag}",
                           command="python",
                           arguments=["main.py", train_dataset_path, test_dataset_path,
                                      model_save_dir,
                                      label_col,
                                      categorical_feature])