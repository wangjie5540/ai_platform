from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *

@mount_data_pv
def lstm_classify_train_op(input_file_train, output_model, class_nums, input_dict_size, epochs, lr, batch_size, image_tag="latest"):
    '''
    基于lstm的分类训练任务
    输入文件格式:
        feature1,feature2...,featuren \t label
          - 其中feature通过逗号分隔，和label之间通过\t分隔
          - feature/label 类型为int(离散型)
    输出文件格式:
        pytorch-model格式

    :param input_file_train: 带训练的特征+label，格式见上
    :param output_model: 输出模型地址
    :param class_nums: 分类个数，例如6
    :param input_dict_size: 离散特征的类别个数（例如，如果是汉字的onehot特征，则为词典大小），如9277
    :param epochs: 训练轮数，如10
    :param lr: 学习率，如0.001
    :param batch_size: 每批样本量，如16
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="lstm_classify_train",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-deeplearning-lstm_classify" + f":{image_tag}",
                           command="python",
                           arguments=["main.py",
                                      "--input_file_train", input_file_train,
                                      "--output_model", output_model,
                                      "--epochs", epochs,
                                      "--lr", lr,
                                      "--vocab_size", input_dict_size,
                                      "--output_size", class_nums,
                                      "--batch_size", batch_size,
                                      "--predict", "",
                                      "--valid", ""])


@mount_data_pv
def lstm_classify_predict_op(input_file_predict, model_file, output_file, class_nums, input_dict_size, image_tag="latest"):
    '''
    基于lstm的分类预测任务
    输入文件格式:
        feature1,feature2...,featuren \t label
          - 其中feature通过逗号分隔，和label之间通过\t分隔
          - feature/label 类型为int(离散型)
    输出文件格式:
        pytorch-model格式

    :param input_file_predict: 带训练的特征+label，格式见上，其中label可以全置为0或1
    :param model_file: 模型地址
    :param output_file: 输出预测结果
    :param class_nums: 分类个数，例如6
    :param input_dict_size: 离散特征的类别个数（例如，如果是汉字的onehot特征，则为词典大小），如9277
    :param image_tag: 组件版本
    :return: op
    '''
    return dsl.ContainerOp(name="lstm_classify_predict",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-deeplearning-lstm_classify" + f":{image_tag}",
                           command="python",
                           arguments=["main.py",
                                      "--input_file_predict", input_file_predict,
                                      "--output_model", model_file,
                                      "--vocab_size", input_dict_size,
                                      "--output_size", class_nums,
                                      "--predict", "",
                                      "--valid", ""])
