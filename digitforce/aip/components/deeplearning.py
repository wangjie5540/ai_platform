from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def lstm_classify_op(train, valid, predict,
                     input_file_train, input_file_valid, input_file_predict,
                     output_model, output_predict_file, epochs, output_size,
                     vocab_size, embed_dim, dropout, hidden_size, num_layers,
                     lr, batch_size, image_tag="latest"):
    '''
    基于lstm的分类任务
    输入文件格式:
        feature1,feature2...,featuren \t label
          - 其中feature通过逗号分隔，和label之间通过\t分隔
          - feature/label 类型为int(离散型)
    输出文件格式:
        - 如果train==True，则保存pytorch-model
        - 如果predict==True，则保存预测结果

    :param train: 是否训练，是为True
    :param valid: 是否验证，是为True
    :param predict: 是否预测，是为True
    :param input_file_train: 待训练的特征+label，格式见上
    :param input_file_valid: 待验证的特征+label，格式见上
    :param input_file_predict: 待预测的特征+label，格式见上
    :param output_model: 输出模型地址
    :param output_predict_file: 输出预测结果的地址
    :param output_size: 分类个数，例如6
    :param vocab_size: 离散特征的类别个数（例如，如果是汉字的onehot特征，则为词典大小），如9277
    :param embed_dim: 对上述的vacab的编码维数
    :param epochs: 训练轮数，如10
    :param lr: 学习率，如0.001
    :param dropout: 丢失概率，如0.5
    :param hidden_size: 隐层数目，如32
    :param num_layers: lstm层数，如1、2等
    :param batch_size: 每批样本量，如16
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["main.py",
                 "--task_type", "classify",
                 "--train", train,
                 "--valid", valid,
                 "--predict", predict,
                 "--input_file_train", input_file_train,
                 "--input_file_valid", input_file_valid,
                 "--input_file_predict", input_file_predict,
                 "--output_model", output_model,
                 "--output_predict_file", output_predict_file,
                 "--epochs", epochs,
                 "--output_size", output_size,
                 "--vocab_size", vocab_size,
                 "--embed_dim", embed_dim,
                 "--dropout", dropout,
                 "--hidden_size", hidden_size,
                 "--num_layers", num_layers,
                 "--lr", lr,
                 "--batch_size", batch_size]
    if not train:
        arguments += ["--train", ""]
    if not valid:
        arguments += ["--valid", ""]
    if not predict:
        arguments += ["--predict", ""]
    return dsl.ContainerOp(name="lstm_classify",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-deeplearning-lstm" + f":{image_tag}",
                           command="python",
                           arguments=arguments)

@mount_data_pv
def lstm_regress_op(train, valid, predict,
                     input_file_train, input_file_valid, input_file_predict,
                     output_model, output_predict_file, epochs,
                     dropout, hidden_size, num_layers,
                     lr, batch_size, image_tag="latest"):
    '''
    基于lstm的回归任务
    输入文件格式:
        feature1,feature2...,featuren \t label
          - 其中feature通过逗号分隔，和label之间通过\t分隔
          - feature/label 为float(连续型)
    输出文件格式:
        - 如果train==True，则保存pytorch-model
        - 如果predict==True，则保存预测结果

    :param train: 是否训练，是为True
    :param valid: 是否验证，是为True
    :param predict: 是否预测，是为True
    :param input_file_train: 待训练的特征+label，格式见上
    :param input_file_valid: 待验证的特征+label，格式见上
    :param input_file_predict: 待预测的特征+label，格式见上
    :param output_model: 输出模型地址
    :param output_predict_file: 输出预测结果的地址
    :param epochs: 训练轮数，如10
    :param lr: 学习率，如0.001
    :param dropout: 丢失概率，如0.5
    :param hidden_size: 隐层数目，如32
    :param num_layers: lstm层数，如1、2等
    :param batch_size: 每批样本量，如16
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["main.py",
                 "--task_type", "regress",
                 "--train", train,
                 "--valid", valid,
                 "--predict", predict,
                 "--input_file_train", input_file_train,
                 "--input_file_valid", input_file_valid,
                 "--input_file_predict", input_file_predict,
                 "--output_model", output_model,
                 "--output_predict_file", output_predict_file,
                 "--output_size", 1,
                 "--epochs", epochs,
                 "--dropout", dropout,
                 "--hidden_size", hidden_size,
                 "--num_layers", num_layers,
                 "--lr", lr,
                 "--batch_size", batch_size]
    if not train:
        arguments += ["--train", ""]
    if not valid:
        arguments += ["--valid", ""]
    if not predict:
        arguments += ["--predict", ""]
    return dsl.ContainerOp(name="lstm_regress",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-deeplearning-lstm" + f":{image_tag}",
                           command="python",
                           arguments=arguments)

@mount_data_pv
def attention_classify_op(train, valid, predict,
                     input_file_train, input_file_valid, input_file_predict,
                     output_model, output_predict_file, epochs, output_size,
                     vocab_size, embed_dim, dropout, hidden_size, num_layers,
                     lr, batch_size, image_tag="latest"):
    '''
    基于attention的分类任务
    输入文件格式:
        feature1,feature2...,featuren \t label
          - 其中feature通过逗号分隔，和label之间通过\t分隔
          - feature/label 类型为int(离散型)
    输出文件格式:
        - 如果train==True，则保存pytorch-model
        - 如果predict==True，则保存预测结果

    :param train: 是否训练，是为True
    :param valid: 是否验证，是为True
    :param predict: 是否预测，是为True
    :param input_file_train: 待训练的特征+label，格式见上
    :param input_file_valid: 待验证的特征+label，格式见上
    :param input_file_predict: 待预测的特征+label，格式见上
    :param output_model: 输出模型地址
    :param output_predict_file: 输出预测结果的地址
    :param output_size: 分类个数，例如6
    :param vocab_size: 离散特征的类别个数（例如，如果是汉字的onehot特征，则为词典大小），如9277
    :param embed_dim: 对上述的vacab的编码维数
    :param epochs: 训练轮数，如10
    :param lr: 学习率，如0.001
    :param dropout: 丢失概率，如0.5
    :param hidden_size: 隐层数目，如32
    :param num_layers: lstm层数，如1、2等
    :param batch_size: 每批样本量，如16
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["main.py",
                 "--task_type", "classify",
                 "--train", train,
                 "--valid", valid,
                 "--predict", predict,
                 "--input_file_train", input_file_train,
                 "--input_file_valid", input_file_valid,
                 "--input_file_predict", input_file_predict,
                 "--output_model", output_model,
                 "--output_predict_file", output_predict_file,
                 "--epochs", epochs,
                 "--output_size", output_size,
                 "--vocab_size", vocab_size,
                 "--embed_dim", embed_dim,
                 "--dropout", dropout,
                 "--hidden_size", hidden_size,
                 "--num_layers", num_layers,
                 "--lr", lr,
                 "--batch_size", batch_size]
    if not train:
        arguments += ["--train", ""]
    if not valid:
        arguments += ["--valid", ""]
    if not predict:
        arguments += ["--predict", ""]
    return dsl.ContainerOp(name="lstm_classify",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-deeplearning-attention" + f":{image_tag}",
                           command="python",
                           arguments=arguments)