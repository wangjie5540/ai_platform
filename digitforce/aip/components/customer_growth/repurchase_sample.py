# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def repurchase_sample_construct(train, predict, train_date, current_date, predict_date, cate_list, image_tag="latest"):
    """
    :param train: train or not
    :param predict: predict or not
    :param train_date: training period: from train date to current date
    :param current_date: current date represented as a string
    :param predict_date: predicting(observing) period: from current date to predict date
    :param cate_list: categories used for repurchase, eg,'(蔬菜,水果)'
    :param image_tag:
    :return:
    """
    arguments = ["main.py",
                 "--train", train,
                 "--predict", predict,
                 "--trian_date", train_date,
                 "--current_date", current_date,
                 "--predict_date", predict_date,
                 "--cate_list", cate_list
                 ]
    if not train:
        arguments += ["--train", ""]
    if not predict:
        arguments += ["--predict", ""]
    return dsl.ContainerOp(name="get_repurchase_sample",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-customer_growth-sample_construct-repurchase" + f":{image_tag}",
                           command="python",
                           arguments=arguments)