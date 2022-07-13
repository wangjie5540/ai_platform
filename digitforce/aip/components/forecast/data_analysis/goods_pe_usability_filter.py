# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def goods_pe_usability_op(goods, price_elasticity, r2_col, pe_col, keys, pe_threshold, r2_threshold, output_pe, output_goods_usable, output_goods_unusable, image_tag="latest"):
    '''
    goods_pe_usability：根据给定价格弹性及拟合优度阈值，筛选可用的商品价格弹性
    :param goods: 商品信息
    :param price_elasticity: 价格弹性
    :param r2_col: 拟合优度对应字段
    :param pe_col: 价格弹性对应的字段
    :param keys: 商品区分主键
    :param pe_threshold：价格弹性阈值
    :param r2_threshold：拟合优度阈值
    :param output_pe: 价格弹性输出文件
    :param output_goods_usable: 商品价格弹性可用
    :param output_goods_unusable: 商品价格弹性不可用
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["goods_pe_usability.py",
                 "--goods", goods,
                 "--price_elasticity", price_elasticity,
                 "--r2_col", r2_col,
                 "--pe_col", pe_col,
                 "--keys", keys,
                 "--pe_threshold", pe_threshold,
                 "--r2_threshold", r2_threshold,
                 "--output_pe", output_pe,
                 "--output_goods_usable", output_goods_usable,
                 "--output_goods_unusable", output_goods_unusable
                 ]
    return dsl.ContainerOp(name="goods_pe_usability",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
