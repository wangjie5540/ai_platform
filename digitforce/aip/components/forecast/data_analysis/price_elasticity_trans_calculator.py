# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def price_elasticity_trans_op(goods_similarity, price_elasticity, pe_col, similarity_col, similarity_threshold, keys, sim_keys, output_file, B_sign='_B', method="trans", image_tag="latest"):
    '''
    price_elasticity_trans基于商品相似度的价格弹性迁移计算
    :param goods_similarity: 商品相似度
    :param price_elasticity: 价格弹性
    :param pe_col: 相关因素，除价格外与商品销量相关的因素
    :param similarity_col: 销量对应的字段
    :param similarity_threshold: 价格对应的字段
    :param keys: 商品区分主键
    :param sim_keys: 价格弹性计算方法
    :param B_sign: 价格弹性计算结果输出文件
    :param method: 价格弹性计算方法，迁移模型参数为trans
    :param output_file: 价格弹性计算结果输出文件
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["price_elasticity.py",
                 "--goods_similarity", goods_similarity,
                 "--price_elasticity", price_elasticity,
                 "--pe_col", pe_col,
                 "--similarity_col", similarity_col,
                 "--similarity_threshold", similarity_threshold,
                 "--keys", keys,
                 "--sim_keys", sim_keys,
                 "--B_sign", B_sign,
                 "--output_file", output_file,
                 "--method", method
                 ]
    return dsl.ContainerOp(name="price_elasticity_trans",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
