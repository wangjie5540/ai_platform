# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def goods_similarity_catg_op(goods_A, goods_B, output_file, keys, catg_sv, image_tag="latest"):
    '''
    goods_similarity_catg基于商品品类计算两组商品相似度
    :param goods_A: A组商品信息
    :param goods_B: B组商品信息
    :param keys: 商品区分主键，链表类型['shop_id', 'goods_id']
    :param catg_sv: 品类相似度值，字典类型{'catg_s_id': 0.9, 'catg_m_id': 0.7, 'catg_l_id': 0.5}
    :param output_file: 相似度计算结果输出文件
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["goods_similarity.py",
                 "--goods_A", goods_A,
                 "--goods_B", goods_B,
                 "--keys", keys,
                 "--catg_sv", catg_sv,
                 "--output_file", output_file
                 ]
    return dsl.ContainerOp(name="goods_similarity_catg",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-image" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
