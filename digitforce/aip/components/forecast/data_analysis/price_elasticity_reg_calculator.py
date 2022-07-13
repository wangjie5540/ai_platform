# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def price_elasticity_trans_op(sales, sep, keys, factor, y, p, output_file, method="log-log", image_tag="latest"):
    '''
    price_elasticity_trans基于商品历史销量的价格弹性计算
    :param sales: 商品历史销量
    :param sep: csv文件数据分隔符
    :param keys: 商品区分主键
    :param factor: 其他相关因素，影响销量的其他因素，如天气 节假日等
    :param y: 销量对应字段
    :param p: 价格对应字段
    :param output_file: 价格弹性计算结果输出文件
    :param method: 价格弹性计算方法，回归模型默认为log-log
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["price_elasticity.py",
                 "--sales", sales,
                 "--sep", sep,
                 "--keys", keys,
                 "--factor", factor,
                 "--y", y,
                 "--p", p,
                 "--output_file", output_file,
                 "--method", method
                 ]
    return dsl.ContainerOp(name="price_elasticity_reg",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-data_analysis-runnable-price_elasticity" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
