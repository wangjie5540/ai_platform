# coding: utf-8
from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def time_series_features_op(sales, keys, features, ts_col, sales_col, output_file, image_tag="latest"):
    '''
    time_series_features基于商品历史销量数据提取时序特征
    :param sales: A组商品信息
    :param keys: 商品区分主键，链表类型['shop_id', 'goods_id']
    :param features: 时序特征，链表类型[]
    :param ts_col: 时序特征，链表类型[]
    :param sales_col: 时序特征，链表类型[]
    :param output_file: 时序特征输出文件
    :param image_tag: 组件版本
    :return: op
    '''
    arguments = ["time_series_features.py",
                 "--sales", sales,
                 "--keys", keys,
                 "--features", features,
                 "--ts_col", ts_col,
                 "--sales_col", sales_col,
                 "--output_file", output_file
                 ]
    return dsl.ContainerOp(name="time_series_features",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-forecast-data_analysis" + f":{image_tag}",
                           command="python",
                           arguments=arguments)
