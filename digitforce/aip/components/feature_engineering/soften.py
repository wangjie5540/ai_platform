from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def fe_soften_op(input_file, output_file,  sep=',', soften_method='z-score', cols=None, thresh_max=None, thresh_min=None,
                 percent_max=None, percent_min=None, image_tag="latest"):
    """

    @param input_file: 输入数据
    @param output_file: 输出文件路径
    @param sep: 分隔符
    @param soften_method: 平滑方式(z-score, thresh, percent, box_plot)
    @param cols: list, 平滑列名
    @param thresh_max: 阈值上限，当平滑方式为阈值时，需配置改参数
    @param thresh_min: 阈值下限，当平滑方式为阈值时，需配置改参数
    @param percent_max: 百分数上限， 当平滑方式为百分位时，需配置改参数
    @param percent_min: 百分数下限， 当平滑方式为百分位时，需配置改参数
    @param image_tag: 版本
    """
    return dsl.ContainerOp(name="feature_soften",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-feature_engineering-feature_soften" + f":{image_tag}",
                           command="python",
                           arguments=['main.py', input_file, output_file, sep, soften_method, cols, thresh_max,
                                      thresh_min, percent_max, percent_min])
