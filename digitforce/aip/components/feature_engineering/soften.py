from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def fe_soften_op(input_file, sep=',', soften_method='z-score', cols=None, thresh_max=None, thresh_min=None,
                 percent_max=None, percent_min=None, image_tag="latest"):
    """

    @param image_tag:
    @param input_file:
    @param sep:
    @param soften_method:
    @param cols:
    @param thresh_max:
    @param thresh_min:
    @param percent_max:
    @param percent_min:
    """
    return dsl.ContainerOp(name="feature_soften",
                           image=f"{AI_PLATFORM_IMAGE_REPO}"
                                 f"/src-feature_engineering-feature_soften" + f":{image_tag}",
                           command="python",
                           arguments=['main.py', input_file, sep, soften_method, cols, thresh_max,
                                      thresh_min, percent_max, percent_min])