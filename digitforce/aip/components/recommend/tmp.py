from digitforce.aip.common.constants.global_constant import AI_PLATFORM_IMAGE_REPO
from digitforce.aip.components.op_decorator import *


@mount_data_pv
def rank_data_process_op(sql, output_file, info_log_file, error_log_file, image_tag='latest'):
    op = dsl.ContainerOp(
        name='rank_data_process_op',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-recommend-rank-data_process" + f":{image_tag}",
        command="python",
        arguments=["main.py", sql, output_file, info_log_file, error_log_file]
    )
    op.container.set_image_pull_policy('Always')
    return op


@mount_data_pv
def lightgbm_train_op(dataset_path, model_output_file, info_log_file, error_log_file, image_tag='latest'):
    op = dsl.ContainerOp(
        name='lightgbm_train_op',
        image=f"{AI_PLATFORM_IMAGE_REPO}"
              f"/src-recommend-rank-lightgbm" + f":{image_tag}",
        command="python",
        arguments=["main.py", dataset_path, model_output_file, info_log_file, error_log_file]
    )
    op.container.set_image_pull_policy('Always')
    return op
